use crate::{
    data::{product_batch::ProductBatchRepository, product_mov_hist::ProductMovHistRepository},
    simulation::parameter::SimulationParameters,
    simulation::per_day::SimulationDay,
};
use sqlx::{postgres::PgPoolOptions, types::BigDecimal, Pool, Postgres};
use uuid::Uuid;

use std::env;

use chrono::{DateTime, Datelike, Days, NaiveDate, Utc};

pub async fn run(
    reference_date: &str,
    days_to_analyze: u64,
    product_id: Uuid,
    default_time_limit: u64,
    stock_limit: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let (initial_date, final_date) = get_initial_and_final_dates(reference_date, days_to_analyze)?;
    let (initial_week, final_week) = get_initial_and_final_weeks(initial_date, final_date);

    let db = get_db_conn_pool().await?;
    let product_mov_hist_repository = ProductMovHistRepository::new(db.clone());
    let product_batch_repository = ProductBatchRepository::new(db.clone());

    let (_, historic) = product_mov_hist_repository
        .aggregate_by_product_id_and_week_of_year_and_day_of_week(
            product_id,
            initial_week,
            final_week,
        )
        .await?;
    let (_, product_batches) = product_batch_repository
        .find_all_by_product(product_id)
        .await?;

    let sim_param =
        SimulationParameters::new(initial_date, stock_limit, default_time_limit, historic);

    let simulation_day0 = SimulationDay {
        date: initial_date,
        batches: product_batches,
        stock_time_limit_exceeded: None,
        stock_shortage: None,
        stock_limit_exceeded: None,
    };

    let mut simulation = Simulation {
        product_id: product_id,
        days: vec![simulation_day0],
    };

    let mut date = initial_date;
    while date < final_date {
        eprintln!("Date: {:?}", date);
        date = date
            .checked_add_days(Days::new(1))
            .expect(format!("Failure to determine next date after {:?}", date).as_str());

        let next_day_opt = simulation
            .days
            .last()
            .map(|last_day| last_day.calculate_next(&sim_param));
        next_day_opt.map(|next_day| simulation.days.push(next_day));
    }

    Ok(())
}

struct Simulation {
    product_id: Uuid,
    days: Vec<SimulationDay>,
}

#[derive(Debug, Clone)]
pub struct SimulationItemBatch {
    pub quantity: BigDecimal,
    //    entry_date: NaiveDate,
    pub deadline_date: NaiveDate,
}

fn get_initial_and_final_weeks(
    initial_date: DateTime<Utc>,
    final_date: DateTime<Utc>,
) -> (i16, i16) {
    (
        initial_date.iso_week().week() as i16,
        final_date.iso_week().week() as i16,
    )
}

fn get_initial_and_final_dates(
    reference_date: &str,
    days_to_analyze: u64,
) -> Result<(DateTime<Utc>, DateTime<Utc>), Box<dyn std::error::Error>> {
    let initial_date = DateTime::parse_from_rfc3339(reference_date)?.to_utc();
    let final_date = initial_date
        .checked_add_days(Days::new(days_to_analyze))
        .expect("Failure to determine final_date");
    eprintln!(
        "initial_date: {:?}, final_date: {:?}",
        initial_date, final_date
    );
    Ok((initial_date, final_date))
}

fn get_all_week_between(initial_value: u32, final_value: u32) -> Vec<i16> {
    let max = 53;
    let end = if initial_value < final_value {
        final_value + 1
    } else {
        final_value + max + 1
    };
    let range = (initial_value..end)
        .map(|v| if v > max { v - max } else { v })
        .map(|v| v as i16)
        .collect::<Vec<i16>>();
    range
}

async fn get_db_conn_pool() -> Result<Pool<Postgres>, Box<dyn std::error::Error>> {
    let database_url = env::var("DATABASE_URL")?;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;
    Ok(pool)
}

#[cfg(test)]
mod tests {

    use chrono::Datelike;

    use super::*;

    #[test]
    fn test_week_and_days_calc() {
        let reference_date = "2023-09-05T00:00:00Z";
        let days_to_analyze = 90;

        let (initial_date, final_date) =
            get_initial_and_final_dates(reference_date, days_to_analyze).unwrap();
        assert_eq!(initial_date.year(), 2023);
        assert_eq!(initial_date.month0(), 8);
        assert_eq!(initial_date.day0(), 4);
        assert_eq!(final_date.year(), 2023);
        assert_eq!(final_date.month0(), 11);
        assert_eq!(final_date.day0(), 3);

        let (initial_week, final_week) = get_initial_and_final_weeks(initial_date, final_date);
        assert_eq!(initial_week, 36);
        assert_eq!(final_week, 49);

        let reference_date = "2024-01-01T00:00:00Z";
        let days_to_analyze = 90;

        let (initial_date, final_date) =
            get_initial_and_final_dates(reference_date, days_to_analyze).unwrap();
        assert_eq!(initial_date.year(), 2024);
        assert_eq!(initial_date.month0(), 0);
        assert_eq!(initial_date.day0(), 0);
        assert_eq!(final_date.year(), 2024);
        assert_eq!(final_date.month0(), 2);
        assert_eq!(final_date.day0(), 30);

        let (initial_week, final_week) = get_initial_and_final_weeks(initial_date, final_date);
        assert_eq!(initial_week, 1);
        assert_eq!(final_week, 13);
    }
}
