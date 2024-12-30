use crate::{
    data, simulation::parameter::SimulationParameters, simulation::per_day::SimulationDay,
};
use sqlx::{postgres::PgPoolOptions, query::QueryAs, types::BigDecimal, FromRow, Pool, Postgres};
use uuid::Uuid;

use std::{collections::LinkedList, env, time::Instant};

use chrono::{Datelike, Days, NaiveDate};

#[derive(Debug, FromRow, Clone)]
pub struct Hist {
    //    product_id: Uuid,
    pub entry_qty: BigDecimal,
    pub withdrawal_qty: BigDecimal,
    pub week_of_year: i16,
    pub day_of_week: i16,
}

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

    let historic = do_query(product_id, initial_week, final_week, db).await?;

    let sim_param =
        SimulationParameters::new(initial_date, stock_limit, default_time_limit, historic);

    let batch = SimulationItemBatch {
        quantity: BigDecimal::from(100),
        deadline_date: initial_date.checked_add_days(Days::new(10)).unwrap(),
    };

    let simulation_day0 = SimulationDay {
        date: initial_date,
        batches: LinkedList::from([batch]),
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

fn get_initial_and_final_weeks(initial_date: NaiveDate, final_date: NaiveDate) -> (u32, u32) {
    (initial_date.iso_week().week(), final_date.iso_week().week())
}

fn get_initial_and_final_dates(
    reference_date: &str,
    days_to_analyze: u64,
) -> Result<(NaiveDate, NaiveDate), Box<dyn std::error::Error>> {
    let initial_date = NaiveDate::parse_from_str(reference_date, "%Y-%m-%d")?;
    let final_date = initial_date
        .checked_add_days(Days::new(days_to_analyze))
        .expect("Failure to determine final_date");
    eprintln!(
        "initial_date: {:?}, final_date: {:?}",
        initial_date, final_date
    );
    Ok((initial_date, final_date))
}

async fn do_query(
    product_id: Uuid,
    initial_week: u32,
    final_week: u32,
    db: Pool<Postgres>,
) -> Result<Vec<Hist>, Box<dyn std::error::Error>> {
    let weeks = get_all_week_between(initial_week, final_week);
    eprintln!("weeks: ({:?})", weeks);
    let query = get_query();
    let start = Instant::now();
    let historic = query
        .bind(product_id)
        .bind(&weeks[..])
        .fetch_all(&db)
        .await?;
    let duration = start.elapsed();
    println!(
        "result set length: {:?}, elapsed {:?}",
        &historic.len(),
        duration
    );
    Ok(historic)
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

fn get_query<'a>() -> QueryAs<'a, Postgres, Hist, sqlx::postgres::PgArguments> {
    sqlx::query_as::<_, Hist>(
        "
        SELECT 
          item_id,
          AVG(entry_qty) AS entry_qty,
          AVG(withdrawal_qty) AS withdrawal_qty,
          week_of_year,
          day_of_week
        FROM item_mov_hist
        WHERE item_id = $1
        AND   week_of_year = ANY ($2)
        GROUP BY item_id, week_of_year, day_of_week
        ORDER BY week_of_year, day_of_week;
    ",
    )
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
        let reference_date = "2023-09-05";
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

        let reference_date = "2024-01-01";
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
