use crate::data::{
    general_conf::GeneralConfRepository,
    product_batch::{ProductBatch, ProductBatchRepository},
    product_mov_hist::{ProductMovHist, ProductMovHistRepository},
    product_props::ProductPropsRepository,
};
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use uuid::Uuid;

use std::{convert::TryFrom, env};

use chrono::{DateTime, Datelike, Days, Utc};

use super::control::SimulationControl;

const DEFAULT_DATABASE_POOL_SIZE: u32 = 5;

struct SimData {
    initial_date: DateTime<Utc>,
    final_date: DateTime<Utc>,
    stock_maximum_quantity: u64,
    new_batch_default_expiration_days: u64,
    product_batches: Vec<ProductBatch>,
    historic: Vec<ProductMovHist>,
}

pub struct Orchestrator {
    product_mov_hist_repository: ProductMovHistRepository,
    product_batch_repository: ProductBatchRepository,
    general_conf_repository: GeneralConfRepository,
    product_props_repository: ProductPropsRepository,
}

impl Orchestrator {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let db = Self::get_db_conn_pool().await?;
        Ok(Self {
            product_mov_hist_repository: ProductMovHistRepository::new(db.clone()),
            product_batch_repository: ProductBatchRepository::new(db.clone()),
            general_conf_repository: GeneralConfRepository::new(db.clone()),
            product_props_repository: ProductPropsRepository::new(db.clone()),
        })
    }

    pub async fn run_by_product(
        &self,
        product_id: Uuid,
        reference_date: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let SimData {
            initial_date,
            final_date,
            stock_maximum_quantity,
            new_batch_default_expiration_days,
            product_batches,
            historic,
        } = self.prepare_data_for(product_id, reference_date).await?;

        let simulation = SimulationControl::new(
            product_id,
            initial_date,
            final_date,
            stock_maximum_quantity,
            new_batch_default_expiration_days,
            product_batches,
            historic,
        );

        let days = simulation.run_once();

        Ok(())
    }

    async fn prepare_data_for(
        &self,
        product_id: Uuid,
        reference_date: &str,
    ) -> Result<SimData, Box<dyn std::error::Error>> {
        let (_, general_conf) = self.general_conf_repository.find_last().await?;
        let (_, product_props) = self
            .product_props_repository
            .find_one_by_product(product_id)
            .await?;

        let days_to_analyze = product_props
            .simulation_forecast_days
            .unwrap_or(general_conf.default_simulation_forecast_days);

        let (initial_date, final_date) =
            Self::get_initial_and_final_dates(reference_date, days_to_analyze)?;
        let (initial_week, final_week) =
            Self::get_initial_and_final_weeks(initial_date, final_date);

        let new_batch_default_expiration_days =
            u64::try_from(product_props.new_batch_default_expiration_days)?;
        let stock_maximum_quantity = u64::try_from(product_props.maximum_quantity)?;

        let (_, historic) = self
            .product_mov_hist_repository
            .aggregate_by_product_id_and_week_of_year_and_day_of_week(
                product_id,
                initial_week,
                final_week,
            )
            .await?;

        let (_, product_batches) = self
            .product_batch_repository
            .find_all_by_product(product_id)
            .await?;

        Ok(SimData {
            initial_date,
            final_date,
            stock_maximum_quantity,
            new_batch_default_expiration_days,
            product_batches,
            historic,
        })
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
        days_to_analyze: i16,
    ) -> Result<(DateTime<Utc>, DateTime<Utc>), Box<dyn std::error::Error>> {
        let initial_date = DateTime::parse_from_rfc3339(reference_date)?.to_utc();
        let final_date = initial_date
            .checked_add_days(Days::new(u64::try_from(days_to_analyze)?))
            .expect("Failure to determine final_date");
        eprintln!(
            "initial_date: {:?}, final_date: {:?}",
            initial_date, final_date
        );
        Ok((initial_date, final_date))
    }

    async fn get_db_conn_pool() -> Result<Pool<Postgres>, Box<dyn std::error::Error>> {
        let database_url = env::var("DATABASE_URL")?;
        let database_pool_size = env::var("DATABASE_POOL_SIZE")
            .and_then(|var| Ok(var.parse::<u32>()))
            .unwrap_or(Ok(DEFAULT_DATABASE_POOL_SIZE))?;

        let pool = PgPoolOptions::new()
            .max_connections(database_pool_size)
            .connect(&database_url)
            .await?;
        Ok(pool)
    }
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
            Orchestrator::get_initial_and_final_dates(reference_date, days_to_analyze).unwrap();
        assert_eq!(initial_date.year(), 2023);
        assert_eq!(initial_date.month0(), 8);
        assert_eq!(initial_date.day0(), 4);
        assert_eq!(final_date.year(), 2023);
        assert_eq!(final_date.month0(), 11);
        assert_eq!(final_date.day0(), 3);

        let (initial_week, final_week) =
            Orchestrator::get_initial_and_final_weeks(initial_date, final_date);
        assert_eq!(initial_week, 36);
        assert_eq!(final_week, 49);

        let reference_date = "2024-01-01T00:00:00Z";
        let days_to_analyze = 90;

        let (initial_date, final_date) =
            Orchestrator::get_initial_and_final_dates(reference_date, days_to_analyze).unwrap();
        assert_eq!(initial_date.year(), 2024);
        assert_eq!(initial_date.month0(), 0);
        assert_eq!(initial_date.day0(), 0);
        assert_eq!(final_date.year(), 2024);
        assert_eq!(final_date.month0(), 2);
        assert_eq!(final_date.day0(), 30);

        let (initial_week, final_week) =
            Orchestrator::get_initial_and_final_weeks(initial_date, final_date);
        assert_eq!(initial_week, 1);
        assert_eq!(final_week, 13);
    }
}
