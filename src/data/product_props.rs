pub mod repository;

use std::{time::Instant, collections::HashMap};
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgPoolOptions, query::QueryAs, types::BigDecimal, FromRow, Pool, Postgres};

pub struct ProductProps { 
    pub id: UUID,
    pub simulation_forecast_days: Option<u16>,
    pub scenario_random_range_factor: Option<f32>,
    pub maximum_historic_days: Option<u16>,
    pub maximum_quantity: u32,
    pub minimum_quantity: u32,
    pub active: bool,
//    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
//    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
}

pub struct ProductPropsRepository {
    db: &Pool<Postgres>
}

impl ProductPropsRepository {

    pub fn new(db: &Pool<Postgres>) -> ProductPropsRepository {
        Repository {
            db: &db
        }
    }
    
    pub async fn find_all(
        &self,
    ) -> Result<(Duration, Vec<ProductProps>), Box<dyn std::error::Error>> {
        let timer = Instant::now();

        let query = sqlx::query_as::<_, ProductProps>("
            SELECT 
                id,
                simulation_forecast_days,
                scenario_random_range_factor,
                maximum_historic_days,
                maximum_quantity,
                minimum_quantity,
                active
            FROM product_props;
        ");

        let query_res = query.fetch_all(&self.db).await?;

        Ok((timer.elapsed(), query_res))
    }

    pub async fn find_all_by_status(
        &self,
        is_active: bool,
    ) -> Result<(Duration, Vec<ProductProps>), Box<dyn std::error::Error>> {
        let timer = Instant::now();

        let query = sqlx::query_as::<_, ProductProps>("
            SELECT 
                id,
                simulation_forecast_days,
                scenario_random_range_factor,
                maximum_historic_days,
                maximum_quantity,
                minimum_quantity,
                active
            FROM product_props
            WHERE active = $1;
        ");

        let query_res = query.bind(is_active).fetch_all(&self.db).await?;

        Ok((timer.elapsed(), query_res))
    }

}