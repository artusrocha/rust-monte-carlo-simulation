use sqlx::{
    types::{BigDecimal, Uuid},
    FromRow, Pool, Postgres,
};
use std::time::{Duration, Instant};

#[derive(Debug, FromRow, Clone)]
pub struct ProductProps {
    pub id: Uuid,
    pub simulation_forecast_days: Option<i16>,
    pub scenario_random_range_factor: Option<BigDecimal>,
    pub maximum_historic_days: Option<i16>,
    pub maximum_quantity: i32,
    pub minimum_quantity: i32,
    pub active: bool,
    //    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    //    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
}

pub struct ProductPropsRepository {
    db: Pool<Postgres>,
}

impl ProductPropsRepository {
    pub fn new(db: Pool<Postgres>) -> ProductPropsRepository {
        ProductPropsRepository { db: db }
    }

    pub async fn find_all(
        &self,
    ) -> Result<(Duration, Vec<ProductProps>), Box<dyn std::error::Error>> {
        let timer = Instant::now();

        let query = sqlx::query_as::<_, ProductProps>(
            "
            SELECT 
                id,
                simulation_forecast_days,
                scenario_random_range_factor,
                maximum_historic_days,
                maximum_quantity,
                minimum_quantity,
                active
            FROM product_props;
        ",
        );

        let query_res = query.fetch_all(&self.db).await?;

        Ok((timer.elapsed(), query_res))
    }

    pub async fn find_all_by_status(
        &self,
        is_active: bool,
    ) -> Result<(Duration, Vec<ProductProps>), Box<dyn std::error::Error>> {
        let timer = Instant::now();

        let query = sqlx::query_as::<_, ProductProps>(
            "
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
        ",
        );

        let query_res = query.bind(is_active).fetch_all(&self.db).await?;

        Ok((timer.elapsed(), query_res))
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use sqlx::postgres::PgPoolOptions;

    use super::*;

    const PRODUCTS_QTY: usize = 50;

    #[tokio::test]
    async fn find_all() {
        let repo = get_db_repo().await;
        let result = repo.find_all().await;
        let (elapsed, products) = result.unwrap();
        assert_eq!(products.len(), PRODUCTS_QTY);
        eprintln!("Query took: {:?}, result: {:?}", elapsed, products);
    }

    #[tokio::test]
    async fn find_all_by_status_is_active_true() {
        let repo = get_db_repo().await;
        let result = repo.find_all_by_status(true).await;
        let (_elapsed, products) = result.unwrap();
        assert_eq!(products.len(), PRODUCTS_QTY);
        //eprintln!("Query took: {:?}, result: {:?}", elapsed, products);
    }

    #[tokio::test]
    async fn find_all_by_status_is_active_false() {
        let repo = get_db_repo().await;
        let result = repo.find_all_by_status(false).await;
        let (_elapsed, products) = result.unwrap();
        assert_eq!(products.len(), 0);
        //eprintln!("Query took: {:?}, result: {:?}", elapsed, products);
    }

    async fn get_db_repo() -> ProductPropsRepository {
        let database_url = env::var("DATABASE_URL").unwrap();
        eprintln!("DATABASE_URL: {:?}", database_url);
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await
            .unwrap();
        ProductPropsRepository::new(pool)
    }
}
