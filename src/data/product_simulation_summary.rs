use chrono::NaiveDate;
use sqlx::{
    types::{BigDecimal, Uuid},
    FromRow, Pool, Postgres,
};
use std::time::{Duration, Instant};

#[derive(Debug, FromRow, Clone)]
pub struct NewProductSimulationSummary {
    pub id: i32,                                   // SERIAL,
    pub product_id: Uuid,                          // UUID REFERENCES product_props (id),
    pub probability_losses_by_missing: BigDecimal, // DECIMAL(3,3) NOT NULL,
    pub probability_losses_by_nospace: BigDecimal, // DECIMAL(3,3) NOT NULL,
    pub probability_losses_by_expirat: BigDecimal, // DECIMAL(3,3) NOT NULL,
    pub start_date: NaiveDate,                     // DATE NOT NULL,
    pub end_date: NaiveDate,                       // DATE NOT NULL,
    pub first_date_with_losses: Option<NaiveDate>, // DATE,
                                                   //pub created_at                    : , // TIMESTAMPTZ NOT NULL DEFAULT NOW(),
}

#[derive(Debug, FromRow, Clone)]
pub struct ProductSimulationSummary {
    pub id: i32,                                   // SERIAL,
    pub product_id: Uuid,                          // UUID REFERENCES product_props (id),
    pub probability_losses_by_missing: BigDecimal, // DECIMAL(3,3) NOT NULL,
    pub probability_losses_by_nospace: BigDecimal, // DECIMAL(3,3) NOT NULL,
    pub probability_losses_by_expirat: BigDecimal, // DECIMAL(3,3) NOT NULL,
    pub start_date: NaiveDate,                     // DATE NOT NULL,
    pub end_date: NaiveDate,                       // DATE NOT NULL,
    pub first_date_with_losses: Option<NaiveDate>, // DATE,
                                                   //pub created_at                    : , // TIMESTAMPTZ NOT NULL DEFAULT NOW(),
}

pub struct ProductSimulationSummaryRepository {
    db: Pool<Postgres>,
}

impl ProductSimulationSummaryRepository {
    pub fn new(db: Pool<Postgres>) -> ProductSimulationSummaryRepository {
        ProductSimulationSummaryRepository { db: db }
    }

    pub async fn find_all(
        &self,
    ) -> Result<(Duration, Vec<ProductSimulationSummary>), Box<dyn std::error::Error>> {
        let timer = Instant::now();

        let query = sqlx::query_as::<_, ProductSimulationSummary>(
            "
            SELECT
                id                            ,
                product_id                    ,
                probability_losses_by_missing ,
                probability_losses_by_nospace ,
                probability_losses_by_expirat ,
                start_date                    ,
                end_date                      ,
                first_date_with_losses        
            FROM product_simulation_summary;
        ",
        );

        let query_res = query.fetch_all(&self.db).await?;

        Ok((timer.elapsed(), query_res))
    }

    pub async fn find_all_by_product(
        &self,
        product_id: Uuid,
    ) -> Result<(Duration, Vec<ProductSimulationSummary>), Box<dyn std::error::Error>> {
        let timer = Instant::now();

        let query = sqlx::query_as::<_, ProductSimulationSummary>(
            "
            SELECT 
                id                            ,
                product_id                    ,
                probability_losses_by_missing ,
                probability_losses_by_nospace ,
                probability_losses_by_expirat ,
                start_date                    ,
                end_date                      ,
                first_date_with_losses
            FROM product_simulation_summary
            WHERE product_id = $1;
        ",
        );

        let query_res = query.bind(product_id).fetch_all(&self.db).await?;

        Ok((timer.elapsed(), query_res))
    }
}

#[cfg(test)]
mod tests {
    use std::{env, str::FromStr};

    use sqlx::postgres::PgPoolOptions;

    use super::*;

    #[tokio::test]
    async fn find_all() {
        let repo = get_db_repo().await;
        let result = repo.find_all().await;
        let (elapsed, products) = result.unwrap();
        assert_eq!(products.len(), 4);
        eprintln!("Query took: {:?}, result: {:?}", elapsed, products);
    }

    #[tokio::test]
    async fn find_all_by_product() {
        let repo = get_db_repo().await;
        let product_id = Uuid::from_str("b010b78b-3236-4ddb-b68e-d833eb75d8be").unwrap();
        let result = repo.find_all_by_product(product_id).await;
        let (elapsed, products) = result.unwrap();
        assert_eq!(products.len(), 2);
        eprintln!("Query took: {:?}, result: {:?}", elapsed, products);
    }

    async fn get_db_repo() -> ProductSimulationSummaryRepository {
        let database_url = env::var("DATABASE_URL").unwrap();
        eprintln!("DATABASE_URL: {:?}", database_url);
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await
            .unwrap();
        ProductSimulationSummaryRepository::new(pool)
    }
}
