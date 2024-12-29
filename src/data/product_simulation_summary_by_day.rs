use std::time::{Duration, Instant};
use chrono::NaiveDate;
use sqlx::{types::BigDecimal, FromRow, Pool, Postgres};

#[derive(Debug, FromRow, Clone)]
pub struct ProductSimulationSummaryByDay { 
    pub product_simulation_summary_id : i32, // INTEGER NOT NULL,
    pub date                          : NaiveDate, // DATE NOT NULL,
    pub probability_losses_by_missing : BigDecimal, // DECIMAL(3,3) NOT NULL,
    pub probability_losses_by_nospace : BigDecimal, // DECIMAL(3,3) NOT NULL,
    pub probability_losses_by_expirat : BigDecimal, // DECIMAL(3,3) NOT NULL,
    //pub created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
}

pub struct ProductSimulationSummaryByDayRepository {
    db: Pool<Postgres>
}

impl ProductSimulationSummaryByDayRepository {

    pub fn new(db: Pool<Postgres>) -> ProductSimulationSummaryByDayRepository {
        ProductSimulationSummaryByDayRepository {
            db: db
        }
    }
    
    pub async fn find_all_by_product_simulation_summary(
        &self,
        product_simulation_summary_id: i32,
    ) -> Result<(Duration, Vec<ProductSimulationSummaryByDay>), Box<dyn std::error::Error>> {
        let timer = Instant::now();

        let query = sqlx::query_as::<_, ProductSimulationSummaryByDay>("
            SELECT
                product_simulation_summary_id ,
                date                          ,
                probability_losses_by_missing ,
                probability_losses_by_nospace ,
                probability_losses_by_expirat
            FROM product_simulation_summary_by_day
            WHERE product_simulation_summary_id = $1;
        ");

        let query_res = query.bind(product_simulation_summary_id).fetch_all(&self.db).await?;

        Ok((timer.elapsed(), query_res))
    }

}

#[cfg(test)]
mod tests {
    use std::env;

    use sqlx::postgres::PgPoolOptions;

    use super::*;

    #[tokio::test]
    async fn find_all_by_product_simulation_summary() {
        let repo = get_db_repo().await;
        let result = repo.find_all_by_product_simulation_summary(1).await;
        let (elapsed, products) = result.unwrap();
        assert_eq!(products.len(), 3);
        eprintln!("Query took: {:?}, result: {:?}", elapsed, products);
    }

    async fn get_db_repo() -> ProductSimulationSummaryByDayRepository {
        let database_url = env::var("DATABASE_URL").unwrap();
        eprintln!("DATABASE_URL: {:?}", database_url);
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await.unwrap();
        ProductSimulationSummaryByDayRepository::new(pool)
    }
}