use std::time::{Duration, Instant};
use sqlx::{types::{BigDecimal, Uuid}, FromRow, Pool, Postgres};


#[derive(Debug, FromRow, Clone)]
pub struct ProductMovHist {
    pub product_id: Uuid,
    pub entry_qty: BigDecimal,
    pub withdrawal_qty: BigDecimal,
    pub week_of_year: i16,
    pub day_of_week: i16
}

pub struct ProductMovHistRepository {
    db: Pool<Postgres>
}

impl ProductMovHistRepository {

    pub fn new(db: Pool<Postgres>) -> ProductMovHistRepository {
        ProductMovHistRepository {
            db: db
        }
    }
    
    pub async fn aggregate_by_product_id_and_week_of_year_and_day_of_week(
        &self,
        product_id: Uuid,
        initial_week: i16,
        final_week: i16,
    ) -> Result<(Duration, Vec<ProductMovHist>), Box<dyn std::error::Error>> {
        let timer = Instant::now();

        let query= sqlx::query_as::<_, ProductMovHist>("
            SELECT 
                product_id,
                AVG(entry_qty) AS entry_qty,
                AVG(withdrawal_qty) AS withdrawal_qty,
                week_of_year,
                day_of_week
            FROM product_mov_hist
            WHERE product_id = $1
            AND   week_of_year >= $2
            AND   week_of_year <= $3
            GROUP BY product_id, week_of_year, day_of_week
            ORDER BY week_of_year, day_of_week;
        ");

        let query_res = query
            .bind(product_id)
            .bind(initial_week)
            .bind(final_week)
            .fetch_all(&self.db).await?;

        Ok((timer.elapsed(), query_res))
    }


}

#[cfg(test)]
mod tests {
    use std::env;

    use sqlx::postgres::PgPoolOptions;

    use super::*;

    const DAYS_IN_THE_PERIOD : usize = 35;
    const FIRST_WEEK : i16 = 1;
    const LAST_WEEK : i16 = 5;

    #[tokio::test]
    async fn aggregate_by_product_id_and_week_of_year_and_day_of_week() {
        let repo = get_db_repo().await;
        let result = repo.aggregate_by_product_id_and_week_of_year_and_day_of_week(
            Uuid::parse_str("d0bd335e-fc46-408d-90fb-209ccc521fa1").unwrap(),
            FIRST_WEEK,
            LAST_WEEK,
        ).await;
        let (elapsed, hist) = result.unwrap();
        assert_eq!(hist.len(), DAYS_IN_THE_PERIOD);
        eprintln!("Query took: {:?}, result: {:?}", elapsed, hist);
    }

    #[tokio::test]
    async fn aggregate_by_product_id_and_week_of_year_and_day_of_week_no_results() {
        let repo = get_db_repo().await;
        let result = repo.aggregate_by_product_id_and_week_of_year_and_day_of_week(
            Uuid::parse_str("d0bd335e-fc46-408d-90fb-000000000000").unwrap(),
            FIRST_WEEK,
            LAST_WEEK,
        ).await;
        let (elapsed, hist) = result.unwrap();
        assert_eq!(hist.len(), 0);
        eprintln!("Query took: {:?}, result: {:?}", elapsed, hist);
    }

    async fn get_db_repo() -> ProductMovHistRepository {
        let database_url = env::var("DATABASE_URL").unwrap();
        eprintln!("DATABASE_URL: {:?}", database_url);
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await.unwrap();
        ProductMovHistRepository::new(pool)
    }
}