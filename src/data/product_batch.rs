use std::time::{Duration, Instant};
use chrono::Utc;
use sqlx::{types::{BigDecimal, Uuid, chrono::DateTime}, FromRow, Pool, Postgres};

#[derive(Debug, FromRow, Clone)]
pub struct ProductBatch {
    pub id            : i32                   , // SERIAL,
    pub product_id    : Uuid                  , // UUID REFERENCES product_props (id),
    pub entry_date    : DateTime<Utc>         , // TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    pub deadline_date : DateTime<Utc>         , // TIMESTAMPTZ NOT NULL,
    pub finished_date : Option<DateTime<Utc>> , // TIMESTAMPTZ,
    pub is_finished   : bool                  , // BOOLEAN NOT NULL GENERATED ALWAYS AS (finished_date IS NOT NULL) STORED,
    pub quantity      : i32                   , // INTEGER NOT NULL CHECK (quantity >= 0) DEFAULT 0,
    // pub created_at    , // TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    // pub updated_at    , // TIMESTAMPTZ NOT NULL DEFAULT NOW(),
}// 

pub struct ProductBatchRepository {
    db: Pool<Postgres>
}

impl ProductBatchRepository {

    pub fn new(db: Pool<Postgres>) -> ProductBatchRepository {
        ProductBatchRepository {
            db: db
        }
    }
    
    pub async fn find_all(
        &self,
    ) -> Result<(Duration, Vec<ProductBatch>), Box<dyn std::error::Error>> {
        let timer = Instant::now();

        let query = sqlx::query_as::<_, ProductBatch>("
            SELECT
              id           ,
              product_id   ,
              entry_date   ,
              deadline_date,
              finished_date,
              is_finished  ,
              quantity
            FROM product_batch;
        ");

        let query_res = query.fetch_all(&self.db).await?;

        Ok((timer.elapsed(), query_res))
    }

    pub async fn find_all_by_product(
        &self,
        product_id: Uuid,
    ) -> Result<(Duration, Vec<ProductBatch>), Box<dyn std::error::Error>> {
        let timer = Instant::now();

        let query = sqlx::query_as::<_, ProductBatch>("
            SELECT
              id           ,
              product_id   ,
              entry_date   ,
              deadline_date,
              finished_date,
              is_finished  ,
              quantity
            FROM product_batch
            WHERE product_id = $1;
        ");

        let query_res = query.bind(product_id).fetch_all(&self.db).await?;

        Ok((timer.elapsed(), query_res))
    }


}

#[cfg(test)]
mod tests {
    use std::env;

    use sqlx::postgres::PgPoolOptions;

    use super::*;

    const BATCHES_QTY : usize = 750;

    const BATCHES_BY_PRODUCT_QTY : usize = 15;

    #[tokio::test]
    async fn find_all() {
        let repo = get_db_repo().await;
        let result = repo.find_all().await;
        let (elapsed, products) = result.unwrap();
        assert_eq!(products.len(), BATCHES_QTY);
        eprintln!("Query took: {:?}, result: {:?}", elapsed, products);
    }

    #[tokio::test]
    async fn find_all_by_product() {
        let product_id : Uuid = Uuid::parse_str("d0bd335e-fc46-408d-90fb-209ccc521fa1").unwrap(); 
        let repo = get_db_repo().await;
        let result = repo.find_all_by_product(product_id).await;
        let (_elapsed, products) = result.unwrap();
        assert_eq!(products.len(), BATCHES_BY_PRODUCT_QTY);
        //eprintln!("Query took: {:?}, result: {:?}", elapsed, products);
    }

    async fn get_db_repo() -> ProductBatchRepository {
        let database_url = env::var("DATABASE_URL").unwrap();
        eprintln!("DATABASE_URL: {:?}", database_url);
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await.unwrap();
        ProductBatchRepository::new(pool)
    }
}