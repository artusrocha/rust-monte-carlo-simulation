pub mod repository;

use std::{time::Instant, collections::HashMap};
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgPoolOptions, query::QueryAs, types::BigDecimal, FromRow, Pool, Postgres};

pub struct ProductMovHist { 
    product_id: UUID,
    entry_qty: u32,
    withdrawal_qty: u32,
    week_of_year: u16,
    day_of_week: u16
}

pub struct ProductMovHistRepository {
    db: &Pool<Postgres>
}

impl ProductMovHistRepository {

    pub fn new(db: &Pool<Postgres>) -> ProductMovHistRepository {
        Repository {
            db: &db
        }
    }
    
    pub async fn find_all_by_product_id_and_week_range(
        &self,
        product_id: UUID,
        initial_week: u32,
        final_week: u32,
    ) -> Result<(Duration, Vec<ProductMovHist>), Box<dyn std::error::Error>> {
        let timer = Instant::now();

        let query = sqlx::query_as::<_, ProductMovHist>("
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