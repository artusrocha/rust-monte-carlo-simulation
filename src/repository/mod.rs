pub mod repository;

use std::{time::Instant, collections::HashMap};
use sqlx::{postgres::PgPoolOptions, types::BigDecimal, Pool, Postgres, FromRow};

pub struct Hist { 
    item_id: i32,
    entry_qty: BigDecimal,
    withdrawal_qty: BigDecimal,
    week_of_year: i16,
    day_of_week: i16
}

pub struct Repository {
    db: &Pool<Postgres>,
    query: sqlx::Query
}

impl Repository {

    pub fn new(db: &Pool<Postgres>) -> Repository {
        Repository {
            db: &db,
            query: sqlx::query_as::<_, Hist>("
                SELECT 
                  item_id,
                  AVG(entry_qty) AS entry_qty,
                  AVG(withdrawal_qty) AS withdrawal_qty,
                  week_of_year,
                  day_of_week
                FROM item_mov_hist
                WHERE item_id = $1
                AND   week_of_year >= $2
                AND   week_of_year <= $3
                GROUP BY item_id, week_of_year, day_of_week
                ORDER BY week_of_year, day_of_week;
            ")
        }
    }
    
    
    pub fn find_historic() -> Vec<Hist> {
        let start = Instant::now();
        let historic = query
            .bind(&item_id)
            .bind(30)
            .bind(43)
            .fetch_all( &db)
            .await?;
        let duration = start.elapsed();
        println!("result set length: {:?}, elapsed {:?}", &historic.len(), duration);
        historic
    }

}