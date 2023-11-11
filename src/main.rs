use std::{env, time::Instant, collections::HashMap, str::FromStr};
use sqlx::{postgres::PgPoolOptions, types::BigDecimal, Pool, Postgres, FromRow};
//use chrono::prelude::*;
use chrono::{NaiveDate, DateTime, Utc, Local, TimeZone};

#[derive(Debug, FromRow)]
struct Hist { 
    item_id: i32,
    entry_qty: BigDecimal,
    withdrawal_qty: BigDecimal,
    week_of_year: i16,
    day_of_week: i16
 }

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let reference_date = NaiveDate::parse_from_str( "2023-09-05", "%Y-%m-%d")?;
    eprintln!("{:?}", reference_date);

    let item_id: i64 = 16489;

    let db = get_db_conn_pool().await?; 

    let query = sqlx::query_as::<_, Hist>("
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
    ");

    let start = Instant::now();
    let historic = query
        .bind(&item_id)
        .bind(30)
        .bind(43)
        .fetch_all( &db)
        .await?;
    let duration = start.elapsed();
    println!("result set length: {:?}, elapsed {:?}", &historic.len(), duration);

    let historic_by_woy_and_dow = group_by_woy_and_dow(&historic);
//    eprintln!("{:?}", &historic_by_week);



    Ok(())
}

async fn get_db_conn_pool() -> Result<Pool<Postgres>, Box<dyn std::error::Error>> {
    let database_url = env::var("DATABASE_URL")?;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;
    Ok(pool)
}

fn group_by_woy_and_dow(vec: &Vec<Hist>) -> HashMap<i16, HashMap<i16, Vec<&Hist>>> {
    let mut map = HashMap::new();
    for e in vec {
        let week = map.entry(e.week_of_year).or_insert_with(|| HashMap::new());
        let day_of_week = week.entry(e.day_of_week).or_insert_with(|| Vec::new());
        day_of_week.push(e);
    }
    map
}

#[test]
fn test_group_by_woy_and_dow() {
    let historic = vec!(
        Hist { item_id: 16489, entry_qty: BigDecimal::from_str("52.5000").unwrap(), withdrawal_qty: BigDecimal::from_str("73.3333").unwrap(), week_of_year: 32, day_of_week: 0 },
        Hist { item_id: 16489, entry_qty: BigDecimal::from_str("58.3333").unwrap(), withdrawal_qty: BigDecimal::from_str("82.1666").unwrap(), week_of_year: 32, day_of_week: 1 },
        Hist { item_id: 16489, entry_qty: BigDecimal::from_str("49.8333").unwrap(), withdrawal_qty: BigDecimal::from_str("65.5000").unwrap(), week_of_year: 32, day_of_week: 2 },
        Hist { item_id: 16489, entry_qty: BigDecimal::from_str("75.5000").unwrap(), withdrawal_qty: BigDecimal::from_str("56.1666").unwrap(), week_of_year: 32, day_of_week: 3 },
        Hist { item_id: 16489, entry_qty: BigDecimal::from_str("67.3333").unwrap(), withdrawal_qty: BigDecimal::from_str("56.1666").unwrap(), week_of_year: 32, day_of_week: 4 },
        Hist { item_id: 16489, entry_qty: BigDecimal::from_str("39.3333").unwrap(), withdrawal_qty: BigDecimal::from_str("52.6666").unwrap(), week_of_year: 32, day_of_week: 5 },
        Hist { item_id: 16489, entry_qty: BigDecimal::from_str("52.5000").unwrap(), withdrawal_qty: BigDecimal::from_str("73.3333").unwrap(), week_of_year: 32, day_of_week: 6 },
        Hist { item_id: 16489, entry_qty: BigDecimal::from_str("52.5000").unwrap(), withdrawal_qty: BigDecimal::from_str("73.3333").unwrap(), week_of_year: 33, day_of_week: 0 },
        Hist { item_id: 16489, entry_qty: BigDecimal::from_str("58.3333").unwrap(), withdrawal_qty: BigDecimal::from_str("82.1666").unwrap(), week_of_year: 33, day_of_week: 1 },
        Hist { item_id: 16489, entry_qty: BigDecimal::from_str("49.8333").unwrap(), withdrawal_qty: BigDecimal::from_str("65.5000").unwrap(), week_of_year: 33, day_of_week: 2 },
        Hist { item_id: 16489, entry_qty: BigDecimal::from_str("75.5000").unwrap(), withdrawal_qty: BigDecimal::from_str("56.1666").unwrap(), week_of_year: 33, day_of_week: 3 },
        Hist { item_id: 16489, entry_qty: BigDecimal::from_str("67.3333").unwrap(), withdrawal_qty: BigDecimal::from_str("56.1666").unwrap(), week_of_year: 33, day_of_week: 4 },
        Hist { item_id: 16489, entry_qty: BigDecimal::from_str("39.3333").unwrap(), withdrawal_qty: BigDecimal::from_str("52.6666").unwrap(), week_of_year: 33, day_of_week: 5 },
        Hist { item_id: 16489, entry_qty: BigDecimal::from_str("39.3333").unwrap(), withdrawal_qty: BigDecimal::from_str("52.6666").unwrap(), week_of_year: 33, day_of_week: 6 },
    );
    let map = group_by_woy_and_dow(&historic);
    assert_eq!(map.len(), 2);
    assert_eq!(map.get(&32).unwrap().len(), 7);
    assert_eq!(map.get(&32).unwrap().get(&0).unwrap().get(0).unwrap().entry_qty, BigDecimal::from_str("52.5000").unwrap());
    assert_eq!(map.get(&33).unwrap().get(&3).unwrap().get(0).unwrap().withdrawal_qty, BigDecimal::from_str("56.1666").unwrap());
}
