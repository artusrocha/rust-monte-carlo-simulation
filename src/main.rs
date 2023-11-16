use sqlx::{
    database::HasArguments, postgres::PgPoolOptions, query::QueryAs, types::BigDecimal, FromRow,
    Pool, Postgres,
};

use std::{
    collections::{HashMap, LinkedList},
    env,
    time::Instant,
};
//use chrono::prelude::*;
use chrono::{Datelike, Days, NaiveDate};

#[derive(Debug, FromRow, Clone)]
struct Hist {
    item_id: i32,
    entry_qty: BigDecimal,
    withdrawal_qty: BigDecimal,
    week_of_year: i16,
    day_of_week: i16,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let reference_date = "2023-06-25";
    let days_to_analyze = 90;
    let item_id = 16489;
    let default_time_limit = 20;
    let stock_limit = BigDecimal::from(100);

    let (initial_date, final_date) = get_initial_and_final_dates(reference_date, days_to_analyze)?;
    let (initial_week, final_week) = get_initial_and_final_weeks(initial_date, final_date);

    let db = get_db_conn_pool().await?;

    let historic = do_query(item_id, initial_week, final_week, db).await?;

    let historic_by_woy_and_dow = group_by_woy_and_dow(&historic);
    //eprintln!("{:?}", &historic_by_week);

    let batch = SimulationItemBatch {
        quantity: BigDecimal::from(100),
        deadline_date: initial_date.checked_add_days(Days::new(10)).unwrap(),
    };

    let simulation_day0 = SimulationDay {
        date: initial_date,
        batches: LinkedList::from([batch]),
        stock_time_limit_exceeded: None,
        stock_shortage: None,
        stock_limit_exceeded: None,
    };

    let mut simulation = Simulation {
        item_id: item_id,
        initial_date: initial_date,
        days: vec![simulation_day0],
    };
    let mut date = initial_date;
    while date < final_date {
        eprintln!("========================================================");
        eprintln!("Date: {:?}", date);
        date = date
            .checked_add_days(Days::new(1))
            .expect(format!("Failure to determine next date after {:?}", date).as_str());
        let woy = date.iso_week().week() as i16;
        let dow = date.weekday().num_days_from_sunday() as i16;
        let date_hist_opt = historic_by_woy_and_dow
            .get(&woy)
            .and_then(|week| week.get(&dow));
        match date_hist_opt {
            Some(date_hist) => {
                let last_day = simulation.days.last();
                let last_batches = &last_day.unwrap().batches;
                eprintln!("last_batch.len(): {:?}", last_batches.len());
                let mut next_batches = last_batches.to_owned();
                eprintln!("1: next_batches.len(): {:?}", next_batches.len());
                eprintln!(
                    "front: {:?}, back: {:?}",
                    next_batches.front(),
                    next_batches.back()
                );

                // do withdraw mov
                let mut withdraw_qty = date_hist.withdrawal_qty.clone();
                eprintln!(
                    "before withdraw | withdraw_qty: {:?}, batches: {:?}",
                    withdraw_qty,
                    next_batches
                        .iter()
                        .map(|e| e.quantity.clone())
                        .reduce(|acc, e| acc + e)
                );
                while withdraw_qty > BigDecimal::from(0) && next_batches.len() > 0 {
                    match next_batches.front_mut() {
                        Some(old) => {
                            if old.quantity > withdraw_qty {
                                old.quantity = &old.quantity - withdraw_qty;
                                withdraw_qty = BigDecimal::from(0);
                            } else {
                                withdraw_qty = withdraw_qty - &old.quantity;
                                next_batches.pop_front();
                            }
                        }
                        None => {}
                    };
                }
                eprintln!(
                    "after withdraw | withdraw_qty: {:?}, batches: {:?}",
                    withdraw_qty,
                    next_batches
                        .iter()
                        .map(|e| e.quantity.clone())
                        .reduce(|acc, e| acc + e)
                );
                eprintln!("2: next_batches.len(): {:?}", next_batches.len());
                let stock_shortage = if withdraw_qty > BigDecimal::from(0) {
                    Some(withdraw_qty)
                } else {
                    None
                };

                // do entry mov
                let batches_qty_sum = next_batches
                    .iter()
                    .map(|e| e.quantity.clone())
                    .reduce(|acc, e| acc + e)
                    .unwrap_or(BigDecimal::from(0));
                eprintln!(
                    "before entry | entry_qty: {:?}, batches: {:?}",
                    date_hist.entry_qty, batches_qty_sum
                );
                let available = &stock_limit - batches_qty_sum;
                let (final_entry_qty, exceeded_entry_qty) = if available > date_hist.entry_qty {
                    (date_hist.entry_qty.clone(), BigDecimal::from(0))
                } else {
                    (available.clone(), (date_hist.entry_qty.clone() - available))
                };
                next_batches.push_back(SimulationItemBatch {
                    quantity: final_entry_qty,
                    deadline_date: date
                        .clone()
                        .checked_add_days(Days::new(default_time_limit))
                        .unwrap(),
                });
                eprintln!(
                    "after entry | entry_qty: {:?}, batches: {:?}",
                    date_hist.entry_qty,
                    next_batches
                        .iter()
                        .map(|e| e.quantity.clone())
                        .reduce(|acc, e| acc + e)
                );
                eprintln!("3: next_batches.len(): {:?}", next_batches.len());
                eprintln!(
                    "front: {:?}, back: {:?}",
                    next_batches.front(),
                    next_batches.back()
                );
                let stock_limit_exceeded = if exceeded_entry_qty > BigDecimal::from(0) {
                    Some(exceeded_entry_qty)
                } else {
                    None
                };

                // remove expired batch
                let mut removed_quantity = BigDecimal::from(0);
                next_batches = next_batches
                    .into_iter()
                    .filter(|e| {
                        let is_within_time = e.deadline_date >= date;
                        if ! is_within_time {
                            removed_quantity += e.quantity.clone();
                        }
                        is_within_time
                    })
                    .collect::<LinkedList<SimulationItemBatch>>();
                let stock_time_limit_exceeded = if removed_quantity > BigDecimal::from(0) {
                    Some(removed_quantity)
                } else {
                    None
                };

                let next_day = SimulationDay {
                    date,
                    batches: next_batches,
                    stock_shortage,
                    stock_limit_exceeded,
                    stock_time_limit_exceeded,
                };
                eprintln!("{:?}", &next_day);
                simulation.days.push(next_day);
            }
            None => {
                let default = get_default_hist(item_id.into(), dow);
                eprintln!("Default: {:?}", default);
            }
        }
    }

    Ok(())
}

fn get_default_hist(item_id: i32, dow: i16) -> Hist {
    Hist {
        item_id: item_id,
        entry_qty: BigDecimal::from(0),
        withdrawal_qty: BigDecimal::from(0),
        week_of_year: 0,
        day_of_week: dow,
    }
}

struct Simulation {
    item_id: i16,
    initial_date: NaiveDate,
    days: Vec<SimulationDay>,
}


#[derive(Debug)]
struct SimulationDay {
    date: NaiveDate,
    // entry_qty: BigDecimal,
    // withdrawal_qty: BigDecimal,
    batches: LinkedList<SimulationItemBatch>,
    stock_shortage: Option<BigDecimal>,
    stock_limit_exceeded: Option<BigDecimal>,
    stock_time_limit_exceeded: Option<BigDecimal>,
}

#[derive(Debug, Clone)]
struct SimulationItemBatch {
    quantity: BigDecimal,
    //    entry_date: NaiveDate,
    deadline_date: NaiveDate,
}

fn get_initial_and_final_weeks(initial_date: NaiveDate, final_date: NaiveDate) -> (u32, u32) {
    (initial_date.iso_week().week(), final_date.iso_week().week())
}

fn get_initial_and_final_dates(
    reference_date: &str,
    days_to_analyze: u64,
) -> Result<(NaiveDate, NaiveDate), Box<dyn std::error::Error>> {
    let initial_date = NaiveDate::parse_from_str(reference_date, "%Y-%m-%d")?;
    let final_date = initial_date
        .checked_add_days(Days::new(days_to_analyze))
        .expect("Failure to determine final_date");
    eprintln!(
        "initial_date: {:?}, final_date: {:?}",
        initial_date, final_date
    );
    Ok((initial_date, final_date))
}

async fn do_query(
    item_id: i16,
    initial_week: u32,
    final_week: u32,
    db: Pool<Postgres>,
) -> Result<Vec<Hist>, Box<dyn std::error::Error>> {
    let weeks = get_all_week_between(initial_week, final_week);
    eprintln!("weeks: ({:?})", weeks);
    let query = get_query();
    let start = Instant::now();
    let historic = query.bind(item_id).bind(&weeks[..]).fetch_all(&db).await?;
    let duration = start.elapsed();
    println!(
        "result set length: {:?}, elapsed {:?}",
        &historic.len(),
        duration
    );
    Ok(historic)
}

fn get_all_week_between(initial_value: u32, final_value: u32) -> Vec<i16> {
    let max = 53;
    let end = if initial_value < final_value {
        final_value + 1
    } else {
        final_value + max + 1
    };
    let range = (initial_value..end)
        .map(|v| if v > max { v - max } else { v })
        .map(|v| v as i16)
        .collect::<Vec<i16>>();
    range
}

fn get_query<'a>() -> QueryAs<'a, Postgres, Hist, <Postgres as HasArguments<'a>>::Arguments> {
    sqlx::query_as::<_, Hist>(
        "
        SELECT 
          item_id,
          AVG(entry_qty) AS entry_qty,
          AVG(withdrawal_qty) AS withdrawal_qty,
          week_of_year,
          day_of_week
        FROM item_mov_hist
        WHERE item_id = $1
        AND   week_of_year = ANY ($2)
        GROUP BY item_id, week_of_year, day_of_week
        ORDER BY week_of_year, day_of_week;
    ",
    )
}

async fn get_db_conn_pool() -> Result<Pool<Postgres>, Box<dyn std::error::Error>> {
    let database_url = env::var("DATABASE_URL")?;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;
    Ok(pool)
}

fn group_by_woy_and_dow(vec: &Vec<Hist>) -> HashMap<i16, HashMap<i16, &Hist>> {
    let mut map = HashMap::new();
    for e in vec {
        let week = map.entry(e.week_of_year).or_insert_with(HashMap::new);
        week.entry(e.day_of_week).or_insert(e);
    }
    map
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use chrono::Datelike;
    use sqlx::types::BigDecimal;

    use crate::{
        get_initial_and_final_dates, get_initial_and_final_weeks, group_by_woy_and_dow, Hist,
    };

    #[test]
    fn test_week_and_days_calc() {
        let reference_date = "2023-09-05";
        let days_to_analyze = 90;

        let (initial_date, final_date) =
            get_initial_and_final_dates(reference_date, days_to_analyze).unwrap();
        assert_eq!(initial_date.year(), 2023);
        assert_eq!(initial_date.month0(), 8);
        assert_eq!(initial_date.day0(), 4);
        assert_eq!(final_date.year(), 2023);
        assert_eq!(final_date.month0(), 11);
        assert_eq!(final_date.day0(), 3);

        let (initial_week, final_week) = get_initial_and_final_weeks(initial_date, final_date);
        assert_eq!(initial_week, 36);
        assert_eq!(final_week, 49);

        let reference_date = "2024-01-01";
        let days_to_analyze = 90;

        let (initial_date, final_date) =
            get_initial_and_final_dates(reference_date, days_to_analyze).unwrap();
        assert_eq!(initial_date.year(), 2024);
        assert_eq!(initial_date.month0(), 0);
        assert_eq!(initial_date.day0(), 0);
        assert_eq!(final_date.year(), 2024);
        assert_eq!(final_date.month0(), 2);
        assert_eq!(final_date.day0(), 30);

        let (initial_week, final_week) = get_initial_and_final_weeks(initial_date, final_date);
        assert_eq!(initial_week, 1);
        assert_eq!(final_week, 13);
    }

    #[test]
    fn test_group_by_woy_and_dow() {
        let historic = vec![
            Hist {
                item_id: 16489,
                entry_qty: BigDecimal::from_str("52.5000").unwrap(),
                withdrawal_qty: BigDecimal::from_str("73.3333").unwrap(),
                week_of_year: 32,
                day_of_week: 0,
            },
            Hist {
                item_id: 16489,
                entry_qty: BigDecimal::from_str("58.3333").unwrap(),
                withdrawal_qty: BigDecimal::from_str("82.1666").unwrap(),
                week_of_year: 32,
                day_of_week: 1,
            },
            Hist {
                item_id: 16489,
                entry_qty: BigDecimal::from_str("49.8333").unwrap(),
                withdrawal_qty: BigDecimal::from_str("65.5000").unwrap(),
                week_of_year: 32,
                day_of_week: 2,
            },
            Hist {
                item_id: 16489,
                entry_qty: BigDecimal::from_str("75.5000").unwrap(),
                withdrawal_qty: BigDecimal::from_str("56.1666").unwrap(),
                week_of_year: 32,
                day_of_week: 3,
            },
            Hist {
                item_id: 16489,
                entry_qty: BigDecimal::from_str("67.3333").unwrap(),
                withdrawal_qty: BigDecimal::from_str("56.1666").unwrap(),
                week_of_year: 32,
                day_of_week: 4,
            },
            Hist {
                item_id: 16489,
                entry_qty: BigDecimal::from_str("39.3333").unwrap(),
                withdrawal_qty: BigDecimal::from_str("52.6666").unwrap(),
                week_of_year: 32,
                day_of_week: 5,
            },
            Hist {
                item_id: 16489,
                entry_qty: BigDecimal::from_str("52.5000").unwrap(),
                withdrawal_qty: BigDecimal::from_str("73.3333").unwrap(),
                week_of_year: 32,
                day_of_week: 6,
            },
            Hist {
                item_id: 16489,
                entry_qty: BigDecimal::from_str("52.5000").unwrap(),
                withdrawal_qty: BigDecimal::from_str("73.3333").unwrap(),
                week_of_year: 33,
                day_of_week: 0,
            },
            Hist {
                item_id: 16489,
                entry_qty: BigDecimal::from_str("58.3333").unwrap(),
                withdrawal_qty: BigDecimal::from_str("82.1666").unwrap(),
                week_of_year: 33,
                day_of_week: 1,
            },
            Hist {
                item_id: 16489,
                entry_qty: BigDecimal::from_str("49.8333").unwrap(),
                withdrawal_qty: BigDecimal::from_str("65.5000").unwrap(),
                week_of_year: 33,
                day_of_week: 2,
            },
            Hist {
                item_id: 16489,
                entry_qty: BigDecimal::from_str("75.5000").unwrap(),
                withdrawal_qty: BigDecimal::from_str("56.1666").unwrap(),
                week_of_year: 33,
                day_of_week: 3,
            },
            Hist {
                item_id: 16489,
                entry_qty: BigDecimal::from_str("67.3333").unwrap(),
                withdrawal_qty: BigDecimal::from_str("56.1666").unwrap(),
                week_of_year: 33,
                day_of_week: 4,
            },
            Hist {
                item_id: 16489,
                entry_qty: BigDecimal::from_str("39.3333").unwrap(),
                withdrawal_qty: BigDecimal::from_str("52.6666").unwrap(),
                week_of_year: 33,
                day_of_week: 5,
            },
            Hist {
                item_id: 16489,
                entry_qty: BigDecimal::from_str("39.3333").unwrap(),
                withdrawal_qty: BigDecimal::from_str("52.6666").unwrap(),
                week_of_year: 33,
                day_of_week: 6,
            },
        ];
        let map = group_by_woy_and_dow(&historic);
        assert_eq!(map.len(), 2);
        assert_eq!(map.get(&32).unwrap().len(), 7);
        assert_eq!(
            map.get(&32).unwrap().get(&0).unwrap().entry_qty,
            BigDecimal::from_str("52.5000").unwrap()
        );
        assert_eq!(
            map.get(&33).unwrap().get(&3).unwrap().withdrawal_qty,
            BigDecimal::from_str("56.1666").unwrap()
        );
    }
}
