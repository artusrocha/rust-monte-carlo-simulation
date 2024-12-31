use crate::data::product_mov_hist::ProductMovHist;

use sqlx::types::BigDecimal;
use uuid::Uuid;

use std::{collections::HashMap, str::FromStr};

use chrono::{DateTime, Datelike, Utc};

#[derive(Debug)]
pub struct SimulationParameters {
    initial_date: DateTime<Utc>,
    pub stock_limit: u64,
    pub time_limit: u64,
    historic_by_woy_and_dow: HashMap<i16, HashMap<i16, ProductMovHist>>,
    default_hist: ProductMovHist,
}

impl SimulationParameters {
    pub fn get_date_hist(&self, date: &DateTime<Utc>) -> &ProductMovHist {
        let woy = date.iso_week().week() as i16;
        let dow = date.weekday().number_from_monday() as i16;
        let date_hist_opt = self
            .historic_by_woy_and_dow
            .get(&woy)
            .and_then(|week| week.get(&dow));
        match date_hist_opt {
            Some(hist) => hist,
            None => &self.default_hist,
        }
    }

    pub fn new(
        initial_date: DateTime<Utc>,
        stock_limit: u64,
        time_limit: u64,
        historic: Vec<ProductMovHist>,
    ) -> Self {
        Self {
            initial_date: initial_date,
            stock_limit: stock_limit,
            time_limit: time_limit,
            historic_by_woy_and_dow: Self::group_by_woy_and_dow(historic),
            default_hist: Self::get_default_hist(),
        }
    }

    fn group_by_woy_and_dow(
        vec: Vec<ProductMovHist>,
    ) -> HashMap<i16, HashMap<i16, ProductMovHist>> {
        let mut map = HashMap::new();
        for e in vec {
            let week = map.entry(e.week_of_year).or_insert_with(HashMap::new);
            week.entry(e.day_of_week).or_insert(e);
        }
        map
    }

    fn get_default_hist() -> ProductMovHist {
        ProductMovHist {
            product_id: Uuid::from_str("c45bd212-8fb8-449b-8cb9-1fb44c0a6cd4").unwrap(),
            entry_qty: BigDecimal::from(0),
            withdrawal_qty: BigDecimal::from(0),
            week_of_year: 0,
            day_of_week: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use sqlx::types::BigDecimal;

    use super::*;

    #[test]
    fn test_group_by_woy_and_dow() {
        let historic = vec![
            ProductMovHist {
                product_id: Uuid::from_str("d0bd335e-fc46-408d-90fb-209ccc521fa1").unwrap(),
                entry_qty: BigDecimal::from_str("52.5000").unwrap(),
                withdrawal_qty: BigDecimal::from_str("73.3333").unwrap(),
                week_of_year: 32,
                day_of_week: 0,
            },
            ProductMovHist {
                product_id: Uuid::from_str("d0bd335e-fc46-408d-90fb-209ccc521fa1").unwrap(),
                entry_qty: BigDecimal::from_str("58.3333").unwrap(),
                withdrawal_qty: BigDecimal::from_str("82.1666").unwrap(),
                week_of_year: 32,
                day_of_week: 1,
            },
            ProductMovHist {
                product_id: Uuid::from_str("d0bd335e-fc46-408d-90fb-209ccc521fa1").unwrap(),
                entry_qty: BigDecimal::from_str("49.8333").unwrap(),
                withdrawal_qty: BigDecimal::from_str("65.5000").unwrap(),
                week_of_year: 32,
                day_of_week: 2,
            },
            ProductMovHist {
                product_id: Uuid::from_str("d0bd335e-fc46-408d-90fb-209ccc521fa1").unwrap(),
                entry_qty: BigDecimal::from_str("75.5000").unwrap(),
                withdrawal_qty: BigDecimal::from_str("56.1666").unwrap(),
                week_of_year: 32,
                day_of_week: 3,
            },
            ProductMovHist {
                product_id: Uuid::from_str("d0bd335e-fc46-408d-90fb-209ccc521fa1").unwrap(),
                entry_qty: BigDecimal::from_str("67.3333").unwrap(),
                withdrawal_qty: BigDecimal::from_str("56.1666").unwrap(),
                week_of_year: 32,
                day_of_week: 4,
            },
            ProductMovHist {
                product_id: Uuid::from_str("d0bd335e-fc46-408d-90fb-209ccc521fa1").unwrap(),
                entry_qty: BigDecimal::from_str("39.3333").unwrap(),
                withdrawal_qty: BigDecimal::from_str("52.6666").unwrap(),
                week_of_year: 32,
                day_of_week: 5,
            },
            ProductMovHist {
                product_id: Uuid::from_str("d0bd335e-fc46-408d-90fb-209ccc521fa1").unwrap(),
                entry_qty: BigDecimal::from_str("52.5000").unwrap(),
                withdrawal_qty: BigDecimal::from_str("73.3333").unwrap(),
                week_of_year: 32,
                day_of_week: 6,
            },
            ProductMovHist {
                product_id: Uuid::from_str("d0bd335e-fc46-408d-90fb-209ccc521fa1").unwrap(),
                entry_qty: BigDecimal::from_str("52.5000").unwrap(),
                withdrawal_qty: BigDecimal::from_str("73.3333").unwrap(),
                week_of_year: 33,
                day_of_week: 0,
            },
            ProductMovHist {
                product_id: Uuid::from_str("d0bd335e-fc46-408d-90fb-209ccc521fa1").unwrap(),
                entry_qty: BigDecimal::from_str("58.3333").unwrap(),
                withdrawal_qty: BigDecimal::from_str("82.1666").unwrap(),
                week_of_year: 33,
                day_of_week: 1,
            },
            ProductMovHist {
                product_id: Uuid::from_str("d0bd335e-fc46-408d-90fb-209ccc521fa1").unwrap(),
                entry_qty: BigDecimal::from_str("49.8333").unwrap(),
                withdrawal_qty: BigDecimal::from_str("65.5000").unwrap(),
                week_of_year: 33,
                day_of_week: 2,
            },
            ProductMovHist {
                product_id: Uuid::from_str("d0bd335e-fc46-408d-90fb-209ccc521fa1").unwrap(),
                entry_qty: BigDecimal::from_str("75.5000").unwrap(),
                withdrawal_qty: BigDecimal::from_str("56.1666").unwrap(),
                week_of_year: 33,
                day_of_week: 3,
            },
            ProductMovHist {
                product_id: Uuid::from_str("d0bd335e-fc46-408d-90fb-209ccc521fa1").unwrap(),
                entry_qty: BigDecimal::from_str("67.3333").unwrap(),
                withdrawal_qty: BigDecimal::from_str("56.1666").unwrap(),
                week_of_year: 33,
                day_of_week: 4,
            },
            ProductMovHist {
                product_id: Uuid::from_str("d0bd335e-fc46-408d-90fb-209ccc521fa1").unwrap(),
                entry_qty: BigDecimal::from_str("39.3333").unwrap(),
                withdrawal_qty: BigDecimal::from_str("52.6666").unwrap(),
                week_of_year: 33,
                day_of_week: 5,
            },
            ProductMovHist {
                product_id: Uuid::from_str("d0bd335e-fc46-408d-90fb-209ccc521fa1").unwrap(),
                entry_qty: BigDecimal::from_str("39.3333").unwrap(),
                withdrawal_qty: BigDecimal::from_str("52.6666").unwrap(),
                week_of_year: 33,
                day_of_week: 6,
            },
        ];
        let map = SimulationParameters::group_by_woy_and_dow(historic);
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
