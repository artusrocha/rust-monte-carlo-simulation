mod parameter;
mod per_day;

use std::collections::HashMap;
use std::str::FromStr;

use crate::data::product_simulation_summary_by_day::NewProductSimulationSummaryByDay;
use crate::data::{product_batch::ProductBatch, product_mov_hist::ProductMovHist};

use crate::simulation::control::{parameter::SimulationParameters, per_day::SimulationDay};

use chrono::{DateTime, Utc};

use sqlx::types::BigDecimal;
use uuid::Uuid;

pub(crate) struct SimulationControl {
    pub(crate) product_id: Uuid,
    pub(crate) first_day: SimulationDay,
    pub(crate) final_date: DateTime<Utc>,
    pub(crate) sim_param: SimulationParameters,
}

impl SimulationControl {
    pub(crate) fn new(
        product_id: Uuid,
        initial_date: DateTime<Utc>,
        final_date: DateTime<Utc>,
        stock_maximum_quantity: u64,
        new_batch_default_expiration_days: u64,
        product_batches: Vec<ProductBatch>,
        historic: Vec<ProductMovHist>,
    ) -> Self {
        let sim_param = SimulationParameters::new(
            stock_maximum_quantity,
            new_batch_default_expiration_days,
            historic,
        );
        let simulation_day0 = SimulationDay {
            date: initial_date,
            batches: product_batches,
            stock_time_limit_exceeded: None,
            stock_shortage: None,
            stock_limit_exceeded: None,
            is_calculated: false,
        };
        SimulationControl {
            product_id,
            first_day: simulation_day0,
            final_date: final_date,
            sim_param: sim_param,
        }
    }

    pub(crate) fn has_next_date(&self, days: &Vec<SimulationDay>) -> bool {
        days.last()
            .is_some_and(|day| day.is_calculated && day.date < self.final_date)
    }

    pub(crate) fn run_once(&self) -> Vec<SimulationDay> {
        let mut first_day = self.first_day.clone();
        let mut is_last_calculated = first_day.calculate(&self.sim_param);
        let mut days = vec![first_day];
        while is_last_calculated && self.has_next_date(&days) {
            is_last_calculated = days
                .last()
                .and_then(|last_day| last_day.create_next())
                .map(|mut next_day| (next_day.calculate(&self.sim_param), next_day))
                .map(|(is_calculated, next_day)| {
                    if is_calculated {
                        days.push(next_day)
                    } else {
                        eprintln!("next_day calculation error.");
                    }
                    is_calculated
                })
                .unwrap_or(false);
        }
        days
    }

    pub(crate) fn run_n_times(&self, n_times: u64) {
        let mut group_by_date: HashMap<DateTime<Utc>, SimulationDayCounter> = HashMap::new();
        for _n in 0..n_times {
            let days: Vec<SimulationDay> = self.run_once();
            for day in days {
                group_by_date
                    .entry(day.date)
                    .or_insert_with(|| SimulationDayCounter::new(day.date))
                    .add(day);
            }
        }
        let daily_summaries_map: HashMap<DateTime<Utc>, Option<NewProductSimulationSummaryByDay>> =
            group_by_date
                .into_iter()
                .map(|(date, counter)| (date, counter.summarize()))
                .collect();
    }
}

struct SimulationDayCounter {
    date: DateTime<Utc>,
    all: Vec<SimulationDay>,
    with_losses_by_missing: Vec<usize>, // Vec<&'a SimulationDay>,
    with_losses_by_nospace: Vec<usize>, // Vec<&'a SimulationDay>,
    with_losses_by_expirat: Vec<usize>, // Vec<&'a SimulationDay>,
}

impl SimulationDayCounter {
    fn new(date: DateTime<Utc>) -> Self {
        Self {
            date,
            all: Vec::new(),
            with_losses_by_missing: Vec::new(),
            with_losses_by_nospace: Vec::new(),
            with_losses_by_expirat: Vec::new(),
        }
    }

    fn add(&mut self, day: SimulationDay) {
        self.all.push(day);
        let i = self.all.len() - 1;
        if self.all[i].stock_shortage.is_some() {
            self.with_losses_by_missing.push(i);
            // self.with_losses_by_missing.push(&self.all[i]);
        }
        if self.all[i].stock_limit_exceeded.is_some() {
            self.with_losses_by_nospace.push(i);
            // self.with_losses_by_nospace.push(&self.all[i]);
        }
        if self.all[i].stock_time_limit_exceeded.is_some() {
            self.with_losses_by_expirat.push(i);
            // self.with_losses_by_expirat.push(&self.all[i]);
        }
    }

    fn summarize(&self) -> Option<NewProductSimulationSummaryByDay> {
        //TODO improve error handling
        match self.try_summarize() {
            Ok(s) => Some(s),
            Err(err) => {
                eprintln!(
                    "Error while trying to summarize date: {:?}, Error: {:?}",
                    self.date, err
                );
                None
            }
        }
    }

    fn try_summarize(
        &self,
    ) -> Result<NewProductSimulationSummaryByDay, Box<dyn std::error::Error>> {
        if self.all.len() == 0 {
            return Err("Empty vec. Division by zero is not allowed!"
                .to_owned()
                .into());
        }
        Ok(NewProductSimulationSummaryByDay {
            date: self.date.date_naive(),
            probability_losses_by_missing: BigDecimal::from_str(
                &(self.with_losses_by_missing.len() as f64 / self.all.len() as f64).to_string(),
            )?,
            probability_losses_by_nospace: BigDecimal::from_str(
                &(self.with_losses_by_nospace.len() as f64 / self.all.len() as f64).to_string(),
            )?,
            probability_losses_by_expirat: BigDecimal::from_str(
                &(self.with_losses_by_expirat.len() as f64 / self.all.len() as f64).to_string(),
            )?,
        })
    }
}

#[cfg(test)]
mod tests {

    use sqlx::types::BigDecimal;

    use super::*;

    #[test]
    fn should_finish_with_batch_len_10_and_batches_qty_sum_100() {
        let simulation = SimulationControl::new(
            Uuid::from_u128(0),
            DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
                .unwrap()
                .to_utc(),
            DateTime::parse_from_rfc3339("2024-01-10T00:00:00Z")
                .unwrap()
                .to_utc(),
            1000,
            11,
            mock_product_batches(),
            vec![
                mock_historic(10, 10, 0, 1), // 2024-01-01 mon
                mock_historic(10, 10, 0, 2), // 2024-01-02 tur
                mock_historic(10, 10, 0, 3), // 2024-01-03 wed
                mock_historic(10, 10, 0, 4), // 2024-01-04 thu
                mock_historic(10, 10, 0, 5), // 2024-01-05 fry
                mock_historic(10, 10, 0, 6), // 2024-01-06 sat
                mock_historic(10, 10, 0, 0), // 2024-01-07 sun
                mock_historic(10, 10, 1, 1), // 2024-01-08 mon
                mock_historic(10, 10, 1, 2), // 2024-01-09 tur
                mock_historic(10, 10, 1, 3), // 2024-01-10 wed
            ],
        );

        let days = simulation.run_once();
        let last_day = days.last().unwrap();
        let total_qty = last_day
            .batches
            .iter()
            .map(|batch| batch.quantity.clone())
            .reduce(|acc, batch_qty| acc + batch_qty)
            .unwrap();
        assert_eq!(last_day.batches.len(), 10);
        assert_eq!(total_qty, BigDecimal::from(100));
    }

    #[test]
    fn should_finish_with_batch_len_6_and_batches_qty_sum_60() {
        let simulation = SimulationControl::new(
            Uuid::from_u128(0),
            DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
                .unwrap()
                .to_utc(),
            DateTime::parse_from_rfc3339("2024-01-10T00:00:00Z")
                .unwrap()
                .to_utc(),
            1000,
            5,
            mock_product_batches(),
            vec![
                mock_historic(10, 10, 0, 0), // 2024-01-01 mon
                mock_historic(10, 10, 0, 1), // 2024-01-02 tur
                mock_historic(10, 10, 0, 2), // 2024-01-03 wed
                mock_historic(10, 10, 0, 3), // 2024-01-04 thu
                mock_historic(10, 10, 0, 4), // 2024-01-05 fry
                mock_historic(10, 10, 0, 5), // 2024-01-06 sat
                mock_historic(10, 10, 0, 6), // 2024-01-07 sun
                mock_historic(10, 10, 1, 1), // 2024-01-08 mon
                mock_historic(10, 10, 1, 2), // 2024-01-09 tur
                mock_historic(10, 10, 1, 3), // 2024-01-10 wed
            ],
        );

        let days = simulation.run_once();
        let last_day = days.last().unwrap();
        let total_qty = last_day
            .batches
            .iter()
            .map(|batch| batch.quantity.clone())
            .reduce(|acc, batch_qty| acc + batch_qty)
            .unwrap();
        assert_eq!(last_day.batches.len(), 6);
        assert_eq!(total_qty, BigDecimal::from(60));
    }

    #[test]
    fn should_finish_with_batch_len_7_and_batches_qty_sum_110() {
        let simulation = SimulationControl::new(
            Uuid::from_u128(0),
            DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
                .unwrap()
                .to_utc(),
            DateTime::parse_from_rfc3339("2024-01-10T00:00:00Z")
                .unwrap()
                .to_utc(),
            1000,
            5,
            mock_product_batches(),
            vec![
                mock_historic(10, 5, 0, 0), // 2024-01-01 mon
                mock_historic(10, 5, 0, 1), // 2024-01-02 tur
                mock_historic(10, 5, 0, 2), // 2024-01-03 wed
                mock_historic(10, 5, 0, 3), // 2024-01-04 thu
                mock_historic(10, 5, 0, 4), // 2024-01-05 fry
                mock_historic(10, 5, 0, 5), // 2024-01-06 sat
                mock_historic(10, 5, 0, 6), // 2024-01-07 sun
                mock_historic(10, 5, 1, 1), // 2024-01-08 mon
                mock_historic(10, 5, 1, 2), // 2024-01-09 tur
                mock_historic(10, 5, 1, 3), // 2024-01-10 wed
            ],
        );

        let days = simulation.run_once();
        let last_day = days.last().unwrap();
        let total_qty = last_day
            .batches
            .iter()
            .map(|batch| batch.quantity.clone())
            .reduce(|acc, batch_qty| acc + batch_qty)
            .unwrap();
        assert_eq!(last_day.batches.len(), 7);
        assert_eq!(total_qty, BigDecimal::from(110));
    }

    #[test]
    fn should_finish_with_batch_len_6_and_batches_qty_sum_30() {
        let simulation = SimulationControl::new(
            Uuid::from_u128(0),
            DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
                .unwrap()
                .to_utc(),
            DateTime::parse_from_rfc3339("2024-01-10T00:00:00Z")
                .unwrap()
                .to_utc(),
            1000,
            5,
            mock_product_batches(),
            vec![
                mock_historic(5, 10, 0, 0), // 2024-01-01 mon
                mock_historic(5, 10, 0, 1), // 2024-01-02 tur
                mock_historic(5, 10, 0, 2), // 2024-01-03 wed
                mock_historic(5, 10, 0, 3), // 2024-01-04 thu
                mock_historic(5, 10, 0, 4), // 2024-01-05 fry
                mock_historic(5, 10, 0, 5), // 2024-01-06 sat
                mock_historic(5, 10, 0, 6), // 2024-01-07 sun
                mock_historic(5, 10, 1, 1), // 2024-01-08 mon
                mock_historic(5, 10, 1, 2), // 2024-01-09 tur
                mock_historic(5, 10, 1, 3), // 2024-01-10 wed
            ],
        );

        let days = simulation.run_once();
        let last_day = days.last().unwrap();
        let total_qty = last_day
            .batches
            .iter()
            .map(|batch| batch.quantity.clone())
            .reduce(|acc, batch_qty| acc + batch_qty)
            .unwrap();
        assert_eq!(last_day.batches.len(), 6);
        assert_eq!(total_qty, BigDecimal::from(30));
    }

    fn mock_product_batches() -> Vec<ProductBatch> {
        vec![ProductBatch {
            quantity: BigDecimal::from(100),
            entry_date: DateTime::parse_from_rfc3339("2023-12-31T00:00:00Z")
                .unwrap()
                .to_utc(),
            deadline_date: DateTime::parse_from_rfc3339("2024-01-11T00:00:00Z")
                .unwrap()
                .to_utc(),
            finished_date: None,
            is_finished: false,
        }]
    }

    fn mock_historic(
        entry_qty: i32,
        withdrawal_qty: i32,
        week_of_year: i16,
        day_of_week: i16,
    ) -> ProductMovHist {
        ProductMovHist {
            product_id: Uuid::from_u128(0),
            entry_qty: BigDecimal::from(entry_qty),
            withdrawal_qty: BigDecimal::from(withdrawal_qty),
            week_of_year,
            day_of_week,
        }
    }
}
