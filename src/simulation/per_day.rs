use crate::{data::product_batch::ProductBatch, simulation::parameter::SimulationParameters};
use sqlx::types::BigDecimal;

use chrono::{DateTime, Days, Utc};

#[derive(Debug)]
pub struct SimulationDay {
    pub date: DateTime<Utc>,
    pub batches: Vec<ProductBatch>,
    pub stock_shortage: Option<BigDecimal>,
    pub stock_limit_exceeded: Option<BigDecimal>,
    pub stock_time_limit_exceeded: Option<BigDecimal>,
}

impl SimulationDay {
    fn do_withdraw_mov(&mut self, sim_param: &SimulationParameters) {
        let date_hist = sim_param.get_date_hist(&self.date);
        let mut withdraw_qty = date_hist.withdrawal_qty.clone();
        eprintln!(
            "before withdraw | withdraw_qty: {:?}, batches: {:?}",
            withdraw_qty,
            self.batches
                .iter()
                .map(|e| e.quantity.clone())
                .reduce(|acc, e| acc + e)
        );
        while withdraw_qty > BigDecimal::from(0) && self.batches.len() > 0 {
            match self.batches.first_mut() {
                Some(old) => {
                    if old.quantity > withdraw_qty {
                        old.quantity = &old.quantity - withdraw_qty.fractional_digit_count();
                        withdraw_qty = BigDecimal::from(0);
                    } else {
                        withdraw_qty = withdraw_qty - &old.quantity;
                        self.batches.remove(0);
                    }
                }
                None => {}
            };
        }
        eprintln!(
            "after withdraw | withdraw_qty: {:?}, batches: {:?}",
            withdraw_qty,
            self.batches
                .iter()
                .map(|e| e.quantity.clone())
                .reduce(|acc, e| acc + e)
        );
        eprintln!("2: next_batches.len(): {:?}", self.batches.len());
        self.stock_shortage = if withdraw_qty > BigDecimal::from(0) {
            Some(withdraw_qty)
        } else {
            None
        };
    }

    fn do_entry_mov(&mut self, sim_param: &SimulationParameters) {
        let date_hist = sim_param.get_date_hist(&self.date);
        let batches_qty_sum = self
            .batches
            .iter()
            .map(|e| e.quantity.clone())
            .reduce(|acc, e| acc + e)
            .unwrap_or(BigDecimal::from(0));
        eprintln!(
            "before entry | entry_qty: {:?}, batches: {:?}",
            date_hist.entry_qty, batches_qty_sum
        );
        let available = BigDecimal::from(sim_param.stock_limit) - batches_qty_sum;
        let (final_entry_qty, exceeded_entry_qty) = if available > date_hist.entry_qty {
            (date_hist.entry_qty.clone(), BigDecimal::from(0))
        } else {
            (available.clone(), (date_hist.entry_qty.clone() - available))
        };
        self.batches.push(ProductBatch {
            quantity: final_entry_qty,
            deadline_date: self
                .date
                .clone()
                .checked_add_days(Days::new(sim_param.time_limit))
                .unwrap(),
            entry_date: self.date.clone(),
            finished_date: None,
            is_finished: false,
        });
        eprintln!(
            "after entry | entry_qty: {:?}, batches: {:?}",
            date_hist.entry_qty,
            self.batches
                .iter()
                .map(|e| e.quantity.clone())
                .reduce(|acc, e| acc + e)
        );
        eprintln!("3: next_batches.len(): {:?}", self.batches.len());
        eprintln!(
            "front: {:?}, back: {:?}",
            self.batches.first(),
            self.batches.last()
        );
        self.stock_limit_exceeded = if exceeded_entry_qty > BigDecimal::from(0) {
            Some(exceeded_entry_qty)
        } else {
            None
        };
    }

    fn do_rm_expired_batch_mov(&mut self) {
        let mut removed_quantity = BigDecimal::from(0);

        for i in 0..self.batches.len() {
            let e = &self.batches[i];
            eprintln!("Element at position {}: {:?}", i, e);
            if e.deadline_date > self.date {
                removed_quantity += e.quantity.clone();
                self.batches.remove(i);
            }
        }
        self.stock_time_limit_exceeded = if removed_quantity > BigDecimal::from(0) {
            Some(removed_quantity)
        } else {
            None
        };
    }

    pub fn calculate_next(&self, sim_param: &SimulationParameters) -> SimulationDay {
        let mut next_day = SimulationDay {
            date: self.date.clone(), //TODO next date
            batches: self.batches.clone(),
            stock_shortage: None,
            stock_limit_exceeded: None,
            stock_time_limit_exceeded: None,
        };
        next_day.do_withdraw_mov(sim_param);
        next_day.do_entry_mov(sim_param);
        next_day.do_rm_expired_batch_mov();
        next_day
    }
}

#[cfg(test)]
mod tests {

    //use super::*;

    #[test]
    fn test() {
        //todo!()
        let mut sum = 0;
        for i in 1..4 {
            sum = sum + i;
            println!("{}", i);
        }
        assert_eq!(sum, 6);
    }
}
