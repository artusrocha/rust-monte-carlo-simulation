use crate::{
    data::product_batch::ProductBatch, simulation::control::parameter::SimulationParameters,
};
use sqlx::types::BigDecimal;

use chrono::{DateTime, Days, Utc};

#[derive(Debug, Clone)]
pub struct SimulationDay {
    pub date: DateTime<Utc>,
    pub batches: Vec<ProductBatch>,
    pub stock_shortage: Option<BigDecimal>,
    pub stock_limit_exceeded: Option<BigDecimal>,
    pub stock_time_limit_exceeded: Option<BigDecimal>,
    pub is_calculated: bool,
}

impl SimulationDay {
    fn do_withdraw_mov(&mut self, sim_param: &SimulationParameters) {
        let date_hist = sim_param.get_date_hist(&self.date);
        eprintln!("date_hist: {:?}", date_hist);
        let mut withdraw_qty = date_hist.withdrawal_qty.clone();
        eprintln!(
            "before withdraw | withdraw_qty: {:?}, batches.len(): {:?}, batches_qty: {:?}",
            withdraw_qty,
            self.batches.len(),
            self.batches
                .iter()
                .map(|e| e.quantity.clone())
                .reduce(|acc, e| acc + e)
        );
        while withdraw_qty > BigDecimal::from(0) && self.batches.len() > 0 {
            match self.batches.first_mut() {
                Some(old) => {
                    if old.quantity > withdraw_qty {
                        old.quantity = &old.quantity - withdraw_qty;
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
            "after withdraw | withdraw_qty: {:?}, batches.len(): {:?}, batches_qty: {:?}",
            withdraw_qty,
            self.batches.len(),
            self.batches
                .iter()
                .map(|e| e.quantity.clone())
                .reduce(|acc, e| acc + e)
        );
        self.stock_shortage = if withdraw_qty > BigDecimal::from(0) {
            Some(withdraw_qty)
        } else {
            None
        };
    }

    fn do_entry_mov(&mut self, sim_param: &SimulationParameters) {
        let date_hist = sim_param.get_date_hist(&self.date);
        eprintln!("date_hist: {:?}", date_hist);
        let batches_qty_sum = self
            .batches
            .iter()
            .map(|e| e.quantity.clone())
            .reduce(|acc, e| acc + e)
            .unwrap_or(BigDecimal::from(0));
        eprintln!(
            "before entry | entry_qty: {:?}, batches.len(): {:?}, batches_qty_sum: {:?}",
            date_hist.entry_qty,
            self.batches.len(),
            batches_qty_sum
        );
        let available = BigDecimal::from(sim_param.stock_maximum_quantity) - batches_qty_sum;
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
                .checked_add_days(Days::new(sim_param.new_batch_default_expiration_days))
                .unwrap(),
            entry_date: self.date.clone(),
            finished_date: None,
            is_finished: false,
        });
        eprintln!(
            "after entry | entry_qty: {:?}, batches.len(): {:?}, batches_qty_sum: {:?}",
            date_hist.entry_qty,
            self.batches.len(),
            self.batches
                .iter()
                .map(|e| e.quantity.clone())
                .reduce(|acc, e| acc + e)
        );

        self.stock_limit_exceeded = if exceeded_entry_qty > BigDecimal::from(0) {
            Some(exceeded_entry_qty)
        } else {
            None
        };
    }

    fn do_rm_expired_batch_mov(&mut self) {
        let mut removed_quantity = BigDecimal::from(0);
        let mut to_remove_idx = Vec::<usize>::new();
        for (i, e) in self.batches.iter().enumerate().rev() {
            let is_to_remove = e.deadline_date < self.date;
            eprintln!(
                "Element at position {}: {:?}, is_to_remove: {}",
                i, e, is_to_remove
            );
            if is_to_remove {
                removed_quantity += e.quantity.clone();
                to_remove_idx.push(i);
            }
        }
        for i in to_remove_idx {
            eprintln!("removing element at position {}", i);
            self.batches.remove(i);
        }

        eprintln!(
            "after rm_expired | batches.len(): {:?}, batches_qty_sum: {:?}",
            self.batches.len(),
            self.batches
                .iter()
                .map(|e| e.quantity.clone())
                .reduce(|acc, e| acc + e)
        );
        self.stock_time_limit_exceeded = if removed_quantity > BigDecimal::from(0) {
            Some(removed_quantity)
        } else {
            None
        };
    }

    pub fn calculate(&mut self, sim_param: &SimulationParameters) -> bool {
        self.do_withdraw_mov(sim_param);
        self.do_entry_mov(sim_param);
        self.do_rm_expired_batch_mov();
        self.is_calculated = true;
        self.is_calculated
    }

    pub fn create_next(&self) -> Option<SimulationDay> {
        match self.date.checked_add_days(Days::new(1)) {
            Some(new_date) => Some(SimulationDay {
                date: new_date,
                batches: self.batches.clone(),
                stock_shortage: None,
                stock_limit_exceeded: None,
                stock_time_limit_exceeded: None,
                is_calculated: false,
            }),
            None => None,
        }
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
