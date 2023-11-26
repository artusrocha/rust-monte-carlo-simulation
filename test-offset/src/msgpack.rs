use std::time::{Instant, Duration};

use crate::pg_repository::Pos;

pub fn convert_all_from_mp(pos_mps: &[Vec<u8>]) -> (Duration, Vec<Pos>) {
    let timer = Instant::now();
    let mapped = pos_mps
        .iter()
        .map(|buf| rmp_serde::from_slice::<Pos>(&buf).unwrap())
        .collect();
    (timer.elapsed(), mapped)
}

pub fn convert_all_to_mp(query_res: &[Pos]) -> (Duration, Vec<Vec<u8>>) {
    let timer = Instant::now();
    let buf = query_res
        .iter()
        .map(|pos| rmp_serde::to_vec(&pos).unwrap())
        .collect();
    (timer.elapsed(), buf)
}
