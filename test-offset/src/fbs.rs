use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;

use bigdecimal::ToPrimitive;
use flatbuffers::FlatBufferBuilder;

use crate::pg_repository;
use crate::pos_fbs;

pub fn raw_to_fbs(pos_fbs: &Vec<Vec<u8>>) -> (Duration, Vec<pos_fbs::pos::Pos>) {
    let timer = Instant::now();
    let mapped = pos_fbs
        .iter()
        .map(|e| unsafe { crate::pos_fbs::pos::root_as_pos_unchecked(&e) })
        //     .filter(|e| e.is_ok() )
        //     .map(|e| e.unwrap() )
        .collect();
    (timer.elapsed(), mapped)
}

pub fn do_pos_fbs_aggregation<'a>(
    pos_fbs: &'a Vec<pos_fbs::pos::Pos<'a>>,
) -> (Duration, HashMap<u32, HashMap<u16, PosFbsAggr<'a>>>) {
    let timer = Instant::now();
    let mut map = HashMap::new();

    pos_fbs.iter().for_each(|fbs| {
        let acc = map.entry(fbs.dst()).or_insert_with(HashMap::new);
        let acc_grp = acc.entry(fbs.grp()).or_insert_with(|| PosFbsAggr::new());
        acc_grp.pos_list.push(*fbs);
    });
    (timer.elapsed(), map)
}

pub struct PosFbsAggr<'a> {
    pos_list: Vec<pos_fbs::pos::Pos<'a>>,
}

impl PosFbsAggr<'static> {
    fn new() -> PosFbsAggr<'static> {
        PosFbsAggr {
            pos_list: Vec::new(),
        }
    }

    fn get_sum(&self) -> f32 {
        self.pos_list
            .iter()
            .map(|e| e.ratio().clone())
            .reduce(|acc, ratio| acc + ratio)
            .unwrap()
    }
}

pub fn convert_all_to_buf(query_res: &Vec<pg_repository::Pos>) -> (Duration, Vec<Vec<u8>>) {
    let timer = Instant::now();
    (
        timer.elapsed(),
        query_res.iter().map(|e| convert_to_fbs(e)).collect(),
    )
}

fn convert_to_fbs(pos: &pg_repository::Pos) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let pos_args = pos_fbs::pos::PosArgs {
        dst: pos.dst.unsigned_abs(),
        acc_id: pos.acc_id.unsigned_abs(),
        ins_id: pos.ins_id.unsigned_abs(),
        grp: pos.grp.unsigned_abs(),
        grpv: *pos.grpv.as_bytes().get(0).unwrap(),
        qty: pos.qty as f32,
        factor: pos.factor.to_f32().unwrap(),
        ratio: pos.ratio.to_f32().unwrap(),
    };
    let pos_fbs = pos_fbs::pos::Pos::create(&mut builder, &pos_args);
    pos_fbs::pos::finish_pos_buffer(&mut builder, pos_fbs);
    // read_t5_fbs( builder.finished_data() )
    Vec::from(builder.finished_data())
}
