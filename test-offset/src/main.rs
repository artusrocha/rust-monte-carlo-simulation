#![allow(dead_code, unused_imports, non_camel_case_types)]


//extern crate flatbuffers;
mod position_generated;

use rand::Rng;

use serde::{Serialize, Deserialize};

use rustc_hash::FxHashMap;

use flatbuffers::FlatBufferBuilder;

use position_generated::pos::finish_size_prefixed_t_5_buffer;
use position_generated::pos::finish_t_5_buffer;
use position_generated::pos::root_as_t_5;
use position_generated::pos::root_as_t_5_unchecked;
use position_generated::pos::size_prefixed_root_as_t_5;
use position_generated::pos::{t5, t5Args, t5Builder, t5Offset};

use sqlx::{
    database::HasArguments, postgres::PgPoolOptions, query::QueryAs, types::BigDecimal, FromRow,
    Pool, Postgres,
};

use bigdecimal::ToPrimitive;

use std::ops::Add;
use std::{
    collections::{HashMap, LinkedList},
    env,
    time::Instant,
};

// #[test]
//#[tokio::main]
//async fn main() -> Result<(), Box<dyn std::error::Error>> {

async fn _main () -> Result<(), Box<dyn std::error::Error>> {

    let mut rng = rand::thread_rng();

    let pos = Pos {
        dst: rng.gen(),
        acc_id: rng.gen(),
        ins_id: rng.gen(),
        grp: rng.gen(),
        grpv: "L".to_owned(),
        qty: rng.gen(),
        factor: BigDecimal::from(rng.gen::<u32>()),
        ratio: BigDecimal::from(rng.gen::<u32>()),
    };

    let pos_buf = rmp_serde::to_vec(&pos).unwrap();
    let pos_tup_buf = rmp_serde::to_vec(&(pos.dst,pos.acc_id,pos.ins_id,pos.grp,pos.grpv,pos.qty,pos.factor,pos.ratio)).unwrap();
    eprintln!("{:?}", pos_buf.len());
    eprintln!("{:?}", pos_tup_buf.len());

    eprintln!("{:?}", rmp_serde::from_slice::<Pos>(&pos_buf).unwrap());
    eprintln!("{:?}", rmp_serde::from_slice::<Pos>(&pos_tup_buf).unwrap());

    Ok(())
    
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let par_id = 1;

    let db = get_db_conn_pool().await?;

    let mut redis_conn = get_redis_conn()?;    
 
    let query = sqlx::query_as::<_, Pos>("
        select dst, acc_id,ins_id, grp, grpv, qty, factor, ratio from acc join t5 on acc_id=id where par_id=$1;
    ");

    let query_aggr = sqlx::query_as::<_, PosAggr>("
        select * from (select dst, grp, sum(ratio) as sum from acc join t5 on t5.acc_id=acc.id where acc.par_id=$1 group by dst, grp); -- where sum>0;
    ");

    let start = Instant::now();
    let query_aggr_res = query_aggr.bind(par_id).fetch_all(&db).await?;
    eprintln!("query_aggr | query_aggr_res.len(): {:?} | Time elapsed {:?}", query_aggr_res.len(), start.elapsed());

    eprintln!("\n============================\n");
 
    let start = Instant::now();
    let query_res = query.bind(par_id).fetch_all(&db).await?;
    eprintln!("query | query_res.len: {:?} | Time elapsed {:?}", query_res.len(), start.elapsed());

    let start = Instant::now();
    let pos_aggr = do_post_aggregation(&query_res);
    eprintln!("pos_aggr | query_res.len(): {:?} | pos_aggr.len(): {:?} | Time elapsed {:?}", &query_res.len(), pos_aggr.len(), start.elapsed());

    eprintln!("\n============================\n");

    let start = Instant::now();
    let pos_fbs = convert_all_to_fbs(&query_res);
    eprintln!("fbs convert | query_res.len(): {:?} | pos_fbs.len(): {:?} | Time elapsed {:?}", &query_res.len(), pos_fbs.len(), start.elapsed());

    let start = Instant::now();
    let pos_fbs_aggr = do_pos_fbs_aggregation(&pos_fbs);
    eprintln!("fbs aggregation | query_res.len(): {:?} | pos_fbs_aggr.len(): {:?} | Time elapsed {:?}", &query_res.len(), pos_fbs_aggr.len(), start.elapsed());

    let start = Instant::now();
    let redis_prepare_sadd_cmd = prepare_redis_sadd_cmd(&pos_fbs, "fbs:par:1");
    eprintln!("fbs redis preparing | Time elapsed {:?}", start.elapsed());

    let start = Instant::now();
    redis_prepare_sadd_cmd.execute(&mut redis_conn);
    eprintln!("fbs redis saving | Time elapsed {:?}", start.elapsed());

    let start = Instant::now();
    let mut redis_smembers_cmd = redis::cmd("SMEMBERS");
    redis_smembers_cmd.arg("fbs:par:1");
    let fbs_from_redis = redis_smembers_cmd.query::<Vec<Vec<u8>>>(&mut redis_conn)?;
    eprintln!("fbs redis reading | fbs_from_redis.len(): {:?} | Time elapsed {:?}", fbs_from_redis.len(), start.elapsed());

    let start = Instant::now();
    let redis_fbs_aggr = do_pos_fbs_aggregation(&fbs_from_redis);
    eprintln!("fbs redis aggregation | fbs_from_redis.len(): {:?} | redis_fbs_aggr.len(): {:?} | Time elapsed {:?}", fbs_from_redis.len(), redis_fbs_aggr.len(), start.elapsed());

    let start = Instant::now();
    let fbs_from_redis = redis::cmd("GET").arg("huge").query::<Vec<Vec<u8>>>(&mut redis_conn)?;
    eprintln!("huge fbs redis reading | fbs_from_redis.len(): {:?} | Time elapsed {:?}", fbs_from_redis.len(), start.elapsed());

    eprintln!("\n============================\n");

    let start = Instant::now();
    let pos_mps = convert_all_to_mp(&query_res);
    eprintln!("mps convert | query_res.len(): {:?} | pos_mps.len(): {:?} | Time elapsed {:?}", &query_res.len(), pos_mps.len(), start.elapsed());

    let start = Instant::now();
    let pos_mps_pos1 = convert_all_from_mp(&pos_mps);
    eprintln!("mps convert | query_res.len(): {:?} | pos_mps_pos2.len(): {:?} | Time elapsed {:?}", &query_res.len(), pos_mps_pos1.len(), start.elapsed());

    let start = Instant::now();
    let pos_mps_pos1_aggr = do_post_aggregation(&pos_mps_pos1);
    eprintln!("pos_mps_pos1_aggr | query_res.len(): {:?} | pos_mps_pos1_aggr.len(): {:?} | Time elapsed {:?}", &query_res.len(), pos_mps_pos1_aggr.len(), start.elapsed());

    let start = Instant::now();
    let redis_prepare_sadd_cmd = prepare_redis_sadd_cmd(&pos_mps, "mps:par:1");
    eprintln!("mps redis preparing | Time elapsed {:?}", start.elapsed());

    let start = Instant::now();
    redis_prepare_sadd_cmd.execute(&mut redis_conn);
    eprintln!("mps redis saving | Time elapsed {:?}", start.elapsed());

    let start = Instant::now();
    let mut redis_smembers_cmd = redis::cmd("SMEMBERS");
    redis_smembers_cmd.arg("mps:par:1");
    let mps_from_redis = redis_smembers_cmd.query::<Vec<Vec<u8>>>(&mut redis_conn)?;
    eprintln!("mps redis reading | mps_from_redis.len(): {:?} | Time elapsed {:?}", mps_from_redis.len(), start.elapsed());

    let start = Instant::now();
    let pos_mps_pos2 = convert_all_from_mp(&mps_from_redis);
    eprintln!("mps convert 2 | pos_mps_pos2.len(): {:?} | Time elapsed {:?}", pos_mps_pos2.len(), start.elapsed());

    let start = Instant::now();
    let redis_mps_aggr = do_post_aggregation(&pos_mps_pos2);
    eprintln!("mps redis aggregation | pos_mps_pos2.len(): {:?} | redis_mps_aggr.len(): {:?} | Time elapsed {:?}", pos_mps_pos2.len(), redis_mps_aggr.len(), start.elapsed());

    eprintln!("\n============================\n");

    let start = Instant::now();
    let redis_mps_aggr_mp = rmp_serde::to_vec(&redis_mps_aggr).unwrap();
    redis::cmd("SET").arg("huge").arg(&redis_mps_aggr_mp).execute(&mut redis_conn);
    eprintln!("huge - mps redis saving | mps_from_redis.len(): {:?} | Time elapsed {:?}", mps_from_redis.len(), start.elapsed());

    let start = Instant::now();
    let mut redis_smembers_cmd = redis::cmd("GET");
    redis_smembers_cmd.arg("huge");
    let mps_from_redis = redis_smembers_cmd.query::<Vec<u8>>(&mut redis_conn)?;
    eprintln!("huge - mps redis reading | mps_from_redis.len(): {:?} | Time elapsed {:?}", mps_from_redis.len(), start.elapsed());

    let start = Instant::now();
    let huge = rmp_serde::from_slice::<HashMap<i32, HashMap<i16, PosPostAggr>>>(&mps_from_redis).unwrap();
    eprintln!("huge - mps convert | huge.len(): {:?} | Time elapsed {:?}", huge.len(), start.elapsed());

    let start = Instant::now();
    let mut sum_part =0;
    let mut sum_full =0;
    huge.iter()
        .for_each(|(_k,v)| {
            sum_part += v.len();
            v.iter().for_each(|(_k,v)| sum_full += v.pos_list.len());
        });
    eprintln!("huge - count | sum_part: {:?} | sum_full {:?} | Time elapsed {:?}", sum_part, sum_full, start.elapsed());

    // let start = Instant::now();
    // let redis_mps_aggr = do_post_aggregation(&pos_mps_pos2);
    // eprintln!("mps redis aggregation | pos_mps_pos2.len(): {:?} | redis_mps_aggr.len(): {:?} | Time elapsed {:?}", pos_mps_pos2.len(), redis_mps_aggr.len(), start.elapsed());

    // BAD PERFORMANCE -> ~ 40s   !!!!!!!!!!!!
    // let start = Instant::now();
    // let mut redis_fbs_iter : redis::Iter<Vec<u8>> = redis::cmd("SSCAN")
    //     .arg("par:1")
    //     .cursor_arg(1_000_000)
    //     .clone()
    //     .iter(&mut redis_conn)?;
    // let mut redis_fbs_cursor_aggr = HashMap::new();
    // for buf in redis_fbs_iter {
    //     let t5 = root_as_t_5(&buf);
    //     match t5 {
    //         Ok(fbs) => {
    //             let acc = redis_fbs_cursor_aggr.entry(fbs.dst()).or_insert_with(HashMap::new);
    //             let acc_grp = acc.entry(fbs.grp()).or_insert_with(|| Vec::new());
    //             acc_grp.push(buf);
    //         },
    //         _ => {},
    //     }
    // }
    //let redis_fbs_aggr = do_pos_fbs_aggregation(fbs_from_redis.as_slice());
    // eprintln!("fbs redis cursor aggregation | fbs_from_redis.len(): {:?} | redis_fbs_cursor_aggr.len(): {:?} | Time elapsed {:?}", fbs_from_redis.len(), redis_fbs_cursor_aggr.len(), start.elapsed());

    Ok(())
}

fn convert_all_from_mp(pos_mps: &[Vec<u8>]) -> Vec<Pos> {
    pos_mps.iter()
        .map(|buf| rmp_serde::from_slice::<Pos>(&buf).unwrap())
//        .filter(|r| r.is_ok())
//        .map(|r| r.unwrap())
        .collect()
}

fn convert_all_to_mp(query_res: &[Pos]) -> Vec<Vec<u8>> {
    query_res.iter()
        .map(|pos| rmp_serde::to_vec(&pos).unwrap() )
        .collect()   
}

fn prepare_redis_sadd_cmd(buf_items:&Vec<Vec<u8>>, key: &str) -> redis::Cmd {
    let mut redis_prepare_sadd_cmd = redis::cmd("SADD");
    redis_prepare_sadd_cmd.arg(key);
    for e in buf_items {
//        let fbs = convert_to_fbs(e);
        redis_prepare_sadd_cmd.arg(e);
    }
    redis_prepare_sadd_cmd
}

fn do_pos_fbs_aggregation(pos_fbs: &Vec<Vec<u8>>)  -> HashMap<u32, HashMap<u16, PosFbsAggr>> {
    let mut map = HashMap::new();

    // let mut test = FxHashMap::default();
    // pos_fbs.iter().for_each( |e| {
    //     //let fbs = root_as_t_5(&e);
    //     //let fbs = unsafe { root_as_t_5_unchecked(&e) };
    //     match root_as_t_5(&e) { //read_t5_fbs(e.as_slice()) { 
    //         Ok(fbs) => {
    //             let acc = test.entry(fbs.dst()).or_insert_with(FxHashMap::default);
    //             // acc.push(fbs);
    //            let acc_grp = acc.entry(fbs.grp()).or_insert_with(|| Vec::new());
    //            acc_grp.push(fbs);
    //         },
    //         _ => {}
    //     }
    // });
//     map

    pos_fbs.iter()
     .map(|e| unsafe { root_as_t_5_unchecked(&e) } )
//     .filter(|e| e.is_ok() )
//     .map(|e| e.unwrap() )
     .map(|e| (e.dst(), e.grp(), e) )
     .for_each(|(dst, grp, fbs)| {
        let acc = map.entry(dst).or_insert_with(HashMap::new);
        let acc_grp = acc.entry(grp).or_insert_with(|| PosFbsAggr::new());
        acc_grp.pos_list.push(fbs);
     });
    map
}

struct PosFbsAggr<'a> {
    pos_list: Vec<t5<'a>>,
}
impl PosFbsAggr<'static> {
    fn new() -> PosFbsAggr<'static> {
        PosFbsAggr { pos_list: Vec::new() }
    }

    fn get_sum(&self) -> f32 {
        self.pos_list.iter()
            .map(|e| e.ratio().clone())
            .reduce(|acc, ratio| acc + ratio )
            .unwrap()
    }
}

fn convert_all_to_fbs(query_res: &Vec<Pos>) -> Vec<Vec<u8>> {
    query_res.iter()
        .map(|e| convert_to_fbs(e))
        .collect()   
}

fn convert_to_fbs(pos: &Pos) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let t5_args = t5Args {
        dst: pos.dst.unsigned_abs() ,
        acc_id: pos.acc_id.unsigned_abs(),
        ins_id: pos.ins_id.unsigned_abs(),
        grp: pos.grp.unsigned_abs(),
        grpv: *pos.grpv.as_bytes().get(0).unwrap(),
        qty: pos.qty as f32,
        factor: pos.factor.to_f32().unwrap(),
        ratio: pos.ratio.to_f32().unwrap(),
    };
    let t5 = t5::create(&mut builder, &t5_args);
    finish_t_5_buffer(&mut builder, t5);
    // read_t5_fbs( builder.finished_data() )
    Vec::from( builder.finished_data() )
}

fn read_t5_fbs(buf: &[u8]) -> Option<t5> {
    match root_as_t_5(buf) {
        Ok(r) => Some(r),
        Err(_) => {
            eprintln!("Error on fbs read");
            None
        },
    }
}

fn do_post_aggregation(query_res: &Vec<Pos>) -> HashMap<i32, HashMap<i16, PosPostAggr>> {
    let mut map = HashMap::new();
    for e in query_res {
        let acc = map.entry(e.dst).or_insert_with(HashMap::new);
        let acc_grp = acc.entry(e.grp).or_insert_with(|| PosPostAggr::new());
        acc_grp.pos_list.push(e.clone());
    }
    map
}

#[derive(Serialize, Deserialize, Debug)]
struct PosPostAggr {
    pos_list: Vec<Pos>
}
impl PosPostAggr {
    fn new() -> PosPostAggr {
        PosPostAggr { pos_list: Vec::new() }
    }

    fn get_sum(&self) -> BigDecimal {
        self.pos_list.iter()
            .map(|e| e.ratio.clone())
            .reduce(|acc, ratio| acc + ratio )
            // .to_owned()
            .unwrap()
            // .clone()
    }
}

#[derive(Debug, FromRow, Clone, Serialize, Deserialize)]
struct Pos {
    dst: i32,
    acc_id: i32,
    ins_id: i32,
    grp: i16,
    grpv: String,
    qty: i32,
    factor: BigDecimal,
    ratio: BigDecimal,
}

#[derive(Debug, FromRow, Clone)]
struct PosAggr {
    dst: i32,
    grp: i16,
    // grpv: String,
    sum: BigDecimal,
}

async fn get_db_conn_pool() -> Result<Pool<Postgres>, Box<dyn std::error::Error>> {
    let database_url = env::var("DATABASE_URL")?;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;
    Ok(pool)
}

fn get_redis_conn() -> Result<redis::Connection, Box<dyn std::error::Error>> { //redis::RedisResult<()> {
    let url = env::var("REDIS_URL")?;
    let client = redis::Client::open(url)?;
    let con = client.get_connection()?;
    Ok(con)
}