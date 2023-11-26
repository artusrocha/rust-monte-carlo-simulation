mod avro;
mod fbs;
mod mongo_repository;
mod msgpack;
mod pg_repository;
mod pos_avro;
mod pos_fbs;
mod redis_repository;

use pg_repository::Pos;

use serde::{Deserialize, Serialize};

use sqlx::types::BigDecimal;

use std::{collections::HashMap, env, time::{Instant, Duration}};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    let par_id = if args.len() > 1 && !args[1].is_empty() {
        args[1].parse().unwrap()
    } else {
        1
    };
    eprintln!("par_id {}", par_id);

    let pg_repo = pg_repository::Repo::init().await?;

    let mut redis_repo = redis_repository::Repo::init()?;

    let mongo_repo = mongo_repository::Repo::new().await?;

    let avro_parser = avro::AvroParser::new();

    println!("type\tt_query\tt_convert\tt_aggr\tt_total");

    let (t_query_aggr_res, query_aggr_res) = pg_repo.query_aggr(par_id).await?;
    eprintln!("query_aggr_res.len(): {:?}", query_aggr_res.len());
    println!(
        "{}\t{}\t{}\t{}",
        "query_aggr",
        t_query_aggr_res.as_millis(),
        "",
        ""
    );

    let (t_query, query_res) = pg_repo.query_full(par_id).await?;

    let (t_pos_aggr, pos_aggr) = do_post_aggregation(&query_res);
    eprintln!("pos_aggr.len(): {:?}", pos_aggr.len());
    println!(
        "{}\t{}\t{}\t{}",
        "query_full",
        t_query.as_millis(),
        "",
        t_pos_aggr.as_millis()
    );

    let t_mongo_save = mongo_repo.save_all(&query_res).await?;
    let (t_mongo_read, mongo_items) = mongo_repo.find_all().await?;
    eprintln!(
        "t_mongo_save: {:?}, t_mongo_read: {:?}, len(): {:?}",
        t_mongo_save,
        t_mongo_read,
        mongo_items.len()
    );

    let (t_convert_rds_fbs, pos_fbs_buf) = fbs::convert_all_to_buf(&query_res);

    let (t_pos_fbs_mapped, pos_fbs_mapped) = fbs::raw_to_fbs(&pos_fbs_buf);
    eprintln!(
        "pos_fbs_mapped.len(): {:?} | time: {:?}",
        pos_fbs_mapped.len(),
        t_pos_fbs_mapped
    );

    let (t_pos_fbs_aggr, pos_fbs_aggr) = fbs::do_pos_fbs_aggregation(&pos_fbs_mapped);
    eprintln!(
        "pos_fbs_aggr.len(): {:?} | time: {:?}",
        pos_fbs_aggr.len(),
        t_pos_fbs_aggr
    );

    let t_save_rds_fbs = redis_repo.save(&pos_fbs_buf, &format!("h:fbs:par:{}", par_id))?;
    println!(
        "{}\t{}\t{}\t{}",
        "save_rds_fbs",
        t_save_rds_fbs.as_millis(),
        t_convert_rds_fbs.as_millis(),
        ""
    );

    let (t_rds_fbs_read, fbs_from_redis) = redis_repo.find(&format!("h:fbs:par:{}", par_id))?;
    
    let (t_rds_fbs_convert, pos_fbs_mapped_from_redis) = fbs::raw_to_fbs(&fbs_from_redis);
    eprintln!(
        "pos_fbs_mapped_from_redis.len(): {:?} | time: {:?}",
        pos_fbs_mapped_from_redis.len(),
        t_rds_fbs_convert
    );

    let (t_rds_fbs_aggr, pos_fbs_aggr_from_redis) =
        fbs::do_pos_fbs_aggregation(&pos_fbs_mapped_from_redis);
    eprintln!(
        "pos_fbs_aggr_from_redis.len(): {:?} | time: {:?}",
        pos_fbs_aggr_from_redis.len(),
        t_rds_fbs_aggr
    );

    println!(
        "{}\t{}\t{}\t{}",
        "read_rds_fbs",
        t_rds_fbs_read.as_millis(),
        t_rds_fbs_convert.as_millis(),
        t_rds_fbs_aggr.as_millis()
    );

    let (t_pos_mps_buf, pos_mps_buf) = msgpack::convert_all_to_mp(&query_res);
    eprintln!(
        "pos_mps_buf.len(): {:?} | time: {:?}",
        pos_mps_buf.len(),
        t_pos_mps_buf
    );

    let (t_mp_convert, pos_mps_pos1) = msgpack::convert_all_from_mp(&pos_mps_buf);

    let (t_pos_mps_pos1_aggr, pos_mps_pos1_aggr) = do_post_aggregation(&pos_mps_pos1);
    eprintln!(
        "pos_mps_pos1_aggr.len(): {:?} | time: {:?}",
        pos_mps_pos1_aggr.len(),
        t_pos_mps_pos1_aggr
    );

    let rds_mps_hash_key = format!("h:mps:par:{}", par_id);
    
    let t_rds_mp_save = redis_repo.save(&pos_mps_buf, &rds_mps_hash_key)?;
    
    println!(
        "{}\t{}\t{}\t{}",
        "read_rds_mp",
        t_rds_mp_save.as_millis(),
        t_mp_convert.as_millis(),
        ""
    );

    let (t_rds_mp_read, mps_from_redis) = redis_repo.find(&rds_mps_hash_key)?;

    let (t_rds_mp_convert, pos_mps_pos2) = msgpack::convert_all_from_mp(&mps_from_redis);

    eprintln!(
        "pos_mps_pos2.len(): {:?} | time: {:?}",
        pos_mps_pos2.len(),
        t_rds_mp_convert
    );

    let (t_rds_mp_aggr, redis_mps_aggr) = do_post_aggregation(&pos_mps_pos2);
    eprintln!(
        "pos_mps_pos2.len(): {:?} | redis_mps_aggr.len(): {:?} | time: {:?}",
        pos_mps_pos2.len(),
        redis_mps_aggr.len(),
        t_rds_mp_aggr
    );

    println!(
        "{}\t{}\t{}\t{}",
        "read_rds_mp",
        t_rds_mp_read.as_millis(),
        t_rds_mp_convert.as_millis(),
        t_rds_mp_aggr.as_millis()
    );

    let timer = Instant::now();
    let avro_bufs: Vec<Vec<u8>> = query_res
        .iter()
        .map(|pos| avro_parser.to_avro_buf(pos))
        .collect();
    let t_avro_convert_to = timer.elapsed();

    let t_avro_rds_save = redis_repo.save(&avro_bufs, &format!("h:avr:par:{}", par_id))?;

    println!(
        "{}\t{}\t{}\t{}",
        "save_rds_avro",
        t_avro_rds_save.as_millis(),
        t_avro_convert_to.as_millis(),
        ""
    );

    let (t_avro_rds_read, avro_from_redis) = redis_repo.find(&format!("h:avr:par:{}", par_id))?;

    let timer = Instant::now();
    let pos_avro_read: Vec<pos_avro::PosAvro> = avro_from_redis
        .iter()
        .map(|buf| avro_parser.from_avro_buf(&buf))
        .collect();
    let t_avro_convert_from = timer.elapsed();
    eprintln!("avro 1: {:?}", pos_avro_read.get(1));
    eprintln!(
        "avro convert from | pos_avro_read.len(): {:?} | time: {:?}",
        pos_avro_read.len(),
        t_avro_convert_from
    );
    println!(
        "{}\t{}\t{}\t{}",
        "read_rds_avro",
        t_avro_rds_read.as_millis(),
        t_avro_convert_from.as_millis(),
        ""
    );

    Ok(())
}

fn do_post_aggregation(query_res: &Vec<Pos>) -> (Duration, HashMap<i32, HashMap<i16, PosPostAggr>>) {
    let timer = Instant::now();
    let mut map = HashMap::new();
    for e in query_res {
        let acc = map.entry(e.dst).or_insert_with(HashMap::new);
        let acc_grp = acc.entry(e.grp).or_insert_with(PosPostAggr::new);
        acc_grp.pos_list.push(e.clone());
    }
    (timer.elapsed(), map)
}

#[derive(Serialize, Deserialize, Debug)]
struct PosPostAggr {
    pos_list: Vec<Pos>,
}

impl PosPostAggr {
    fn new() -> PosPostAggr {
        PosPostAggr {
            pos_list: Vec::new(),
        }
    }

    fn get_sum(&self) -> BigDecimal {
        self.pos_list
            .iter()
            .map(|e| e.ratio.clone())
            .reduce(|acc, ratio| acc + ratio)
            .unwrap()
    }
}
