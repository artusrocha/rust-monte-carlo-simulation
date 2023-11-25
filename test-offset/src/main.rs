//#![allow(dead_code, unused_imports, non_camel_case_types)]

mod pos_fbs;
mod pos_avro;
mod pg_repository;
mod redis_repository;

use pg_repository::Pos;

use rand::Rng;

use serde::{Serialize, Deserialize};

use flatbuffers::FlatBufferBuilder;

use sqlx::types::BigDecimal;

use bigdecimal::ToPrimitive;

use std::collections::BTreeMap;
use std::{
    collections::HashMap,
    env,
    time::Instant,
};

use apache_avro::{to_value, to_avro_datum, from_avro_datum, from_value};

const POS_AVRO_SCHEMA: &str = include_str!("../position.avsc");

fn to_avro_buf(pos: &Pos, schema: &apache_avro::Schema) -> Vec<u8> {

    let pos = pos_avro::PosAvroBuilder::default()
        .dst( pos.dst )
        .acc_id( pos.acc_id )
        .ins_id( pos.ins_id )
        .grp( pos.grp )
        .grpv( pos.grpv.to_owned() )
        .qty( pos.qty.to_f32().unwrap() )
        .factor( pos.factor.to_f32().unwrap() )
        .ratio( pos.ratio.to_f32().unwrap() )
        .build()
        .unwrap();

    to_avro_datum( &schema, to_value(&pos).unwrap() ).unwrap()
}

fn from_avro_buf(pos_avr_buf: &Vec<u8>, schema: &apache_avro::Schema) -> pos_avro::PosAvro {
    let p1 = from_avro_datum(&schema, &mut pos_avr_buf.as_slice(), None).unwrap();
    from_value::<pos_avro::PosAvro>(&p1).unwrap()
}

// #[tokio::main]
async fn _main () -> Result<(), Box<dyn std::error::Error>> {
    
   let mut rng = rand::thread_rng();
   let schema = apache_avro::Schema::parse_str( POS_AVRO_SCHEMA ).unwrap();

    let pos = Pos {
       dst: rng.gen::<i32>(),
       acc_id: rng.gen::<i32>(),
       ins_id: rng.gen::<i32>(),
       grp: rng.gen::<i16>(),
       grpv: "L".to_owned(),
       qty: rng.gen::<i32>(),
       factor: BigDecimal::from(rng.gen::<u32>()),
       ratio:  BigDecimal::from(rng.gen::<u32>()),
    };

    let pos_avr_buf = to_avro_buf(&pos, &schema);
    let pos_avro = from_avro_buf(&pos_avr_buf, &schema);
    eprintln!("av {:?} \t| {:?}", pos_avr_buf.len(), &pos_avro );

    Ok(())
    
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let args: Vec<String> = env::args().collect();

    let schema = apache_avro::Schema::parse_str( POS_AVRO_SCHEMA ).unwrap();

    let par_id = if args.len()>1 && !args[1].is_empty() { args[1].parse().unwrap() } else { 1 };
    eprintln!("par_id {}", par_id);

    let pg_repo = pg_repository::Repo::init().await?;

    let mut redis_repo = redis_repository::Repo::init()?;

    println!("type\tt_query\tt_convert\tt_aggr\tt_total");

    let timer = Instant::now();
    let query_aggr_res = pg_repo.query_aggr(par_id).await?;
    eprintln!("query_aggr | query_aggr_res.len(): {:?} | Time elapsed {:?}", query_aggr_res.len(), timer.elapsed());
    println!("{}\t{}\t{}\t{}", "query_aggr", timer.elapsed().as_millis(), "", "");

    eprintln!("\n========================================================");
 
    let timer = Instant::now();
    let query_res = pg_repo.query_full(par_id).await?;
    eprintln!("query | query_res.len: {:?} | Time elapsed {:?}", query_res.len(), timer.elapsed());
    let t_query = timer.elapsed();

    let timer = Instant::now();
    let pos_aggr = do_post_aggregation(&query_res);
    eprintln!("pos_aggr | query_res.len(): {:?} | pos_aggr.len(): {:?} | Time elapsed {:?}", &query_res.len(), pos_aggr.len(), timer.elapsed());
    let t_aggr = timer.elapsed();
    println!("{}\t{}\t{}\t{}", "query_full", t_query.as_millis(), "", t_aggr.as_millis());

    eprintln!("\n========================================================");

    let timer = Instant::now();
    let pos_fbs = convert_all_to_fbs(&query_res);
    eprintln!("fbs convert | query_res.len(): {:?} | pos_fbs.len(): {:?} | Time elapsed {:?}", &query_res.len(), pos_fbs.len(), timer.elapsed());
    let t_convert = timer.elapsed();

    let timer = Instant::now();
    let pos_fbs_mapped = raw_to_fbs(&pos_fbs);
    eprintln!("fbs aggregation | query_res.len(): {:?} | pos_fbs_aggr.len(): {:?} | Time elapsed {:?}", &query_res.len(), pos_fbs_mapped.len(), timer.elapsed());

    let timer = Instant::now();
    let pos_fbs_aggr = do_pos_fbs_aggregation(&pos_fbs_mapped);
    eprintln!("fbs aggregation | query_res.len(): {:?} | pos_fbs_aggr.len(): {:?} | Time elapsed {:?}", &query_res.len(), pos_fbs_aggr.len(), timer.elapsed());

    let timer = Instant::now();
    redis_repo.save( &pos_fbs, &format!("h:fbs:par:{}", par_id) )?;
    let t_save_rds = timer.elapsed();
    println!("{}\t{}\t{}\t{}", "save_rds_fbs", t_save_rds.as_millis(), t_convert.as_millis(), "");

    let timer = Instant::now();
    let fbs_from_redis = redis_repo.find( &format!("h:fbs:par:{}", par_id) )?;
    let t_rds_read = timer.elapsed();

    let timer = Instant::now();
    let pos_fbs_mapped_from_redis = raw_to_fbs(&fbs_from_redis);
    eprintln!("fbs redis convert | query_res.len(): {:?} | pos_fbs_aggr.len(): {:?} | Time elapsed {:?}", &query_res.len(), pos_fbs_mapped_from_redis.len(), timer.elapsed());
    let t_rds_convert = timer.elapsed();

    let timer = Instant::now();
    let pos_fbs_aggr_from_redis = do_pos_fbs_aggregation(&pos_fbs_mapped_from_redis);
    eprintln!("fbs redis aggregation | query_res.len(): {:?} | pos_fbs_aggr.len(): {:?} | Time elapsed {:?}", &query_res.len(), pos_fbs_aggr_from_redis.len(), timer.elapsed());
    let t_rds_aggr = timer.elapsed();

    println!("{}\t{}\t{}\t{}", "read_rds_fbs", t_rds_read.as_millis(), t_rds_convert.as_millis(), t_rds_aggr.as_millis());

    eprintln!("\n========================================================");

    let timer = Instant::now();
    let pos_mps = convert_all_to_mp(&query_res);
    eprintln!("mps convert | query_res.len(): {:?} | pos_mps.len(): {:?} | Time elapsed {:?}", &query_res.len(), pos_mps.len(), timer.elapsed());

    let timer = Instant::now();
    let pos_mps_pos1 = convert_all_from_mp(&pos_mps);
    eprintln!("mps convert | query_res.len(): {:?} | pos_mps_pos2.len(): {:?} | Time elapsed {:?}", &query_res.len(), pos_mps_pos1.len(), timer.elapsed());

    let timer = Instant::now();
    let pos_mps_pos1_aggr = do_post_aggregation(&pos_mps_pos1);
    eprintln!("pos_mps_pos1_aggr | query_res.len(): {:?} | pos_mps_pos1_aggr.len(): {:?} | Time elapsed {:?}", &query_res.len(), pos_mps_pos1_aggr.len(), timer.elapsed());

    let timer = Instant::now();
    redis_repo.save( &pos_mps, &format!("h:mps:par:{}", par_id) )?;
    let t_rds_mp_save = timer.elapsed();

    let timer = Instant::now();
    let mps_from_redis = redis_repo.find( &format!("h:mps:par:{}", par_id) )?;
    let t_mp_read = timer.elapsed();

    let timer = Instant::now();
    let pos_mps_pos2 = convert_all_from_mp(&mps_from_redis);
    eprintln!("mps convert 2 | pos_mps_pos2.len(): {:?} | Time elapsed {:?}", pos_mps_pos2.len(), timer.elapsed());
    let t_mp_convert = timer.elapsed();

    let timer = Instant::now();
    let redis_mps_aggr = do_post_aggregation(&pos_mps_pos2);
    eprintln!("mps redis aggregation | pos_mps_pos2.len(): {:?} | redis_mps_aggr.len(): {:?} | Time elapsed {:?}", pos_mps_pos2.len(), redis_mps_aggr.len(), timer.elapsed());
    let t_mp_aggr = timer.elapsed();

    println!("{}\t{}\t{}\t{}", "read_rds_mp", t_mp_read.as_millis(), t_mp_convert.as_millis(), t_mp_aggr.as_millis());

    eprintln!("\n========================================================");

    // let timer = Instant::now();
    // let redis_mps_aggr_mp = rmp_serde::to_vec(&redis_mps_aggr).unwrap();
    // redis::cmd("SET").arg("huge").arg(&redis_mps_aggr_mp).execute(&mut redis_conn);
    // eprintln!("huge - mps redis saving | mps_from_redis.len(): {:?} | Time elapsed {:?}", mps_from_redis.len(), timer.elapsed());

    // let timer = Instant::now();
    // let mut redis_read_cmd = redis::cmd("GET");
    // redis_read_cmd.arg("huge");
    // let mps_from_redis = redis_read_cmd.query::<Vec<u8>>(&mut redis_conn)?;
    // eprintln!("huge - mps redis reading | mps_from_redis.len(): {:?} | Time elapsed {:?}", mps_from_redis.len(), timer.elapsed());
    // let t_mp_hg_read = timer.elapsed();

    // let timer = Instant::now();
    // let huge = rmp_serde::from_slice::<HashMap<i32, HashMap<i16, PosPostAggr>>>(&mps_from_redis).unwrap();
    // eprintln!("huge - mps convert | huge.len(): {:?} | Time elapsed {:?}", huge.len(), timer.elapsed());
    // let t_mp_hg_convert = timer.elapsed();
    
    // println!("{}\t{}\t{}\t{}", "read_rds_mp_hg", t_mp_hg_read.as_millis(), t_mp_hg_convert.as_millis(), "");

    // let timer = Instant::now();
    // let mut sum_part =0;
    // let mut sum_full =0;
    // huge.iter()
    //     .for_each(|(_k,v)| {
    //         sum_part += v.len();
    //         v.iter().for_each(|(_k,v)| sum_full += v.pos_list.len());
    //     });
    // eprintln!("huge - count | sum_part: {:?} | sum_full {:?} | Time elapsed {:?}", sum_part, sum_full, timer.elapsed());

    eprintln!("\n========================================================");

    let timer = Instant::now();
    let avro_bufs: Vec<Vec<u8>> = query_res.iter().map(|pos| to_avro_buf(pos, &schema)).collect();
    let t_avro_convert_to = timer.elapsed();

    let timer = Instant::now();
    redis_repo.save(&avro_bufs, &format!("h:avr:par:{}", par_id) )?;
    let t_avro_rds_save = timer.elapsed();

    println!("{}\t{}\t{}\t{}", "save_rds_avro", t_avro_rds_save.as_millis(), t_avro_convert_to.as_millis(), "");

    eprintln!("\n========================================================");

    let timer = Instant::now();
    let avro_from_redis = redis_repo.find( &format!("h:avr:par:{}", par_id) )?;
    let t_avro_rds_read = timer.elapsed();

    let timer = Instant::now();
    let pos_avro_read: Vec<pos_avro::PosAvro> = avro_from_redis.iter().map(|buf| from_avro_buf(&buf, &schema)).collect();
    eprintln!("avro 1: {:?}", pos_avro_read.get(1));
    eprintln!("avro convert from | pos_avro_read.len(): {:?} | Time elapsed {:?}", pos_avro_read.len(), timer.elapsed());
    let t_avro_convert_from = timer.elapsed();
    println!("{}\t{}\t{}\t{}", "read_rds_avro", t_avro_rds_read.as_millis(), t_avro_convert_from.as_millis(), "");

    // let timer = Instant::now();
    // let redis_mps_aggr = do_post_aggregation(&pos_mps_pos2);
    // eprintln!("mps redis aggregation | pos_mps_pos2.len(): {:?} | redis_mps_aggr.len(): {:?} | Time elapsed {:?}", pos_mps_pos2.len(), redis_mps_aggr.len(), timer.elapsed());

    // BAD PERFORMANCE -> ~ 40s   !!!!!!!!!!!!
    // let timer = Instant::now();
    // let mut redis_fbs_iter : redis::Iter<Vec<u8>> = redis::cmd("SSCAN")
    //     .arg( &format!("par:{}", par_id) )
    //     .cursor_arg(1_000_000)
    //     .clone()
    //     .iter(&mut redis_conn)?;
    // let mut redis_fbs_cursor_aggr = HashMap::new();
    // for buf in redis_fbs_iter {
    //     let t5 = pos_fbs::pos::root_as_pos(&buf);
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
    // eprintln!("fbs redis cursor aggregation | fbs_from_redis.len(): {:?} | redis_fbs_cursor_aggr.len(): {:?} | Time elapsed {:?}", fbs_from_redis.len(), redis_fbs_cursor_aggr.len(), timer.elapsed());

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

fn raw_to_fbs(pos_fbs: &Vec<Vec<u8>>)  -> Vec<pos_fbs::pos::Pos> {
    pos_fbs.iter()
     .map(|e| unsafe { pos_fbs::pos::root_as_pos_unchecked(&e) } )
//     .filter(|e| e.is_ok() )
//     .map(|e| e.unwrap() )
    .collect()
}

fn do_pos_fbs_aggregation<'a>(pos_fbs: &'a Vec<pos_fbs::pos::Pos<'a>>)  -> HashMap<u32, HashMap<u16, PosFbsAggr<'a>>> {
    let mut map = HashMap::new();

    pos_fbs.iter().for_each(|fbs| {
        let acc = map.entry(fbs.dst()).or_insert_with(HashMap::new);
        let acc_grp = acc.entry(fbs.grp()).or_insert_with(|| PosFbsAggr::new());
        acc_grp.pos_list.push(*fbs);
     });
    map
}

struct PosFbsAggr<'a> {
    pos_list: Vec<pos_fbs::pos::Pos<'a>>,
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
    let pos_args = pos_fbs::pos::PosArgs {
        dst: pos.dst.unsigned_abs() ,
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
    Vec::from( builder.finished_data() )
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
