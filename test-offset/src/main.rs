#![allow(dead_code, unused_imports, non_camel_case_types)]

mod position_generated;
mod pos_avro;

use rand::Rng;

use redis::FromRedisValue;
use serde::{Serialize, Deserialize};

use flatbuffers::FlatBufferBuilder;

use position_generated::pos::finish_t_5_buffer;
use position_generated::pos::root_as_t_5;
use position_generated::pos::root_as_t_5_unchecked;
use position_generated::pos::{t5, t5Args, t5Builder};

use sqlx::{
    postgres::PgPoolOptions, types::BigDecimal, FromRow, Pool, Postgres,
};

use bigdecimal::ToPrimitive;

use std::collections::BTreeMap;
use std::{
    collections::HashMap,
    env,
    time::Instant,
};

use apache_avro::{to_value, to_avro_datum_schemata, to_avro_datum, from_avro_datum, from_value};
use apache_avro::Writer;

const POS_AVRO_SCHEMA: &str = include_str!("../position.avsc");

fn to_avro_buf(pos: &Pos) -> Vec<u8> {

    let schema = apache_avro::Schema::parse_str( POS_AVRO_SCHEMA ).unwrap();

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

fn from_avro_buf(pos_avr_buf: &Vec<u8>) -> pos_avro::PosAvro {
    let schema = apache_avro::Schema::parse_str( POS_AVRO_SCHEMA ).unwrap();
    let p1 = from_avro_datum(&schema, &mut pos_avr_buf.as_slice(), None).unwrap();
    from_value::<pos_avro::PosAvro>(&p1).unwrap()
}

// #[tokio::main]
async fn _main () -> Result<(), Box<dyn std::error::Error>> {
    
   let mut rng = rand::thread_rng();
   let schema = apache_avro::Schema::parse_str( POS_AVRO_SCHEMA )?;
//
//    let pos = pos_avro::PosAvroBuilder::default()
//        .dst( rng.gen::<i32>() )
//        .acc_id( rng.gen::<i32>() )
//        .ins_id( rng.gen::<i32>() )
//        .grp( rng.gen::<i32>() )
//        .grpv( "L".to_owned() )
//        .qty( rng.gen::<f32>() )
//        .factor( rng.gen::<f32>() )
//        .ratio(  rng.gen::<f32>() )
//        .build()?;
//
//
//    let pos_mp_buf = rmp_serde::to_vec(&pos).unwrap();
//    eprintln!("mp {:?} \t| {:?}", pos_mp_buf.len(), rmp_serde::from_slice::<pos_avro::PosAvro>(&pos_mp_buf).unwrap());

    // let mut avro_writer = Writer::new( &schema, Vec::new() );
    // let _ = avro_writer.append( to_avro_datum(&schema, to_value(&pos)? )? );
    // let pos_avr_buf = avro_writer.into_inner()?;
//    let pos_avr_buf = to_avro_datum(&schema, to_value(&pos)?)?;

    // let mut avro_reader = apache_avro::Reader::with_schema(&schema, &pos_avr_buf[..])?; //apache_avro::Reader::new( &schema );
    // let av_readed = avro_reader.next()
    //     .map(|o| from_value::<pos_avro::PosAvro>(&o.unwrap()).unwrap()).unwrap();
    // eprintln!("av {:?} \t| {:?}", pos_avr_buf.len(), &av_readed );
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

    let pos_avr_buf = to_avro_buf(&pos);
    let pos_avro = from_avro_buf(&pos_avr_buf);
    eprintln!("av {:?} \t| {:?}", pos_avr_buf.len(), &pos_avro );

    Ok(())
    
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let args: Vec<String> = env::args().collect();

    let par_id = if args.len()>1 && !args[1].is_empty() { args[1].parse().unwrap() } else { 1 };
    eprintln!("par_id {}", par_id);

    let db = get_db_conn_pool().await?;

    let mut redis_conn = get_redis_conn()?;    
 
    let query = sqlx::query_as::<_, Pos>("
        select dst, acc_id,ins_id, grp, grpv, qty, factor, ratio from acc join t5 on acc_id=id where par_id=$1;
    ");

    let query_aggr = sqlx::query_as::<_, PosAggr>("
        select * from (select dst, grp, sum(ratio) as sum from acc join t5 on t5.acc_id=acc.id where acc.par_id=$1 group by dst, grp); -- where sum>0;
    ");

    println!("type\tt_query\tt_convert\tt_aggr\tt_total");

    let timer = Instant::now();
    let query_aggr_res = query_aggr.bind(par_id).fetch_all(&db).await?;
    eprintln!("query_aggr | query_aggr_res.len(): {:?} | Time elapsed {:?}", query_aggr_res.len(), timer.elapsed());
    println!("{}\t{}\t{}\t{}", "query_aggr", timer.elapsed().as_millis(), "", "");

    eprintln!("\n============================\n");
 
    let timer = Instant::now();
    let query_res = query.bind(par_id).fetch_all(&db).await?;
    eprintln!("query | query_res.len: {:?} | Time elapsed {:?}", query_res.len(), timer.elapsed());
    let t_query = timer.elapsed();

    let timer = Instant::now();
    let pos_aggr = do_post_aggregation(&query_res);
    eprintln!("pos_aggr | query_res.len(): {:?} | pos_aggr.len(): {:?} | Time elapsed {:?}", &query_res.len(), pos_aggr.len(), timer.elapsed());
    let t_aggr = timer.elapsed();
    println!("{}\t{}\t{}\t{}", "query_full", t_query.as_millis(), "", t_aggr.as_millis());

    eprintln!("\n============================\n");

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
    let redis_prepare_sadd_cmd = prepare_redis_save_cmd(&pos_fbs, &format!("h:fbs:par:{}", par_id) );
    eprintln!("fbs redis preparing | Time elapsed {:?}", timer.elapsed());
    let t_save_rds = timer.elapsed();

    let timer = Instant::now();
    redis_prepare_sadd_cmd.execute(&mut redis_conn);
    eprintln!("fbs redis saving | Time elapsed {:?}", timer.elapsed());
    println!("{}\t{}\t{}\t{}", "save_rds_fbs", "", t_convert.as_millis(), "");

    let timer = Instant::now();
    let mut redis_read_cmd = redis::cmd("HVALS");
    redis_read_cmd.arg( &format!("h:fbs:par:{}", par_id) );
    let fbs_from_redis = redis_read_cmd.query::<Vec<Vec<u8>>>(&mut redis_conn)?;
    eprintln!("fbs redis reading | fbs_from_redis.len(): {:?} | Time elapsed {:?}", fbs_from_redis.len(), timer.elapsed());
    let t_rds_read = timer.elapsed();

    let timer = Instant::now();
    let pos_fbs_mapped_from_redis = raw_to_fbs(&pos_fbs);
    eprintln!("fbs redis convert | query_res.len(): {:?} | pos_fbs_aggr.len(): {:?} | Time elapsed {:?}", &query_res.len(), pos_fbs_mapped_from_redis.len(), timer.elapsed());
    let t_rds_convert = timer.elapsed();

    let timer = Instant::now();
    let pos_fbs_aggr_from_redis = do_pos_fbs_aggregation(&pos_fbs_mapped_from_redis);
    eprintln!("fbs redis aggregation | query_res.len(): {:?} | pos_fbs_aggr.len(): {:?} | Time elapsed {:?}", &query_res.len(), pos_fbs_aggr_from_redis.len(), timer.elapsed());
    let t_rds_aggr = timer.elapsed();

    println!("{}\t{}\t{}\t{}", "read_rds_fbs", t_rds_read.as_millis(), t_rds_convert.as_millis(), t_rds_aggr.as_millis());

    eprintln!("\n============================\n");

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
    let redis_prepare_sadd_cmd = prepare_redis_save_cmd(&pos_mps, &format!("h:mps:par:{}", par_id) );
    eprintln!("mps redis preparing | Time elapsed {:?}", timer.elapsed());

    let timer = Instant::now();
    redis_prepare_sadd_cmd.execute(&mut redis_conn);
    eprintln!("mps redis saving | Time elapsed {:?}", timer.elapsed());

    let timer = Instant::now();
    let mut redis_read_cmd = redis::cmd("HVALS");
    redis_read_cmd.arg( &format!("h:mps:par:{}", par_id) );
    let mps_from_redis = redis_read_cmd.query::<Vec<Vec<u8>>>(&mut redis_conn)?;
    eprintln!("mps redis reading | mps_from_redis.len(): {:?} | Time elapsed {:?}", mps_from_redis.len(), timer.elapsed());
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

    eprintln!("\n============================\n");

    let timer = Instant::now();
    let redis_mps_aggr_mp = rmp_serde::to_vec(&redis_mps_aggr).unwrap();
    redis::cmd("SET").arg("huge").arg(&redis_mps_aggr_mp).execute(&mut redis_conn);
    eprintln!("huge - mps redis saving | mps_from_redis.len(): {:?} | Time elapsed {:?}", mps_from_redis.len(), timer.elapsed());

    let timer = Instant::now();
    let mut redis_read_cmd = redis::cmd("GET");
    redis_read_cmd.arg("huge");
    let mps_from_redis = redis_read_cmd.query::<Vec<u8>>(&mut redis_conn)?;
    eprintln!("huge - mps redis reading | mps_from_redis.len(): {:?} | Time elapsed {:?}", mps_from_redis.len(), timer.elapsed());
    let t_mp_hg_read = timer.elapsed();

    let timer = Instant::now();
    let huge = rmp_serde::from_slice::<HashMap<i32, HashMap<i16, PosPostAggr>>>(&mps_from_redis).unwrap();
    eprintln!("huge - mps convert | huge.len(): {:?} | Time elapsed {:?}", huge.len(), timer.elapsed());
    let t_mp_hg_convert = timer.elapsed();
    
    println!("{}\t{}\t{}\t{}", "read_rds_mp_hg", t_mp_hg_read.as_millis(), t_mp_hg_convert.as_millis(), "");

    let timer = Instant::now();
    let mut sum_part =0;
    let mut sum_full =0;
    huge.iter()
        .for_each(|(_k,v)| {
            sum_part += v.len();
            v.iter().for_each(|(_k,v)| sum_full += v.pos_list.len());
        });
    eprintln!("huge - count | sum_part: {:?} | sum_full {:?} | Time elapsed {:?}", sum_part, sum_full, timer.elapsed());

    eprintln!("\n============================\n");

    let timer = Instant::now();
    let avro_bufs: Vec<Vec<u8>> = query_res.iter().map(|pos| to_avro_buf(pos)).collect();
    let t_avro_convert_to = timer.elapsed();
    let timer = Instant::now();
    let redis_prepare_sadd_cmd = prepare_redis_save_cmd(&avro_bufs, &format!("h:avr:par:{}", par_id) );
    eprintln!("avro redis preparing | Time elapsed {:?}", timer.elapsed());
    redis_prepare_sadd_cmd.execute(&mut redis_conn);
    eprintln!("avro redis saving | Time elapsed {:?}", timer.elapsed());
    let t_avro_rds_save = timer.elapsed();
    println!("{}\t{}\t{}\t{}", "save_rds_avro", t_avro_rds_save.as_millis(), t_avro_convert_to.as_millis(), "");

    eprintln!("\n============================\n");

    let timer = Instant::now();
    let mut redis_read_cmd = redis::cmd("HVALS");
    redis_read_cmd.arg( &format!("h:avr:par:{}", par_id) );
    let avro_from_redis = redis_read_cmd.query::<Vec<Vec<u8>>>(&mut redis_conn)?;
    eprintln!("avro redis reading | avro_from_redis.len(): {:?} | Time elapsed {:?}", avro_from_redis.len(), timer.elapsed());
    let t_avro_rds_read = timer.elapsed();

    let timer = Instant::now();
    let pos_avro_read: Vec<pos_avro::PosAvro> = avro_from_redis.iter().map(|buf| from_avro_buf(&buf)).collect();
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

fn prepare_redis_save_cmd(buf_items:&Vec<Vec<u8>>, key: &str) -> redis::Cmd {
    let mut map: BTreeMap<String, &[u8]> = BTreeMap::new();
    for (i, e) in buf_items.iter().enumerate() {
        //        let fbs = convert_to_fbs(e);
        map.insert( i.to_string(), e.as_slice() );
    }
    let mut redis_prepare_sadd_cmd = redis::cmd("HSET");
    redis_prepare_sadd_cmd.arg(key).arg(map);
    redis_prepare_sadd_cmd
}

fn raw_to_fbs(pos_fbs: &Vec<Vec<u8>>)  -> Vec<t5> {
    pos_fbs.iter()
     .map(|e| unsafe { root_as_t_5_unchecked(&e) } )
//     .filter(|e| e.is_ok() )
//     .map(|e| e.unwrap() )
    .collect()
}

fn do_pos_fbs_aggregation<'a>(pos_fbs: &'a Vec<t5<'a>>)  -> HashMap<u32, HashMap<u16, PosFbsAggr<'a>>> {
    let mut map = HashMap::new();

    pos_fbs.iter()
//     .map(|e| unsafe { root_as_t_5_unchecked(&e) } )
//     .filter(|e| e.is_ok() )
//     .map(|e| e.unwrap() )
     .for_each(|fbs| {
        let acc = map.entry(fbs.dst()).or_insert_with(HashMap::new);
        let acc_grp = acc.entry(fbs.grp()).or_insert_with(|| PosFbsAggr::new());
        acc_grp.pos_list.push(*fbs);
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