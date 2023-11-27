use std::collections::BTreeMap;
use std::time::Duration;
use std::{env, time::Instant};

pub(crate) struct Repo {
    redis_conn: redis::Connection,
}

impl Repo {
    pub fn new() -> Result<Repo, Box<dyn std::error::Error>> {
        let redis_conn = Self::get_redis_conn()?;
        let repo = Repo { redis_conn };
        Ok(repo)
    }

    pub fn find(&mut self, key: &str) -> Result<(Duration, Vec<Vec<u8>>), Box<dyn std::error::Error>> {
        let timer = Instant::now();
        let mut redis_read_cmd = redis::cmd("HVALS");
        redis_read_cmd.arg(key);
        let result = redis_read_cmd.query::<Vec<Vec<u8>>>(&mut self.redis_conn)?;
        eprintln!("redis reading | key: {}, time: {:?}", &key, timer.elapsed());
        Ok((timer.elapsed(), result))
    }

    pub fn save(
        &mut self,
        buf_items: &[Vec<u8>],
        key: &str,
    ) -> Result<Duration, Box<dyn std::error::Error>> {
        let timer = Instant::now();
        let command = Self::prepare_redis_save_cmd(buf_items, key);
        command.execute(&mut self.redis_conn);
        eprintln!("redis saving | key: {}, time: {:?}", &key, timer.elapsed());
        Ok(timer.elapsed())
    }

    fn prepare_redis_save_cmd(buf_items: &[Vec<u8>], key: &str) -> redis::Cmd {
        let timer = Instant::now();
        let mut map: BTreeMap<String, &[u8]> = BTreeMap::new();
        for (i, e) in buf_items.iter().enumerate() {
            map.insert(i.to_string(), e.as_slice());
        }
        let mut redis_prepare_sadd_cmd = redis::cmd("HSET");
        redis_prepare_sadd_cmd.arg(key).arg(map);
        eprintln!(
            "redis preparing | key: {}, time: {:?}",
            &key,
            timer.elapsed()
        );
        redis_prepare_sadd_cmd
    }

    fn get_redis_conn() -> Result<redis::Connection, Box<dyn std::error::Error>> {
        let url = env::var("REDIS_URL")?;
        let client = redis::Client::open(url)?;
        let con = client.get_connection()?;
        Ok(con)
    }
}
