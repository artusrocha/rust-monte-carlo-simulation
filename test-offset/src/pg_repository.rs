use serde::{Deserialize, Serialize};

use sqlx::{postgres::PgPoolOptions, query::QueryAs, types::BigDecimal, FromRow, Pool, Postgres};

use std::{
    env,
    time::{Duration, Instant},
};

#[derive(Debug, FromRow, Clone)]
pub struct PosAggr {
    pub dst: i32,
    pub grp: i16,
    //pub grpv: String,
    pub sum: BigDecimal,
}

#[derive(Debug, FromRow, Clone, Serialize, Deserialize)]
pub struct Pos {
    pub dst: i32,
    pub acc_id: i32,
    pub ins_id: i32,
    pub grp: i16,
    pub grpv: String,
    pub qty: i32,
    pub factor: BigDecimal,
    pub ratio: BigDecimal,
}

pub struct Repo {
    db: Pool<Postgres>,
}

impl Repo {
    pub async fn new() -> Result<Repo, Box<dyn std::error::Error>> {
        let repo = Repo {
            db: Self::get_db_conn_pool().await?,
        };
        Ok(repo)
    }

    pub async fn query_aggr(
        &self,
        id: i32,
    ) -> Result<(Duration, Vec<PosAggr>), Box<dyn std::error::Error>> {
        let timer = Instant::now();
        let query_aggr = sqlx::query_as::<_, PosAggr>("
            select * from (select dst, grp, sum(ratio) as sum from acc join t5 on t5.acc_id=acc.id where acc.par_id=$1 group by dst, grp); -- where sum>0;
        ");

        let query_aggr_res = query_aggr.bind(id).fetch_all(&self.db).await?;

        Ok((timer.elapsed(), query_aggr_res))
    }

    pub async fn query_full(
        &self,
        id: i32,
    ) -> Result<(Duration, Vec<Pos>), Box<dyn std::error::Error>> {
        let timer = Instant::now();

        let query = sqlx::query_as::<_, Pos>("
            select dst, acc_id,ins_id, grp, grpv, qty, factor, ratio from acc join t5 on acc_id=id where par_id=$1;
        ");

        let query_res = query.bind(id).fetch_all(&self.db).await?;

        Ok((timer.elapsed(), query_res))
    }

    async fn get_db_conn_pool() -> Result<Pool<Postgres>, Box<dyn std::error::Error>> {
        let database_url = env::var("DATABASE_URL")?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await?;
        Ok(pool)
    }
}
