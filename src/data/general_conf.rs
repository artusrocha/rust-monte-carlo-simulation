use sqlx::{types::BigDecimal, FromRow, Pool, Postgres};
use std::time::{Duration, Instant};

#[derive(Debug, FromRow, Clone)]
pub struct GeneralConf {
    pub id: i32,                                          // SERIAL PRIMARY KEY,
    pub default_simulation_forecast_days: i16, // SMALLINT NOT NULL CHECK(default_simulation_forecast_days >= 0),
    pub default_scenario_random_range_factor: BigDecimal, // DECIMAL(3,2) NOT NULL,
    pub default_maximum_historic_days: i16, // SMALLINT NOT NULL CHECK(default_maximum_historic_days >= 0),
                                            //    pub created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
}

pub struct GeneralConfRepository {
    db: Pool<Postgres>,
}

impl GeneralConfRepository {
    pub fn new(db: Pool<Postgres>) -> GeneralConfRepository {
        GeneralConfRepository { db: db }
    }

    pub async fn find_last(&self) -> Result<(Duration, GeneralConf), Box<dyn std::error::Error>> {
        let timer = Instant::now();

        let query = sqlx::query_as::<_, GeneralConf>(
            "
            SELECT 
                id,
                default_simulation_forecast_days,
                default_scenario_random_range_factor,
                default_maximum_historic_days
            FROM general_conf
            ORDER BY id DESC
            LIMIT 1;
        ",
        );

        let query_res = query.fetch_one(&self.db).await?;

        Ok((timer.elapsed(), query_res))
    }

    pub async fn find_all(
        &self,
    ) -> Result<(Duration, Vec<GeneralConf>), Box<dyn std::error::Error>> {
        let timer = Instant::now();

        let query = sqlx::query_as::<_, GeneralConf>(
            "
            SELECT 
                id,
                default_simulation_forecast_days,
                default_scenario_random_range_factor,
                default_maximum_historic_days
            FROM general_conf
            ORDER BY id ASC;
        ",
        );

        let query_res = query.fetch_all(&self.db).await?;

        Ok((timer.elapsed(), query_res))
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use sqlx::postgres::PgPoolOptions;

    use super::*;

    #[tokio::test]
    async fn find_all() {
        let repo = get_db_repo().await;
        let result = repo.find_all().await;
        let (elapsed, confs) = result.unwrap();
        assert_eq!(confs.len(), 3);
        eprintln!("Query took: {:?}, result: {:?}", elapsed, confs);
    }

    #[tokio::test]
    async fn find_last() {
        let repo = get_db_repo().await;
        let result = repo.find_last().await;
        let (elapsed, conf) = result.unwrap();
        assert_eq!(1, 1);
        assert_eq!(3, conf.id);
        eprintln!("Query took: {:?}, result: {:?}", elapsed, conf);
    }

    async fn get_db_repo() -> GeneralConfRepository {
        let database_url = env::var("DATABASE_URL").unwrap();
        eprintln!("DATABASE_URL: {:?}", database_url);
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await
            .unwrap();
        GeneralConfRepository::new(pool)
    }
}
