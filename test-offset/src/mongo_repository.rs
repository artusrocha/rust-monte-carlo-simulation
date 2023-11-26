//use serde::{Deserialize, Serialize};

use mongodb::{Client, Collection, Database};

use mongodb::bson::doc;

use std::env;
use std::time::{Duration, Instant};

use crate::pg_repository::Pos;

pub struct Repo {
    db: Database,
    collection: Collection<Pos>,
}

impl Repo {
    pub async fn new() -> Result<Repo, Box<dyn std::error::Error>> {
        let mongo_url = env::var("MONGODB_URL")?;
        let mongodb_db = env::var("MONGODB_DB")?;
        let mongodb_collection = env::var("MONGODB_COLLECTION")?;
        let db = Client::with_uri_str(&mongo_url)
            .await?
            .database(&mongodb_db);
        let collection: Collection<Pos> = db.collection(&mongodb_collection);
        collection.delete_many(doc![], None).await?;
        let repo = Repo { db, collection };
        Ok(repo)
    }

    pub async fn save_all(&self, items: &Vec<Pos>) -> Result<Duration, Box<dyn std::error::Error>> {
        let timer = Instant::now();
        self.collection.insert_many(items, None).await?;
        eprintln!("mongo saving | time: {:?}", timer.elapsed());
        Ok(timer.elapsed())
    }

    pub async fn find_all(&self) -> Result<(Duration, Vec<Pos>), Box<dyn std::error::Error>> {
        let timer = Instant::now();
        let filters = doc! {};
        let mut cursor = self.collection.find(filters, None).await?;
        let mut results = vec![];
        while cursor.advance().await? {
            results.push(cursor.deserialize_current()?);
        }

        eprintln!(
            "mongo reading | time: {:?} len: {:?}",
            timer.elapsed(),
            results.len()
        );
        Ok((timer.elapsed(), results))
    }

}
