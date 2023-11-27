use mongodb::{Client, Collection, Database};
use mongodb::bson::doc;
use serde::{Serialize, Deserialize};
use std::env;
use std::time::{Duration, Instant};
use crate::pg_repository::Pos;

pub struct Repo {
    db: Database,
    collection: Collection<MongoDocWrap<PosId, Pos>>,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
struct MongoDocWrap<ID, T> {
    _id: ID,
    val: T,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PosId {
    ins_id: i32,
    acc_id: i32,
}

impl Repo {
    pub async fn new() -> Result<Repo, Box<dyn std::error::Error>> {
        let mongo_url = env::var("MONGODB_URL")?;
        let mongodb_db = env::var("MONGODB_DB")?;
        let mongodb_collection = env::var("MONGODB_COLLECTION")?;
        let db = Client::with_uri_str(&mongo_url)
            .await?
            .database(&mongodb_db);
        let collection: Collection<MongoDocWrap<PosId, Pos>> = db.collection(&mongodb_collection);
        //collection.drop(None).await?;
        let repo = Repo { db, collection };
        Ok(repo)
    }

    pub async fn save_all(&self, items: &Vec<Pos>) -> Result<Duration, Box<dyn std::error::Error>> {
        let timer = Instant::now();
        let wraps: Vec<MongoDocWrap<PosId, Pos>> = items.iter()
            .map(|item| MongoDocWrap { 
                _id: PosId{ ins_id: item.ins_id, acc_id: item.acc_id}, 
                val: item.clone() 
            })
            .collect();
        self.collection.insert_many(wraps, None).await?;
        eprintln!("mongo saving | time: {:?}", timer.elapsed());
        Ok(timer.elapsed())
    }

    pub async fn find_all(&self) -> Result<(Duration, Vec<Pos>), Box<dyn std::error::Error>> {
        let timer = Instant::now();
        let filters = doc! {};
        let mut cursor = self.collection.find(filters, None).await?;
        let mut results = vec![];
        while cursor.advance().await? {
            results.push(cursor.deserialize_current()?.val);
        }

        eprintln!(
            "mongo reading | time: {:?} len: {:?}",
            timer.elapsed(),
            results.len()
        );
        Ok((timer.elapsed(), results))
    }

}
