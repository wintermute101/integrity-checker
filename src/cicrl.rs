use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use serde::{Deserialize, Serialize};
use redb::{Database, TableDefinition, Value};
use postcard::{from_bytes, to_allocvec};
use log::{error, trace};
use reqwest::{Client, StatusCode};
use super::types::Hash;
use super::error::IntegrityWatcherError;

const TABLE_HASH: TableDefinition<Hash, CacheEntry> = TableDefinition::new("circl_cache");

#[derive(Debug,Serialize,Deserialize)]
struct CacheEntry{
    score: Option<u8>,
    entry_time: i64,
}

impl CacheEntry{
    fn new(score: Option<u8>) -> Self{
        let t = chrono::Utc::now().timestamp();
        CacheEntry { score, entry_time: t }
    }

    fn is_valid(&self) -> bool{
        let end = if self.score.is_some(){
            chrono::Utc::now() + chrono::Duration::days(30)
        } else{
            chrono::Utc::now() + chrono::Duration::days(7)
        };
        self.entry_time < end.timestamp()
    }

    fn get_score(&self) -> Option<u8>{
        //assume cache is valid because it was cleared on start
        self.score
    }
}

impl Value for CacheEntry{
    type SelfType<'a> = Self;
    type AsBytes<'a> = Vec<u8>;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
        where Self: 'a{
        from_bytes(data).unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a> {
        to_allocvec(value).unwrap()
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("FileMetadata")
    }

}

struct CirclCache{
    db: Database,
}

impl CirclCache {
    fn new(path: &str) -> Result<Self, IntegrityWatcherError> {
        let db = Database::create(path)?;
        let write_txn = db.begin_write()?;
        {
            let _table = write_txn.open_table(TABLE_HASH)?;
        }
        write_txn.commit()?;

        Ok(CirclCache { db })
    }

    fn clear_old(&self) -> Result<(), IntegrityWatcherError>{
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(TABLE_HASH)?;
            table.retain(|_h,v| v.is_valid())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    fn insert(&self, hash: &Hash, entry: CacheEntry) -> Result<(), IntegrityWatcherError>{
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(TABLE_HASH)?;
            trace!("Adding hash: {hash}: {entry:?}");
            table.insert(hash, entry)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    fn contains(&self, hash: &Hash) -> Result<Option<CacheEntry>, IntegrityWatcherError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(TABLE_HASH)?;
        let r = table.get(hash)?;
        Ok(r.map(|v| v.value()))
    }
}

pub struct CirclQuery{
    client: Arc<Client>,
    limit: Arc<Semaphore>,
    cache: CirclCache,
}

impl CirclQuery {
    pub fn new(path: &str) -> Result<Self, IntegrityWatcherError>{
        let client = Arc::new(Client::builder().timeout(Duration::from_secs(3)).build()?);
        let limit = Arc::new(Semaphore::new(8));
        let cache = CirclCache::new(path)?;
        cache.clear_old()?;
        Ok(CirclQuery{ client, limit, cache })
    }

    pub async fn query(&self, hash: &Hash) -> Result<Option<u8>, IntegrityWatcherError>{
        #[derive(Deserialize)]
        struct HashLookupResponse {
            #[serde(rename = "hashlookup:trust")]
            trust_score: u8,
        }

        if let Some(score) = self.cache.contains(hash)?{
            return Ok(score.get_score());
        }

        let client = self.client.clone();
        let limit = self.limit.clone();
        let _permit = limit.acquire().await?;

        let url = format!("https://hashlookup.circl.lu/lookup/sha256/{}", hash);
        let retries = 3;
        let mut cnt = 0;
        loop{
            cnt += 1;
            let response = match client.get(&url).send().await{
                Ok(r) => r,
                Err(e) =>{
                    if cnt == retries{
                        return Err(e.into());
                    }
                    error!("Error {e} retrying");
                    tokio::time::sleep(Duration::from_millis(50*cnt)).await;
                    continue;
                }
            };
            let status = response.status();
            match status {
                StatusCode::OK => {
                    let r =  response.json::<HashLookupResponse>().await?;
                    self.cache.insert(hash, CacheEntry::new(Some(r.trust_score)))?;
                    return Ok(Some(r.trust_score))
                }
                StatusCode::NOT_FOUND =>{
                    self.cache.insert(hash, CacheEntry::new(None))?;
                    return Ok(None)
                }
                _ => {
                    if cnt == retries{
                        return Err(IntegrityWatcherError::InvalidReponse { status: status.as_u16(), hash: hash.clone() })
                    }
                    else{
                        error!("Got wrong status {status} on {url} retrying ");
                    }
                }
            };
        }
    }

}