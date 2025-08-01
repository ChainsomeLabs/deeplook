use redis::AsyncCommands;

use redis::{Connection, RedisError};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Error;
use url::Url;

use redis::Commands;

const LATEST_TRADE_SIZE: usize = 100;

impl Clone for Cache {
    fn clone(&self) -> Self {
        let client = redis::Client::open(self._connection_string.clone())
            .expect("Failed creating Redis client in Clone");
        let redis_connection = client
            .get_connection()
            .expect("Failed getting redis connection");

        Cache {
            _connection_string: self._connection_string.clone(),
            redis_connection,
            latest_trades_size: LATEST_TRADE_SIZE,
        }
    }
}

pub struct Cache {
    _connection_string: Url,
    redis_connection: Connection,
    latest_trades_size: usize,
}

#[derive(Debug)]
pub enum CacheError {
    Serialization(Error),
    DeSerialization(Error),
    Redis(RedisError),
}

impl Cache {
    pub fn new(connection_string: Url) -> Self {
        let client =
            redis::Client::open(connection_string.clone()).expect("Failed creating Redis client");
        let redis_connection = client
            .get_connection()
            .expect("Failed getting redis connection");

        Cache {
            _connection_string: connection_string,
            redis_connection,
            latest_trades_size: LATEST_TRADE_SIZE,
        }
    }

    pub fn set<T: Serialize>(&mut self, key: &str, value: &T) -> Result<(), CacheError> {
        let serialized = match serde_json::to_string(value) {
            Ok(v) => v,
            Err(e) => {
                return Err(CacheError::Serialization(e));
            }
        };
        if let Err(e) = self
            .redis_connection
            .set::<&str, String, ()>(key, serialized)
        {
            return Err(CacheError::Redis(e));
        }
        Ok(())
    }

    pub fn get<T: DeserializeOwned>(&mut self, key: &str) -> Result<Option<T>, CacheError> {
        let val: Option<String> = match self.redis_connection.get::<&str, Option<String>>(key) {
            Ok(v) => v,
            Err(e) => {
                return Err(CacheError::Redis(e));
            }
        };

        let val = match val {
            Some(s) => s,
            None => {
                return Ok(None);
            }
        };

        let deserialized =
            serde_json::from_str(&val).map_err(|e| CacheError::DeSerialization(e))?;
        Ok(Some(deserialized))
    }

    pub fn push<T: Serialize>(&mut self, key: &str, value: &T) -> Result<(), CacheError> {
        let serialized = serde_json::to_string(value).map_err(CacheError::Serialization)?;

        self.redis_connection
            .rpush::<&str, String, ()>(key, serialized)
            .map_err(CacheError::Redis)?;

        self.redis_connection
            .ltrim::<&str, ()>(key, -(self.latest_trades_size as isize), -1)
            .map_err(CacheError::Redis)?;

        Ok(())
    }
}

#[derive(Clone)]
pub struct AsyncCache {
    pub client: redis::Client,
}

impl AsyncCache {
    pub fn new(redis_url: Url) -> Self {
        let client =
            redis::Client::open(redis_url).expect("Failed creating Redis client for AsyncCache");
        Self { client }
    }

    pub async fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, CacheError> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(CacheError::Redis)?;
        let val: Option<String> = conn.get(key).await.map_err(CacheError::Redis)?;

        if let Some(json) = val {
            let deserialized = serde_json::from_str(&json).map_err(CacheError::DeSerialization)?;
            Ok(Some(deserialized))
        } else {
            Ok(None)
        }
    }

    pub async fn set<T: Serialize>(&self, key: &str, value: &T) -> Result<(), CacheError> {
        let json = serde_json::to_string(value).map_err(CacheError::Serialization)?;
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(CacheError::Redis)?;
        conn.set(key, json).await.map_err(CacheError::Redis)
    }

    pub async fn get_array<T: DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<Vec<T>>, CacheError> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(CacheError::Redis)?;

        let items: Vec<String> = conn.lrange(key, 0, -1).await.map_err(CacheError::Redis)?;

        if items.is_empty() {
            return Ok(None);
        }

        let mut result = Vec::with_capacity(items.len());
        for item in items {
            let value: T = serde_json::from_str(&item).map_err(CacheError::DeSerialization)?;
            result.push(value);
        }

        Ok(Some(result))
    }
}
