use redis::{ Connection, RedisError };
use serde_json::Error;
use url::Url;
use serde::{ Serialize, de::DeserializeOwned };

use redis::Commands;

impl Clone for Cache {
    fn clone(&self) -> Self {
        let client = redis::Client
            ::open(self._connection_string.clone())
            .expect("Failed creating Redis client in Clone");
        let redis_connection = client.get_connection().expect("Failed getting redis connection");

        Cache {
            _connection_string: self._connection_string.clone(),
            redis_connection,
        }
    }
}

pub struct Cache {
    _connection_string: Url,
    redis_connection: Connection,
}

#[derive(Debug)]
pub enum CacheError {
    Serialization(Error),
    DeSerialization(Error),
    Redis(RedisError),
}

impl Cache {
    pub fn new(connection_string: Url) -> Self {
        let client = redis::Client
            ::open(connection_string.clone())
            .expect("Failed creating Redis client");
        let redis_connection = client.get_connection().expect("Failed getting redis connection");

        Cache { _connection_string: connection_string, redis_connection }
    }

    pub fn set<T: Serialize>(&mut self, key: &str, value: &T) -> Result<(), CacheError> {
        let serialized = match serde_json::to_string(value) {
            Ok(v) => v,
            Err(e) => {
                return Err(CacheError::Serialization(e));
            }
        };
        if let Err(e) = self.redis_connection.set::<&str, String, ()>(key, serialized) {
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

        let deserialized = serde_json::from_str(&val).map_err(|e| CacheError::DeSerialization(e))?;
        Ok(Some(deserialized))
    }
}
