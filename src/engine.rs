use std::{sync::Arc, time::Duration};

use tokio::sync::{Notify, RwLock};

use crate::{
    commands::Command,
    common::{current_time_secs_f64, Error},
    database::Database,
    resp::RespValue,
};

enum ArrayDirection {
    Front,
    Back,
}

pub(crate) struct Engine {
    db: RwLock<Database>,
    list_notification: Arc<Notify>,
}

impl Engine {
    pub(crate) fn new() -> Self {
        Self {
            db: RwLock::new(Database::new()),
            list_notification: Arc::new(Notify::new()),
        }
    }

    pub(crate) async fn execute(&self, command: &Command) -> Result<RespValue, Error> {
        match command {
            Command::Ping => Ok(RespValue::SimpleString("PONG".to_string())),

            Command::Echo(arg) => Ok(RespValue::BulkString(arg.clone())),

            Command::Set(key, value, expiry) => {
                match self
                    .db
                    .write()
                    .await
                    .set(key.clone(), value.clone(), expiry.clone())
                {
                    Ok(_) => Ok(RespValue::SimpleString("OK".into())),
                    Err(err) => Ok(RespValue::SimpleError(err)),
                }
            }

            Command::Get(key) => match self.db.read().await.get(key) {
                Ok(Some(v)) => Ok(RespValue::BulkString(v.clone())),
                Ok(None) => Ok(RespValue::NullBulkString),
                Err(err) => Ok(RespValue::SimpleError(err)),
            },

            Command::Lrange(key, start, end) => {
                match self.db.read().await.get_list_lrange(key, *start, *end) {
                    Ok(array) => Ok(RespValue::Array(
                        array
                            .into_iter()
                            .map(|elem| RespValue::BulkString(elem))
                            .collect::<Vec<_>>(),
                    )),
                    Err(err) => Ok(RespValue::SimpleError(err)),
                }
            }

            Command::Llen(key) => match self.db.read().await.list_length(key) {
                Ok(n) => Ok(RespValue::Integer(n as i64)),
                Err(err) => Ok(RespValue::SimpleError(err)),
            },

            Command::Rpush(key, values) => self.push(key, values, ArrayDirection::Back).await,

            Command::Lpush(key, values) => self.push(key, values, ArrayDirection::Front).await,

            Command::Lpop(key) => self.pop(key, ArrayDirection::Front).await,

            Command::Rpop(key) => self.pop(key, ArrayDirection::Back).await,

            Command::Lpopn(key, n) => self.pop_multi(key, n, ArrayDirection::Front).await,

            Command::Rpopn(key, n) => self.pop_multi(key, n, ArrayDirection::Back).await,

            Command::Blpop(keys, timeout_secs) => {
                self.blocking_pop(keys, timeout_secs, ArrayDirection::Front)
                    .await
            }

            Command::Brpop(keys, timeout_secs) => {
                self.blocking_pop(keys, timeout_secs, ArrayDirection::Back)
                    .await
            }

            Command::Type(key) => Ok(RespValue::SimpleString(
                self.db.read().await.get_key_type_name(key).to_string(),
            )),

            Command::Xadd(key, id, entries) => {
                match self
                    .db
                    .write()
                    .await
                    .stream_push(key.clone(), id.clone(), entries.clone())
                {
                    Ok(final_id) => Ok(RespValue::BulkString(final_id.to_string())),
                    Err(err) => Ok(RespValue::SimpleError(err)),
                }
            }
        }
    }

    async fn push(
        &self,
        key: &String,
        values: &Vec<String>,
        dir: ArrayDirection,
    ) -> Result<RespValue, Error> {
        let result = match dir {
            ArrayDirection::Back => self
                .db
                .write()
                .await
                .push_to_array(key.clone(), values.clone()),
            ArrayDirection::Front => self
                .db
                .write()
                .await
                .insert_to_array(key.clone(), values.clone()),
        };
        match result {
            Ok(count) => {
                self.list_notification.notify_one();
                Ok(RespValue::Integer(count as i64))
            }
            Err(err) => Ok(RespValue::SimpleError(err)),
        }
    }

    async fn pop(&self, key: &String, dir: ArrayDirection) -> Result<RespValue, Error> {
        let result = match dir {
            ArrayDirection::Back => self.db.write().await.list_pop_one_back(key),
            ArrayDirection::Front => self.db.write().await.list_pop_one_front(key),
        };
        match result {
            Ok(Some(v)) => return Ok(RespValue::BulkString(v)),
            Ok(None) => return Ok(RespValue::NullBulkString),
            Err(err) => Ok(RespValue::SimpleError(err)),
        }
    }

    async fn pop_multi(
        &self,
        key: &String,
        n: &usize,
        dir: ArrayDirection,
    ) -> Result<RespValue, Error> {
        let result = match dir {
            ArrayDirection::Back => self.db.write().await.list_pop_multi_back(key, *n),
            ArrayDirection::Front => self.db.write().await.list_pop_multi_front(key, *n),
        };
        match result {
            Ok(Some(elems)) => Ok(RespValue::Array(
                elems
                    .into_iter()
                    .map(|e| RespValue::BulkString(e))
                    .collect(),
            )),
            Ok(None) => return Ok(RespValue::NullBulkString),
            Err(err) => Ok(RespValue::SimpleError(err)),
        }
    }

    async fn blocking_pop(
        &self,
        keys: &Vec<String>,
        timeout_secs: &f64,
        dir: ArrayDirection,
    ) -> Result<RespValue, Error> {
        let now_secs = current_time_secs_f64();
        let end_secs = now_secs + timeout_secs;

        loop {
            for key in keys {
                let result = match dir {
                    ArrayDirection::Back => self.db.write().await.list_pop_one_back(key)?,
                    ArrayDirection::Front => self.db.write().await.list_pop_one_front(key)?,
                };
                if let Some(v) = result {
                    return Ok(RespValue::Array(vec![
                        RespValue::BulkString(key.clone()),
                        RespValue::BulkString(v),
                    ]));
                }
            }

            let ttl = end_secs - current_time_secs_f64();
            if ttl <= 0.0 {
                return Ok(RespValue::NullArray);
            }

            tokio::spawn({
                let notification = self.list_notification.clone();

                async move {
                    tokio::time::sleep(Duration::from_secs_f64(ttl)).await;
                    notification.notify_waiters();
                }
            });

            self.list_notification.notified().await;
        }
    }
}
