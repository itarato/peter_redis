use std::{sync::Arc, time::Duration};

use tokio::sync::{Notify, RwLock};

use crate::{
    commands::Command,
    common::{current_time_secs_f64, Error},
    database::Database,
    resp::RespValue,
};

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
                Ok(None) => Ok(RespValue::Null),
                Err(err) => Ok(RespValue::SimpleError(err)),
            },

            Command::Rpush(key, values) => {
                match self
                    .db
                    .write()
                    .await
                    .push_to_array(key.clone(), values.clone())
                {
                    Ok(count) => {
                        self.list_notification.notify_one();
                        Ok(RespValue::Integer(count as i64))
                    }
                    Err(err) => Ok(RespValue::SimpleError(err)),
                }
            }

            Command::Lpush(key, values) => {
                match self
                    .db
                    .write()
                    .await
                    .insert_to_array(key.clone(), values.clone())
                {
                    Ok(count) => {
                        self.list_notification.notify_one();
                        Ok(RespValue::Integer(count as i64))
                    }
                    Err(err) => Ok(RespValue::SimpleError(err)),
                }
            }

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

            Command::Lpop(key) => match self.db.write().await.list_pop_one_front(key) {
                Ok(Some(v)) => return Ok(RespValue::BulkString(v)),
                Ok(None) => return Ok(RespValue::Null),
                Err(err) => Ok(RespValue::SimpleError(err)),
            },

            Command::Rpop(key) => match self.db.write().await.list_pop_one_back(key) {
                Ok(Some(v)) => return Ok(RespValue::BulkString(v)),
                Ok(None) => return Ok(RespValue::Null),
                Err(err) => Ok(RespValue::SimpleError(err)),
            },

            Command::Lpopn(key, n) => match self.db.write().await.list_pop_multi_front(key, *n) {
                Ok(Some(elems)) => Ok(RespValue::Array(
                    elems
                        .into_iter()
                        .map(|e| RespValue::BulkString(e))
                        .collect(),
                )),
                Ok(None) => return Ok(RespValue::Null),
                Err(err) => Ok(RespValue::SimpleError(err)),
            },

            Command::Rpopn(key, n) => match self.db.write().await.list_pop_multi_back(key, *n) {
                Ok(Some(elems)) => Ok(RespValue::Array(
                    elems
                        .into_iter()
                        .map(|e| RespValue::BulkString(e))
                        .collect(),
                )),
                Ok(None) => return Ok(RespValue::Null),
                Err(err) => Ok(RespValue::SimpleError(err)),
            },

            Command::Blpop(keys, timeout_secs) => {
                let now_secs = current_time_secs_f64();
                let end_secs = now_secs + timeout_secs;

                loop {
                    for key in keys {
                        if let Some(v) = self.db.write().await.list_pop_one_front(key)? {
                            return Ok(RespValue::Array(vec![
                                RespValue::BulkString(key.clone()),
                                RespValue::BulkString(v),
                            ]));
                        }
                    }

                    let ttl = end_secs - current_time_secs_f64();
                    if ttl <= 0.0 {
                        return Ok(RespValue::Null);
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

            Command::Brpop(keys, timeout_secs) => {
                let now_secs = current_time_secs_f64();
                let end_secs = now_secs + timeout_secs;

                loop {
                    for key in keys {
                        if let Some(v) = self.db.write().await.list_pop_one_back(key)? {
                            return Ok(RespValue::Array(vec![
                                RespValue::BulkString(key.clone()),
                                RespValue::BulkString(v),
                            ]));
                        }
                    }

                    let ttl = end_secs - current_time_secs_f64();
                    if ttl <= 0.0 {
                        return Ok(RespValue::Null);
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
    }
}
