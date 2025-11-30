use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::sync::{Mutex, Notify, RwLock};

use crate::{
    commands::Command,
    common::{current_time_ms, current_time_secs_f64, Error, RangeStreamEntryID},
    database::{Database, StreamEntry},
    resp::RespValue,
};

enum ArrayDirection {
    Front,
    Back,
}

pub(crate) struct Engine {
    db: RwLock<Database>,
    notification: Arc<Notify>,
    transaction_store: Mutex<HashMap<u64, Vec<Command>>>,
}

impl Engine {
    pub(crate) fn new() -> Self {
        Self {
            db: RwLock::new(Database::new()),
            notification: Arc::new(Notify::new()),
            transaction_store: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) async fn execute(
        &self,
        command: &Command,
        request_count: u64,
    ) -> Result<RespValue, Error> {
        if !command.is_exec() && !command.is_discard() {
            if self.is_transaction(request_count).await {
                return if command.is_multi() {
                    Ok(RespValue::SimpleString(
                        "ERR MULTI calls can not be nested".to_string(),
                    ))
                } else {
                    {
                        let mut transaction_store = self.transaction_store.lock().await;
                        let transactions = transaction_store.get_mut(&request_count).unwrap();
                        transactions.push(command.clone());
                    }
                    Ok(RespValue::SimpleString("QUEUED".to_string()))
                };
            }
        }

        self.execute_now(command, request_count).await
    }

    async fn execute_now(&self, command: &Command, request_count: u64) -> Result<RespValue, Error> {
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
                    Ok(final_id) => {
                        self.notification.notify_one();
                        Ok(RespValue::BulkString(final_id.to_string()))
                    }
                    Err(err) => Ok(RespValue::SimpleError(err)),
                }
            }

            Command::Xrange(key, start, end, count) => {
                if *count == 0 {
                    return Ok(RespValue::NullBulkString);
                }

                let start = match start {
                    RangeStreamEntryID::Fixed(v) => v,
                    RangeStreamEntryID::Latest => {
                        &self.db.read().await.resolve_latest_stream_id(key)?
                    }
                };

                let end = match end {
                    RangeStreamEntryID::Fixed(v) => v,
                    RangeStreamEntryID::Latest => {
                        &self.db.read().await.resolve_latest_stream_id(key)?
                    }
                };

                match self
                    .db
                    .read()
                    .await
                    .stream_get_range(key, start, end, *count)
                {
                    Ok(stream_entry) => Ok(Self::stream_to_resp(stream_entry)),
                    Err(err) => Ok(RespValue::SimpleError(err)),
                }
            }

            Command::Xread(key_id_pairs, count, blocking_ttl) => {
                let end_ms = current_time_ms() + blocking_ttl.unwrap_or(0);

                // Resolve any `Latest` ids here in the async context (await is allowed).
                let mut resolved_key_id_pairs = vec![];
                for (key, id) in key_id_pairs {
                    let id = match id {
                        RangeStreamEntryID::Fixed(v) => v,
                        RangeStreamEntryID::Latest => {
                            &self.db.read().await.resolve_latest_stream_id(&key)?
                        }
                    };
                    resolved_key_id_pairs.push((key.clone(), id.clone()));
                }

                loop {
                    match self
                        .db
                        .read()
                        .await
                        .stream_read_multi_from_id_exclusive(&resolved_key_id_pairs, *count)
                    {
                        Ok(result) => {
                            if !result.is_empty() || blocking_ttl.is_none() {
                                return Ok(RespValue::Array(
                                    result
                                        .into_iter()
                                        .map(|(key, stream_entry)| {
                                            RespValue::Array(vec![
                                                RespValue::BulkString(key),
                                                Self::stream_to_resp(stream_entry),
                                            ])
                                        })
                                        .collect::<Vec<_>>(),
                                ));
                            }
                        }
                        Err(err) => return Ok(RespValue::SimpleError(err)),
                    }

                    let now_ms = current_time_ms();
                    if end_ms <= now_ms {
                        return Ok(RespValue::NullArray);
                    }
                    let ttl = end_ms - now_ms;

                    tokio::spawn({
                        let notification = self.notification.clone();

                        async move {
                            tokio::time::sleep(Duration::from_millis(ttl as u64)).await;
                            notification.notify_waiters();
                        }
                    });

                    self.notification.notified().await;
                }
            }

            Command::Incr(key) => match self.db.write().await.incr(key) {
                Ok(n) => Ok(RespValue::Integer(n)),
                Err(err) => Ok(RespValue::SimpleError(err)),
            },

            Command::Multi => {
                self.transaction_store
                    .lock()
                    .await
                    .insert(request_count, vec![]);
                Ok(RespValue::SimpleString("OK".to_string()))
            }

            Command::Exec => {
                let mut transaction_store = self.transaction_store.lock().await;

                match transaction_store.remove(&request_count) {
                    Some(commands) => {
                        let mut results = vec![];

                        for command in commands {
                            let result =
                                Box::pin(self.execute_now(&command, request_count)).await?;
                            results.push(result);
                        }

                        Ok(RespValue::Array(results))
                    }
                    None => Ok(RespValue::SimpleError("ERR EXEC without MULTI".to_string())),
                }
            }

            Command::Discard => {
                if self.is_transaction(request_count).await {
                    {
                        let mut transaction_store = self.transaction_store.lock().await;
                        transaction_store.remove(&request_count);
                    }
                    Ok(RespValue::SimpleString("OK".to_string()))
                } else {
                    Ok(RespValue::SimpleError(
                        "ERR DISCARD without MULTI".to_string(),
                    ))
                }
            }
        }
    }

    fn stream_to_resp(stream_entry: StreamEntry) -> RespValue {
        RespValue::Array(
            stream_entry
                .into_iter()
                .map(|value| {
                    RespValue::Array(vec![
                        RespValue::BulkString(value.id.to_string()),
                        RespValue::Array(
                            value
                                .kvpairs
                                .into_iter()
                                .flat_map(|kvpair| {
                                    vec![
                                        RespValue::BulkString(kvpair.0),
                                        RespValue::BulkString(kvpair.1),
                                    ]
                                })
                                .collect::<Vec<_>>(),
                        ),
                    ])
                })
                .collect::<Vec<_>>(),
        )
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
                self.notification.notify_one();
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
                let notification = self.notification.clone();

                async move {
                    tokio::time::sleep(Duration::from_secs_f64(ttl)).await;
                    notification.notify_waiters();
                }
            });

            self.notification.notified().await;
        }
    }

    async fn is_transaction(&self, request_count: u64) -> bool {
        self.transaction_store
            .lock()
            .await
            .contains_key(&request_count)
    }
}
