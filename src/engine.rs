use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Context;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpSocket, TcpStream},
    sync::{Mutex, Notify, RwLock},
};

use crate::{
    commands::Command,
    common::{
        current_time_ms, current_time_secs_f64, new_master_replid, read_bulk_bytes_from_tcp_stream,
        read_resp_value_from_tcp_stream, ClientCapability, ClientInfo, Error, RangeStreamEntryID,
        ReaderRole, ReplicationRole, WriterRole,
    },
    database::{Database, StreamEntry},
    resp::RespValue,
};

const INFO_SECTIONS: [&'static str; 1] = ["replication"];

enum ArrayDirection {
    Front,
    Back,
}

pub(crate) struct Engine {
    db: RwLock<Database>,
    notification: Arc<Notify>,
    transaction_store: Mutex<HashMap<u64, Vec<Command>>>,
    replication_role: RwLock<ReplicationRole>,
}

impl Engine {
    pub(crate) fn new(replica_of: Option<(String, u16)>) -> Self {
        let replication_role = match replica_of {
            Some((host, port)) => ReplicationRole::Reader(ReaderRole {
                writer_host: host,
                writer_port: port,
            }),
            None => ReplicationRole::Writer(WriterRole {
                replid: new_master_replid(),
                offset: 0,
                clients: HashMap::new(),
            }),
        };

        Self {
            db: RwLock::new(Database::new()),
            notification: Arc::new(Notify::new()),
            transaction_store: Mutex::new(HashMap::new()),
            replication_role: RwLock::new(replication_role),
        }
    }

    pub(crate) async fn init(&self, server_port: u16) -> Result<(), Error> {
        if !self.replication_role.read().await.is_reader() {
            return Ok(());
        }

        let (writer_host, writer_port) = {
            let ReplicationRole::Reader(ref reader) = *self.replication_role.read().await else {
                unreachable!();
            };

            (reader.writer_host.clone(), reader.writer_port)
        };

        self.handshake(server_port, writer_host, writer_port).await
    }

    pub(crate) async fn handshake(
        &self,
        server_port: u16,
        writer_host: String,
        writer_port: u16,
    ) -> Result<(), Error> {
        let socket_addr = {
            if let Ok(addr) =
                format!("{}:{}", writer_host, writer_port).parse::<std::net::SocketAddr>()
            {
                addr
            } else {
                let mut addrs = tokio::net::lookup_host((writer_host.as_str(), writer_port))
                    .await
                    .context("lookup-host")?;
                addrs.next().ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "failed to resolve writer address",
                    )
                })?
            }
        };

        let mut stream = TcpSocket::new_v4()?
            .connect(socket_addr)
            .await
            .context("connecting-to-writer")?;

        Self::handshake_step(
            &mut stream,
            RespValue::Array(vec![RespValue::BulkString("PING".into())]),
            RespValue::SimpleString("PONG".to_string()),
        )
        .await?;

        Self::handshake_step(
            &mut stream,
            RespValue::Array(vec![
                RespValue::BulkString("REPLCONF".into()),
                RespValue::BulkString("listening-port".into()),
                RespValue::BulkString(format!("{}", server_port)),
            ]),
            RespValue::SimpleString("OK".to_string()),
        )
        .await?;

        Self::handshake_step(
            &mut stream,
            RespValue::Array(vec![
                RespValue::BulkString("REPLCONF".into()),
                RespValue::BulkString("capa".into()),
                RespValue::BulkString("psync2".into()),
            ]),
            RespValue::SimpleString("OK".to_string()),
        )
        .await?;

        stream
            .write_all(
                &RespValue::Array(vec![
                    RespValue::BulkString("PSYNC".into()),
                    RespValue::BulkString("?".into()),
                    RespValue::BulkString("-1".into()),
                ])
                .serialize(),
            )
            .await
            .context("responding-to-writer")?;

        let response = read_resp_value_from_tcp_stream(&mut stream).await?;
        debug!("Handshake response: {:?}", response);

        let response = read_bulk_bytes_from_tcp_stream(&mut stream).await?;
        debug!("Handshake final response: {} bytes", response.len());

        Ok(())
    }

    pub(crate) async fn execute(
        &self,
        command: &Command,
        request_count: u64,
    ) -> Result<Vec<RespValue>, Error> {
        if !command.is_exec() && !command.is_discard() {
            if self.is_transaction(request_count).await {
                return if command.is_multi() {
                    Ok(vec![RespValue::SimpleString(
                        "ERR MULTI calls can not be nested".to_string(),
                    )])
                } else {
                    {
                        let mut transaction_store = self.transaction_store.lock().await;
                        let transactions = transaction_store.get_mut(&request_count).unwrap();
                        transactions.push(command.clone());
                    }
                    Ok(vec![RespValue::SimpleString("QUEUED".to_string())])
                };
            }
        }

        self.execute_now(command, request_count).await
    }

    async fn execute_now(
        &self,
        command: &Command,
        request_count: u64,
    ) -> Result<Vec<RespValue>, Error> {
        match command {
            Command::Ping => Ok(vec![RespValue::SimpleString("PONG".to_string())]),

            Command::Echo(arg) => Ok(vec![RespValue::BulkString(arg.clone())]),

            Command::Set(key, value, expiry) => {
                match self
                    .db
                    .write()
                    .await
                    .set(key.clone(), value.clone(), expiry.clone())
                {
                    Ok(_) => Ok(vec![RespValue::SimpleString("OK".into())]),
                    Err(err) => Ok(vec![RespValue::SimpleError(err)]),
                }
            }

            Command::Get(key) => match self.db.read().await.get(key) {
                Ok(Some(v)) => Ok(vec![RespValue::BulkString(v.clone())]),
                Ok(None) => Ok(vec![RespValue::NullBulkString]),
                Err(err) => Ok(vec![RespValue::SimpleError(err)]),
            },

            Command::Lrange(key, start, end) => {
                match self.db.read().await.get_list_lrange(key, *start, *end) {
                    Ok(array) => Ok(vec![RespValue::Array(
                        array
                            .into_iter()
                            .map(|elem| RespValue::BulkString(elem))
                            .collect::<Vec<_>>(),
                    )]),
                    Err(err) => Ok(vec![RespValue::SimpleError(err)]),
                }
            }

            Command::Llen(key) => match self.db.read().await.list_length(key) {
                Ok(n) => Ok(vec![RespValue::Integer(n as i64)]),
                Err(err) => Ok(vec![RespValue::SimpleError(err)]),
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

            Command::Type(key) => Ok(vec![RespValue::SimpleString(
                self.db.read().await.get_key_type_name(key).to_string(),
            )]),

            Command::Xadd(key, id, entries) => {
                match self
                    .db
                    .write()
                    .await
                    .stream_push(key.clone(), id.clone(), entries.clone())
                {
                    Ok(final_id) => {
                        self.notification.notify_one();
                        Ok(vec![RespValue::BulkString(final_id.to_string())])
                    }
                    Err(err) => Ok(vec![RespValue::SimpleError(err)]),
                }
            }

            Command::Xrange(key, start, end, count) => {
                if *count == 0 {
                    return Ok(vec![RespValue::NullBulkString]);
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
                    Ok(stream_entry) => Ok(vec![Self::stream_to_resp(stream_entry)]),
                    Err(err) => Ok(vec![RespValue::SimpleError(err)]),
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
                                return Ok(vec![RespValue::Array(
                                    result
                                        .into_iter()
                                        .map(|(key, stream_entry)| {
                                            RespValue::Array(vec![
                                                RespValue::BulkString(key),
                                                Self::stream_to_resp(stream_entry),
                                            ])
                                        })
                                        .collect::<Vec<_>>(),
                                )]);
                            }
                        }
                        Err(err) => return Ok(vec![RespValue::SimpleError(err)]),
                    }

                    let now_ms = current_time_ms();
                    if end_ms <= now_ms {
                        return Ok(vec![RespValue::NullArray]);
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
                Ok(n) => Ok(vec![RespValue::Integer(n)]),
                Err(err) => Ok(vec![RespValue::SimpleError(err)]),
            },

            Command::Multi => {
                self.transaction_store
                    .lock()
                    .await
                    .insert(request_count, vec![]);
                Ok(vec![RespValue::SimpleString("OK".to_string())])
            }

            Command::Exec => {
                let mut transaction_store = self.transaction_store.lock().await;

                match transaction_store.remove(&request_count) {
                    Some(commands) => {
                        let mut results = vec![];

                        for command in commands {
                            let mut result =
                                Box::pin(self.execute_now(&command, request_count)).await?;
                            if result.len() != 1 {
                                return Err("Unacceptable mutli command response length".into());
                            }

                            results.push(result.remove(0));
                        }

                        Ok(vec![RespValue::Array(results)])
                    }
                    None => Ok(vec![RespValue::SimpleError(
                        "ERR EXEC without MULTI".to_string(),
                    )]),
                }
            }

            Command::Discard => {
                if self.is_transaction(request_count).await {
                    {
                        let mut transaction_store = self.transaction_store.lock().await;
                        transaction_store.remove(&request_count);
                    }
                    Ok(vec![RespValue::SimpleString("OK".to_string())])
                } else {
                    Ok(vec![RespValue::SimpleError(
                        "ERR DISCARD without MULTI".to_string(),
                    )])
                }
            }

            Command::Info(sections) => {
                let mut section_strs = String::new();
                if sections.is_empty() {
                    for section_name in INFO_SECTIONS {
                        section_strs.push_str(&self.section_info(section_name).await);
                    }
                } else {
                    for section_name in sections {
                        section_strs.push_str(&self.section_info(section_name).await);
                    }
                }

                Ok(vec![RespValue::BulkString(section_strs)])
            }

            Command::Replconf(args) => {
                if self.replication_role.read().await.is_writer() {
                    if args.len() == 2 && args[0].to_lowercase() == "listening-port" {
                        let listening_port =
                            u16::from_str_radix(&args[1], 10).expect("convert-port");

                        let ReplicationRole::Writer(ref mut writer) =
                            *self.replication_role.write().await
                        else {
                            unreachable!()
                        };

                        let client_info = writer
                            .clients
                            .entry(request_count)
                            .or_insert(ClientInfo::new());
                        client_info.port = Some(listening_port);

                        Ok(vec![RespValue::SimpleString("OK".into())])
                    } else if args.len() == 2 && args[0].to_lowercase() == "capa" {
                        let capa = ClientCapability::from_str(&args[1])
                            .ok_or("ERR invalid client capability".to_string())?;

                        let ReplicationRole::Writer(ref mut writer) =
                            *self.replication_role.write().await
                        else {
                            unreachable!()
                        };

                        let client_info = writer
                            .clients
                            .entry(request_count)
                            .or_insert(ClientInfo::new());
                        client_info.capabilities.insert(capa);

                        Ok(vec![RespValue::SimpleString("OK".into())])
                    } else {
                        Ok(vec![RespValue::SimpleError(
                            "ERR unrecognized argument for 'replconf' command".into(),
                        )])
                    }
                } else {
                    Ok(vec![RespValue::SimpleError(
                        "ERR writer commands on a non-writer node".into(),
                    )])
                }
            }

            Command::Psync2(_replication_id, offset) => {
                if self.replication_role.read().await.is_writer() {
                    let ReplicationRole::Writer(ref mut writer) =
                        *self.replication_role.write().await
                    else {
                        unreachable!()
                    };

                    let client_info = writer
                        .clients
                        .entry(request_count)
                        .or_insert(ClientInfo::new());

                    client_info.current_offset = *offset;

                    let fake_rdb_file_bytes_str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
                    let fake_rdb_file_bytes = (0..fake_rdb_file_bytes_str.len() / 2)
                        .into_iter()
                        .map(|i| {
                            u8::from_str_radix(&fake_rdb_file_bytes_str[i..=i + 1], 16).unwrap()
                        })
                        .collect::<Vec<_>>();

                    Ok(vec![
                        RespValue::SimpleString(format!("FULLRESYNC {} 0", writer.replid)),
                        RespValue::BulkBytes(fake_rdb_file_bytes),
                    ])
                } else {
                    Ok(vec![RespValue::SimpleError(
                        "ERR writer commands on a non-writer node".into(),
                    )])
                }
            }

            Command::Unknown(msg) => Ok(vec![RespValue::SimpleError(format!(
                "Unrecognized command: {}",
                msg
            ))]),
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
    ) -> Result<Vec<RespValue>, Error> {
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
                Ok(vec![RespValue::Integer(count as i64)])
            }
            Err(err) => Ok(vec![RespValue::SimpleError(err)]),
        }
    }

    async fn pop(&self, key: &String, dir: ArrayDirection) -> Result<Vec<RespValue>, Error> {
        let result = match dir {
            ArrayDirection::Back => self.db.write().await.list_pop_one_back(key),
            ArrayDirection::Front => self.db.write().await.list_pop_one_front(key),
        };
        match result {
            Ok(Some(v)) => return Ok(vec![RespValue::BulkString(v)]),
            Ok(None) => return Ok(vec![RespValue::NullBulkString]),
            Err(err) => Ok(vec![RespValue::SimpleError(err)]),
        }
    }

    async fn pop_multi(
        &self,
        key: &String,
        n: &usize,
        dir: ArrayDirection,
    ) -> Result<Vec<RespValue>, Error> {
        let result = match dir {
            ArrayDirection::Back => self.db.write().await.list_pop_multi_back(key, *n),
            ArrayDirection::Front => self.db.write().await.list_pop_multi_front(key, *n),
        };
        match result {
            Ok(Some(elems)) => Ok(vec![RespValue::Array(
                elems
                    .into_iter()
                    .map(|e| RespValue::BulkString(e))
                    .collect(),
            )]),
            Ok(None) => return Ok(vec![RespValue::NullBulkString]),
            Err(err) => Ok(vec![RespValue::SimpleError(err)]),
        }
    }

    async fn blocking_pop(
        &self,
        keys: &Vec<String>,
        timeout_secs: &f64,
        dir: ArrayDirection,
    ) -> Result<Vec<RespValue>, Error> {
        let now_secs = current_time_secs_f64();
        let end_secs = now_secs + timeout_secs;

        loop {
            for key in keys {
                let result = match dir {
                    ArrayDirection::Back => self.db.write().await.list_pop_one_back(key)?,
                    ArrayDirection::Front => self.db.write().await.list_pop_one_front(key)?,
                };
                if let Some(v) = result {
                    return Ok(vec![RespValue::Array(vec![
                        RespValue::BulkString(key.clone()),
                        RespValue::BulkString(v),
                    ])]);
                }
            }

            let ttl = end_secs - current_time_secs_f64();
            if ttl <= 0.0 {
                return Ok(vec![RespValue::NullArray]);
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

    async fn section_info(&self, section: &str) -> String {
        match section {
            "replication" => match *self.replication_role.read().await {
                ReplicationRole::Writer(ref role) => format!("# Replication\r\nrole:master\r\nmaster_replid:{}\r\nmaster_repl_offset:{}\r\n\r\n", role.replid, role.offset),
                ReplicationRole::Reader(ref _role) => "# Replication\r\nrole:slave\r\n\r\n".to_string(),
            },
            _ => String::new(),
        }
    }

    async fn handshake_step(
        stream: &mut TcpStream,
        payload: RespValue,
        expected_response: RespValue,
    ) -> Result<(), Error> {
        stream
            .write_all(&payload.serialize())
            .await
            .context("responding-to-writer")?;

        let response = read_resp_value_from_tcp_stream(stream).await?;
        debug!("Handshake response: {:?}", response);

        if response != Some(expected_response) {
            return Err("Unexpected response to handshake".into());
        };

        Ok(())
    }
}
