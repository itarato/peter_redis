use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{TcpSocket, TcpStream},
    sync::{Mutex, Notify, RwLock},
};

use crate::{
    command_parser::CommandParser,
    commands::Command,
    common::{
        current_time_ms, current_time_secs_f64, new_master_replid, read_bulk_bytes_from_tcp_stream,
        read_resp_value_from_buf_reader, ClientCapability, ClientInfo, Error, RangeStreamEntryID,
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
    write_queue_notification: Notify,
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
                write_queue: VecDeque::new(),
            }),
        };

        Self {
            db: RwLock::new(Database::new()),
            notification: Arc::new(Notify::new()),
            transaction_store: Mutex::new(HashMap::new()),
            replication_role: RwLock::new(replication_role),
            write_queue_notification: Notify::new(),
        }
    }

    pub(crate) async fn init(&self, server_port: u16) -> Result<(), Error> {
        if self.replication_role.read().await.is_writer() {
            Ok(())
        } else if self.replication_role.read().await.is_reader() {
            self.read_replication_handle(server_port).await
        } else {
            unreachable!()
        }
    }

    async fn read_replication_handle(&self, server_port: u16) -> Result<(), Error> {
        let (writer_host, writer_port) = {
            let ReplicationRole::Reader(ref reader) = *self.replication_role.read().await else {
                unreachable!();
            };

            (reader.writer_host.clone(), reader.writer_port)
        };

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
        let mut buf_reader = BufReader::new(&mut stream);

        self.handshake(server_port, &mut buf_reader).await?;

        self.listen_for_replication_updates(&mut buf_reader).await?;

        Ok(())
    }

    async fn listen_for_replication_updates(
        &self,
        buf_reader: &mut BufReader<&mut TcpStream>,
    ) -> Result<(), Error> {
        loop {
            debug!("Start waiting for replication input");
            match read_resp_value_from_buf_reader(buf_reader, None).await? {
                Some(value) => {
                    let command = CommandParser::parse(value)?;
                    debug!("Reader replicates command: {:?}", &command);
                    self.execute_only(&command, None).await?;
                }
                None => {
                    debug!("Reader listening has ended due to stream closing");
                    return Ok(());
                }
            }
        }
    }

    async fn handshake(
        &self,
        server_port: u16,
        buf_reader: &mut BufReader<&mut TcpStream>,
    ) -> Result<(), Error> {
        Self::handshake_step(
            buf_reader,
            RespValue::Array(vec![RespValue::BulkString("PING".into())]),
            RespValue::SimpleString("PONG".to_string()),
        )
        .await?;

        Self::handshake_step(
            buf_reader,
            RespValue::Array(vec![
                RespValue::BulkString("REPLCONF".into()),
                RespValue::BulkString("listening-port".into()),
                RespValue::BulkString(format!("{}", server_port)),
            ]),
            RespValue::SimpleString("OK".to_string()),
        )
        .await?;

        Self::handshake_step(
            buf_reader,
            RespValue::Array(vec![
                RespValue::BulkString("REPLCONF".into()),
                RespValue::BulkString("capa".into()),
                RespValue::BulkString("psync2".into()),
            ]),
            RespValue::SimpleString("OK".to_string()),
        )
        .await?;

        buf_reader
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

        let response = read_resp_value_from_buf_reader(buf_reader, None).await?;
        debug!("Handshake response: {:?}", response);

        let response = read_bulk_bytes_from_tcp_stream(buf_reader, None).await?;
        debug!("Handshake final response: {} bytes", response.len());

        // TODO: replace DB to `response`

        Ok(())
    }

    pub(crate) async fn execute(
        &self,
        command: &Command,
        request_count: u64,
        stream: &mut TcpStream,
    ) -> Result<(), Error> {
        if !command.is_exec() && !command.is_discard() && self.is_transaction(request_count).await {
            if command.is_multi() {
                stream
                    .write_all(
                        &RespValue::SimpleString("ERR MULTI calls can not be nested".to_string())
                            .serialize(),
                    )
                    .await
                    .context("write-simple-value-back-to-stream")?;
            } else {
                {
                    let mut transaction_store = self.transaction_store.lock().await;
                    let transactions = transaction_store.get_mut(&request_count).unwrap();
                    transactions.push(command.clone());
                }

                stream
                    .write_all(&RespValue::SimpleString("QUEUED".to_string()).serialize())
                    .await
                    .context("write-simple-value-back-to-stream")?;
            }
        } else if command.is_psync() {
            self.psync(stream, request_count, command).await?;
        } else {
            self.execute_and_reply(command, request_count, stream)
                .await?;
        }

        Ok(())
    }

    async fn execute_and_reply(
        &self,
        command: &Command,
        request_count: u64,
        stream: &mut TcpStream,
    ) -> Result<(), Error> {
        let response_value = self.execute_only(command, Some(request_count)).await?;

        stream
            .write_all(&response_value.serialize())
            .await
            .context("write-simple-value-back-to-stream")?;

        Ok(())
    }

    async fn execute_only(
        &self,
        command: &Command,
        request_count: Option<u64>,
    ) -> Result<RespValue, Error> {
        let value = match command {
            Command::Ping => RespValue::SimpleString("PONG".to_string()),

            Command::Echo(arg) => RespValue::BulkString(arg.clone()),

            Command::Set(key, value, expiry) => {
                match self
                    .db
                    .write()
                    .await
                    .set(key.clone(), value.clone(), expiry.clone())
                {
                    Ok(_) => RespValue::SimpleString("OK".into()),
                    Err(err) => RespValue::SimpleError(err),
                }
            }

            Command::Get(key) => match self.db.read().await.get(key) {
                Ok(Some(v)) => RespValue::BulkString(v.clone()),
                Ok(None) => RespValue::NullBulkString,
                Err(err) => RespValue::SimpleError(err),
            },

            Command::Lrange(key, start, end) => {
                match self.db.read().await.get_list_lrange(key, *start, *end) {
                    Ok(array) => RespValue::Array(
                        array
                            .into_iter()
                            .map(|elem| RespValue::BulkString(elem))
                            .collect::<Vec<_>>(),
                    ),
                    Err(err) => RespValue::SimpleError(err),
                }
            }

            Command::Llen(key) => match self.db.read().await.list_length(key) {
                Ok(n) => RespValue::Integer(n as i64),
                Err(err) => RespValue::SimpleError(err),
            },

            Command::Rpush(key, values) => self.push(key, values, ArrayDirection::Back).await?,

            Command::Lpush(key, values) => self.push(key, values, ArrayDirection::Front).await?,

            Command::Lpop(key) => self.pop(key, ArrayDirection::Front).await?,

            Command::Rpop(key) => self.pop(key, ArrayDirection::Back).await?,

            Command::Lpopn(key, n) => self.pop_multi(key, n, ArrayDirection::Front).await?,

            Command::Rpopn(key, n) => self.pop_multi(key, n, ArrayDirection::Back).await?,

            Command::Blpop(keys, timeout_secs) => {
                self.blocking_pop(keys, timeout_secs, ArrayDirection::Front)
                    .await?
            }

            Command::Brpop(keys, timeout_secs) => {
                self.blocking_pop(keys, timeout_secs, ArrayDirection::Back)
                    .await?
            }

            Command::Type(key) => {
                RespValue::SimpleString(self.db.read().await.get_key_type_name(key).to_string())
            }

            Command::Xadd(key, id, entries) => {
                match self
                    .db
                    .write()
                    .await
                    .stream_push(key.clone(), id.clone(), entries.clone())
                {
                    Ok(final_id) => {
                        self.notification.notify_one();
                        RespValue::BulkString(final_id.to_string())
                    }
                    Err(err) => RespValue::SimpleError(err),
                }
            }

            Command::Xrange(key, start, end, count) => {
                if *count == 0 {
                    RespValue::NullBulkString
                } else {
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
                        Ok(stream_entry) => Self::stream_to_resp(stream_entry),
                        Err(err) => RespValue::SimpleError(err),
                    }
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
                                break RespValue::Array(
                                    result
                                        .into_iter()
                                        .map(|(key, stream_entry)| {
                                            RespValue::Array(vec![
                                                RespValue::BulkString(key),
                                                Self::stream_to_resp(stream_entry),
                                            ])
                                        })
                                        .collect::<Vec<_>>(),
                                );
                            }
                        }
                        Err(err) => break RespValue::SimpleError(err),
                    }

                    let now_ms = current_time_ms();
                    if end_ms <= now_ms {
                        break RespValue::NullArray;
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
                Ok(n) => RespValue::Integer(n),
                Err(err) => RespValue::SimpleError(err),
            },

            Command::Multi => {
                self.transaction_store
                    .lock()
                    .await
                    .insert(request_count.unwrap(), vec![]);
                RespValue::SimpleString("OK".to_string())
            }

            Command::Exec => {
                let mut transaction_store = self.transaction_store.lock().await;

                match transaction_store.remove(&request_count.unwrap()) {
                    Some(commands) => {
                        let mut subvalues = vec![];
                        for command in commands {
                            let subvalue =
                                Box::pin(self.execute_only(&command, request_count)).await?;

                            subvalues.push(subvalue);
                        }

                        RespValue::Array(subvalues)
                    }
                    None => RespValue::SimpleError("ERR EXEC without MULTI".to_string()),
                }
            }

            Command::Discard => {
                if self.is_transaction(request_count.unwrap()).await {
                    {
                        let mut transaction_store = self.transaction_store.lock().await;
                        transaction_store.remove(&request_count.unwrap());
                    }
                    RespValue::SimpleString("OK".to_string())
                } else {
                    RespValue::SimpleError("ERR DISCARD without MULTI".to_string())
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

                RespValue::BulkString(section_strs)
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
                            .entry(request_count.unwrap())
                            .or_insert(ClientInfo::new());
                        client_info.port = Some(listening_port);

                        RespValue::SimpleString("OK".into())
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
                            .entry(request_count.unwrap())
                            .or_insert(ClientInfo::new());
                        client_info.capabilities.insert(capa);

                        RespValue::SimpleString("OK".into())
                    } else {
                        RespValue::SimpleError(
                            "ERR unrecognized argument for 'replconf' command".into(),
                        )
                    }
                } else {
                    RespValue::SimpleError("ERR writer commands on a non-writer node".into())
                }
            }

            Command::Psync(_replication_id, _offset) => unreachable!(),

            Command::Unknown(msg) => {
                RespValue::SimpleError(format!("Unrecognized command: {}", msg))
            }
        };

        if command.is_write() {
            if self.replication_role.read().await.is_writer() {
                self.replication_role
                    .write()
                    .await
                    .writer_mut()
                    .push_write_command(command.clone());
                self.write_queue_notification.notify_waiters();
            }
        }

        Ok(value)
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
        buf_reader: &mut BufReader<&mut TcpStream>,
        payload: RespValue,
        expected_response: RespValue,
    ) -> Result<(), Error> {
        buf_reader
            .write_all(&payload.serialize())
            .await
            .context("responding-to-writer")?;

        let response = read_resp_value_from_buf_reader(buf_reader, None).await?;
        debug!("Handshake response: {:?}", response);

        if response != Some(expected_response) {
            return Err("Unexpected response to handshake".into());
        };

        Ok(())
    }

    async fn psync(
        &self,
        stream: &mut TcpStream,
        request_count: u64,
        command: &Command,
    ) -> Result<(), Error> {
        let Command::Psync(_replication_id, offset) = command else {
            unreachable!()
        };

        if !self.replication_role.read().await.is_writer() {
            stream
                .write_all(
                    &RespValue::SimpleError("ERR writer commands on a non-writer node".into())
                        .serialize(),
                )
                .await
                .context("write-simple-value-back-to-stream")?;
            return Ok(());
        }

        let writer_replid;

        {
            let ReplicationRole::Writer(ref mut writer) = *self.replication_role.write().await
            else {
                unreachable!()
            };
            writer_replid = writer.replid.clone();

            let client_info = writer
                .clients
                .entry(request_count)
                .or_insert(ClientInfo::new());

            client_info.current_offset = *offset;
        }

        let fake_rdb_file_bytes_str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
        let fake_rdb_file_bytes = (0..fake_rdb_file_bytes_str.len() / 2)
            .into_iter()
            .map(|i| {
                u8::from_str_radix(&fake_rdb_file_bytes_str[(i * 2)..=(i * 2) + 1], 16).unwrap()
            })
            .collect::<Vec<_>>();

        stream
            .write_all(
                &RespValue::SimpleString(format!("FULLRESYNC {} 0", writer_replid)).serialize(),
            )
            .await
            .context("write-simple-value-back-to-stream")?;

        stream
            .write_all(&RespValue::BulkBytes(fake_rdb_file_bytes).serialize())
            .await
            .context("write-simple-value-back-to-stream")?;

        loop {
            let write_commands;
            {
                let mut writer_guard = self.replication_role.write().await;
                let writer = writer_guard.writer_mut();
                write_commands = writer.pop_write_command(request_count);
            }

            if write_commands.is_empty() {
                debug!("Wait for write events to send to readers");
                self.write_queue_notification.notified().await;
            }

            for command in write_commands {
                stream.write_all(&command.into_resp().serialize()).await?;
            }
        }
    }
}
