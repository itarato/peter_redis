use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::u128;

use anyhow::Context;
use rand::rng;
use rand::RngCore;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;
use tokio::io::BufReader;
use tokio::net::TcpStream;

use crate::commands::Command;
use crate::resp::RespValue;

pub(crate) type Error = Box<dyn std::error::Error + Send + Sync>;

pub(crate) struct ReaderRole {
    pub(crate) writer_host: String,
    pub(crate) writer_port: u16,
}

#[derive(Hash, PartialEq, Eq)]
pub(crate) enum ClientCapability {
    Psync2,
}

impl ClientCapability {
    pub(crate) fn from_str(raw: &str) -> Option<Self> {
        match raw.to_lowercase().as_str() {
            "psync2" => Some(ClientCapability::Psync2),
            _ => None,
        }
    }
}

pub(crate) struct ClientInfo {
    pub(crate) port: Option<u16>,
    pub(crate) capabilities: HashSet<ClientCapability>,
    pub(crate) current_offset: i64,
}

impl ClientInfo {
    pub(crate) fn new() -> Self {
        Self {
            port: None,
            capabilities: HashSet::new(),
            current_offset: -1,
        }
    }
}

pub(crate) struct WriterRole {
    pub(crate) replid: String,
    pub(crate) offset: u64,
    //                          vvv--request-count
    pub(crate) clients: HashMap<u64, ClientInfo>,
    pub(crate) write_queue: VecDeque<Command>,
}

impl WriterRole {
    pub(crate) fn push_write_command(&mut self, command: Command) {
        self.write_queue.push_back(command);
    }

    pub(crate) fn pop_write_command(&mut self, request_count: u64) -> Vec<Command> {
        let client_info = self
            .clients
            .get_mut(&request_count)
            .expect("loading client info");

        let last_read_index = client_info.current_offset;
        let latest_readable_index = self.write_queue.len() as i64 - 1;

        if latest_readable_index == last_read_index {
            return vec![];
        }
        if latest_readable_index < last_read_index {
            panic!("Latest index is greater than last index");
        }

        let mut out = vec![];
        for i in (last_read_index + 1)..=latest_readable_index {
            out.push(self.write_queue[i as usize].clone());
        }

        client_info.current_offset = latest_readable_index;

        out
    }
}

pub(crate) enum ReplicationRole {
    Reader(ReaderRole),
    Writer(WriterRole),
}

impl ReplicationRole {
    pub(crate) fn is_writer(&self) -> bool {
        match self {
            ReplicationRole::Writer(_) => true,
            _ => false,
        }
    }

    pub(crate) fn is_reader(&self) -> bool {
        match self {
            ReplicationRole::Reader(_) => true,
            _ => false,
        }
    }

    pub(crate) fn writer_mut(&mut self) -> &mut WriterRole {
        match self {
            ReplicationRole::Writer(writer) => writer,
            _ => panic!("Caller must ensure role is writer"),
        }
    }
}

pub(crate) type KeyValuePair = (String, String);

#[derive(Debug, Clone, PartialEq, Eq, Ord)]
pub(crate) struct CompleteStreamEntryID(pub(crate) u128, pub(crate) usize);

#[derive(Debug, Clone)]
pub(crate) enum RangeStreamEntryID {
    Fixed(CompleteStreamEntryID),
    Latest,
}

impl CompleteStreamEntryID {
    pub(crate) fn to_string(&self) -> String {
        format!("{}-{}", self.0, self.1)
    }

    pub(crate) fn max() -> Self {
        CompleteStreamEntryID(u128::MAX, usize::MAX)
    }
}

impl PartialOrd for CompleteStreamEntryID {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.0.cmp(&other.0) {
            std::cmp::Ordering::Greater | std::cmp::Ordering::Less => self.0.partial_cmp(&other.0),
            std::cmp::Ordering::Equal => self.1.partial_cmp(&other.1),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum StreamEntryID {
    Wildcard,
    MsOnly(u128),
    Full(CompleteStreamEntryID),
}

impl StreamEntryID {
    pub(crate) fn to_resp_string(&self) -> String {
        match self {
            StreamEntryID::Wildcard => "*".to_string(),
            StreamEntryID::MsOnly(ms) => ms.to_string(),
            StreamEntryID::Full(id) => format!("{}-{}", id.0, id.1),
        }
    }
}

pub(crate) fn current_time_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before UNIX EPOCH")
        .as_millis()
}

pub(crate) fn current_time_secs_f64() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before UNIX EPOCH")
        .as_secs_f64()
}

pub(crate) fn new_master_replid() -> String {
    let mut rnd = rng();
    let mut bytes: [u8; 20] = [0; 20];
    rnd.fill_bytes(&mut bytes);

    bytes.map(|b| format!("{:x}", b)).join("")
}

pub(crate) async fn read_resp_value_from_tcp_stream(
    stream: &mut TcpStream,
    request_count: Option<u64>,
) -> Result<Option<RespValue>, Error> {
    let mut buf_reader = BufReader::new(stream);
    read_resp_value(&mut buf_reader, request_count).await
}

async fn read_resp_value(
    buf_reader: &mut BufReader<&mut TcpStream>,
    request_count: Option<u64>,
) -> Result<Option<RespValue>, Error> {
    let line = read_line_from_tcp_stream(buf_reader, request_count).await?;

    if line.is_empty() {
        return Ok(None);
    } else if line.starts_with("+") {
        return Ok(Some(RespValue::SimpleString(line[1..].trim().to_string())));
    } else if line.starts_with("$") {
        let bulk_str_len =
            usize::from_str_radix(&line[1..].trim(), 10).context("parse-bulk-str-len")?;

        let next_line = read_line_from_tcp_stream(buf_reader, request_count).await?;

        if next_line.trim().len() != bulk_str_len {
            return Err(format!(
                "Bulk string len mismatch. Expected {}, got {}. Bulk string: {}",
                bulk_str_len,
                next_line.len(),
                &next_line
            )
            .into());
        }

        return Ok(Some(RespValue::BulkString(next_line.trim().to_string())));
    } else if line.starts_with("*") {
        let array_len = usize::from_str_radix(&line[1..].trim(), 10).context("parse-array-len")?;
        let mut items = vec![];

        for _ in 0..array_len {
            match Box::pin(read_resp_value(buf_reader, request_count)).await? {
                Some(item) => items.push(item),
                None => return Err("Missing array item".into()),
            }
        }

        return Ok(Some(RespValue::Array(items)));
    } else if line.starts_with(":") {
        let v = i64::from_str_radix(&line[1..].trim(), 10).context("parse-array-len")?;
        return Ok(Some(RespValue::Integer(v)));
    }

    Err(format!("Unexpected incoming RESP string from connection: {}", line).into())
}

async fn read_line_from_tcp_stream(
    buf_reader: &mut BufReader<&mut TcpStream>,
    request_count: Option<u64>,
) -> Result<String, Error> {
    let mut buf = String::new();
    buf_reader
        .read_line(&mut buf)
        .await
        .context("number-read")?;

    debug!(
        "Incoming {} bytes from TcpStream [{}] (RC: {:?})",
        buf.len(),
        buf.trim_end(),
        request_count
    );

    Ok(buf)
}

pub(crate) async fn read_bulk_bytes_from_tcp_stream(
    stream: &mut TcpStream,
    request_count: Option<u64>,
) -> Result<Vec<u8>, Error> {
    let mut buf_reader = BufReader::new(stream);

    let mut size_raw = String::new();
    buf_reader
        .read_line(&mut size_raw)
        .await
        .context("read-bulk-bytes-size")?;

    let size_raw = size_raw.trim_end();

    if !size_raw.starts_with("$") {
        let next = read_line_from_tcp_stream(&mut buf_reader, request_count).await?;
        return Err(format!(
            "Invalid start of size for bulk bytes. Got: {} (Next: {}) (RC: {:?})",
            size_raw, next, request_count
        )
        .into());
    }

    let size = usize::from_str_radix(&size_raw[1..], 10).context("convert-size-to-usize")?;

    let mut buf = Vec::with_capacity(size);
    buf.resize(size, 0);
    let read_size = buf_reader
        .read_exact(&mut buf[0..size])
        .await
        .context("number-read")?;

    if read_size != size {
        return Err(format!("Read size mismatch. Read {}. Expected {}.", read_size, size).into());
    }

    debug!(
        "Incoming {} bytes from TcpStream (RC: {:?})",
        buf.len(),
        request_count
    );

    Ok(buf)
}
