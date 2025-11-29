use anyhow::Context;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::net::TcpStream;

use crate::resp::RespValue;

pub(crate) type Error = Box<dyn std::error::Error + Send + Sync>;

pub(crate) type KeyValuePair = (String, String);

#[derive(Debug, Clone)]
pub(crate) struct CompleteStreamEntryID(pub(crate) u128, pub(crate) usize);

impl CompleteStreamEntryID {
    pub(crate) fn to_string(&self) -> String {
        format!("{}-{}", self.0, self.1)
    }
}

#[derive(Debug, Clone)]
pub(crate) enum StreamEntryID {
    Wildcard,
    MsOnly(u128),
    Full(CompleteStreamEntryID),
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

pub(crate) async fn read_from_tcp_stream(
    stream: &mut TcpStream,
) -> Result<Option<RespValue>, Error> {
    let mut buf_reader = BufReader::new(stream);
    read_resp_value(&mut buf_reader).await
}

async fn read_resp_value(
    buf_reader: &mut BufReader<&mut TcpStream>,
) -> Result<Option<RespValue>, Error> {
    let line = read_line_from_tcp_stream(buf_reader).await?;

    if line.is_empty() {
        return Ok(None);
    } else if line.starts_with("+") {
        return Ok(Some(RespValue::SimpleString(line[1..].trim().to_string())));
    } else if line.starts_with("$") {
        let bulk_str_len =
            usize::from_str_radix(&line[1..].trim(), 10).context("parse-bulk-str-len")?;

        let next_line = read_line_from_tcp_stream(buf_reader).await?;

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
            match Box::pin(read_resp_value(buf_reader)).await? {
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
) -> Result<String, Error> {
    let mut buf = String::new();
    buf_reader
        .read_line(&mut buf)
        .await
        .context("number-read")?;
    Ok(buf)
}
