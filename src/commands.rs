use crate::resp::RespValue;

#[derive(Debug)]
pub(crate) enum Command {
    Ping,
    Echo(RespValue),
    Set(String, String, Option<u128>),
    Get(String),
    Rpush(String, Vec<String>),
    Lpush(String, Vec<String>),
    LRange(String, i64, i64),
}
