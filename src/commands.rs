#[derive(Debug)]
pub(crate) enum Command {
    Ping,
    Echo(String),
    Set(String, String, Option<u128>),
    Get(String),
    Rpush(String, Vec<String>),
    Lpush(String, Vec<String>),
    Lrange(String, i64, i64),
    Llen(String),
}
