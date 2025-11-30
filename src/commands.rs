use crate::common::{CompleteStreamEntryIDOrLatest, KeyValuePair, StreamEntryID};

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
    Lpop(String),
    Rpop(String),
    Lpopn(String, usize),
    Rpopn(String, usize),
    Blpop(Vec<String>, f64),
    Brpop(Vec<String>, f64),
    Type(String),
    Xadd(String, StreamEntryID, Vec<KeyValuePair>),
    Xrange(
        String,
        CompleteStreamEntryIDOrLatest,
        CompleteStreamEntryIDOrLatest,
        usize,
    ),
    Xread(
        Vec<(String, CompleteStreamEntryIDOrLatest)>,
        usize,
        Option<u128>,
    ),
}
