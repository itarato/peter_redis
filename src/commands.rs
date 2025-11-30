use crate::common::{KeyValuePair, RangeStreamEntryID, StreamEntryID};

#[derive(Debug, Clone)]
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
    Xrange(String, RangeStreamEntryID, RangeStreamEntryID, usize),
    Xread(Vec<(String, RangeStreamEntryID)>, usize, Option<u128>),
    Incr(String),
    Multi,
    Exec,
}

impl Command {
    pub(crate) fn is_exec(&self) -> bool {
        match self {
            Command::Exec => true,
            _ => false,
        }
    }

    pub(crate) fn is_multi(&self) -> bool {
        match self {
            Command::Multi => true,
            _ => false,
        }
    }
}
