use crate::resp::RespValue;

#[derive(Debug)]
pub(crate) enum Command {
    Ping,
    Echo(RespValue),
    Set(String, RespValue, Option<u128>),
    Get(String),
}
