use crate::resp::RespValue;

pub(crate) enum Command {
    Ping,
    Echo(RespValue),
    Set(String, RespValue),
    Get(String),
}
