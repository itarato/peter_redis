use crate::resp::RespValue;

pub(crate) enum Command {
    Ping,
    Echo(RespValue),
}
