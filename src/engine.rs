use crate::{commands::Command, common::Error, resp::RespValue};

pub(crate) struct Engine {}

impl Engine {
    pub(crate) fn new() -> Self {
        Self {}
    }

    pub(crate) fn execute(&mut self, command: &Command) -> Result<RespValue, Error> {
        match command {
            Command::Ping => Ok(RespValue::SimpleString("PONG".to_string())),
            Command::Echo(arg) => Ok(arg.clone()),
        }
    }
}
