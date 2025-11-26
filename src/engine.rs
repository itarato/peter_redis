use crate::{commands::Command, common::Error, database::Database, resp::RespValue};

pub(crate) struct Engine {
    db: Database,
}

impl Engine {
    pub(crate) fn new() -> Self {
        Self {
            db: Database::new(),
        }
    }

    pub(crate) fn execute(&mut self, command: &Command) -> Result<RespValue, Error> {
        match command {
            Command::Ping => Ok(RespValue::SimpleString("PONG".to_string())),
            Command::Echo(arg) => Ok(arg.clone()),
            Command::Set(key, value, expiry) => {
                self.db.set(key.clone(), value.clone(), expiry.clone());
                Ok(RespValue::SimpleString("OK".into()))
            }
            Command::Get(key) => match self.db.get(key) {
                Some(v) => Ok(v.clone()),
                None => Ok(RespValue::Null),
            },
        }
    }
}
