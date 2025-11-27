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

            Command::Echo(arg) => Ok(RespValue::BulkString(arg.clone())),

            Command::Set(key, value, expiry) => {
                match self.db.set(key.clone(), value.clone(), expiry.clone()) {
                    Ok(_) => Ok(RespValue::SimpleString("OK".into())),
                    Err(err) => Ok(RespValue::SimpleError(err)),
                }
            }

            Command::Get(key) => match self.db.get(key) {
                Ok(Some(v)) => Ok(RespValue::BulkString(v.clone())),
                Ok(None) => Ok(RespValue::Null),
                Err(err) => Ok(RespValue::SimpleError(err)),
            },

            Command::Rpush(key, values) => {
                match self.db.push_to_array(key.clone(), values.clone()) {
                    Ok(count) => Ok(RespValue::Integer(count as i64)),
                    Err(err) => Ok(RespValue::SimpleError(err)),
                }
            }

            Command::Lpush(key, values) => {
                match self.db.insert_to_array(key.clone(), values.clone()) {
                    Ok(count) => Ok(RespValue::Integer(count as i64)),
                    Err(err) => Ok(RespValue::SimpleError(err)),
                }
            }

            Command::Lrange(key, start, end) => match self.db.get_list_lrange(key, *start, *end) {
                Ok(array) => Ok(RespValue::Array(
                    array
                        .into_iter()
                        .map(|elem| RespValue::BulkString(elem))
                        .collect::<Vec<_>>(),
                )),
                Err(err) => Ok(RespValue::SimpleError(err)),
            },

            Command::Llen(key) => match self.db.list_length(key) {
                Ok(n) => Ok(RespValue::Integer(n as i64)),
                Err(err) => Ok(RespValue::SimpleError(err)),
            },

            Command::Lpop(key) => match self.db.list_pop_one_front(key) {
                Ok(Some(v)) => return Ok(RespValue::BulkString(v)),
                Ok(None) => return Ok(RespValue::Null),
                Err(err) => Ok(RespValue::SimpleError(err)),
            },

            Command::Rpop(key) => match self.db.list_pop_one_back(key) {
                Ok(Some(v)) => return Ok(RespValue::BulkString(v)),
                Ok(None) => return Ok(RespValue::Null),
                Err(err) => Ok(RespValue::SimpleError(err)),
            },

            Command::Lpopn(key, n) => match self.db.list_pop_multi_front(key, *n) {
                Ok(Some(elems)) => Ok(RespValue::Array(
                    elems
                        .into_iter()
                        .map(|e| RespValue::BulkString(e))
                        .collect(),
                )),
                Ok(None) => return Ok(RespValue::Null),
                Err(err) => Ok(RespValue::SimpleError(err)),
            },

            Command::Rpopn(key, n) => match self.db.list_pop_multi_back(key, *n) {
                Ok(Some(elems)) => Ok(RespValue::Array(
                    elems
                        .into_iter()
                        .map(|e| RespValue::BulkString(e))
                        .collect(),
                )),
                Ok(None) => return Ok(RespValue::Null),
                Err(err) => Ok(RespValue::SimpleError(err)),
            },
        }
    }
}
