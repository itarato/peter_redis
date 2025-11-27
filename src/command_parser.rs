use crate::{commands::Command, resp::RespValue};

pub(crate) struct CommandParser;

impl CommandParser {
    pub(crate) fn parse(input: &RespValue) -> Result<Command, String> {
        match input {
            RespValue::Array(items) => {
                if items.is_empty() {
                    return Err("ERR missing command".into());
                }

                if let Some(name) = items[0].as_string() {
                    if name.to_lowercase() == "ping" {
                        if items.len() != 1 {
                            return Err("ERR wrong number of arguments for 'ping' command".into());
                        }
                        return Ok(Command::Ping);
                    }

                    if name.to_lowercase() == "echo" {
                        if items.len() != 2 {
                            return Err("ERR wrong number of arguments for 'echo' command".into());
                        }
                        return Ok(Command::Echo(items[1].clone()));
                    }

                    if name.to_lowercase() == "get" {
                        if items.len() != 2 {
                            return Err("ERR wrong number of arguments for 'get' command".into());
                        }
                        if let Some(key) = items[1].as_string() {
                            return Ok(Command::Get(key.clone()));
                        }

                        return Err("ERR wrong key for 'get' command".into());
                    }

                    if name.to_lowercase() == "set" {
                        let Some(key) = items[1].as_string() else {
                            return Err("ERR wrong number of arguments for 'set' command".into());
                        };
                        let Some(value) = items[2].as_string() else {
                            return Err("ERR wrong value for 'set' command".into());
                        };

                        if items.len() == 3 {
                            return Ok(Command::Set(key.clone(), value.clone(), None));
                        } else if items.len() == 5 {
                            let Some(expiry_kind) = items[3].as_string() else {
                                return Err("ERR wrong expiry type for 'set' command".into());
                            };
                            let Some(expiry_value_str) = items[4].as_string() else {
                                return Err("ERR wrong expiry value for 'set' command".into());
                            };
                            let Ok(expiry_value) = u128::from_str_radix(&expiry_value_str, 10)
                            else {
                                return Err("ERR wrong expiry value for 'set' command".into());
                            };
                            let expiry_ms = if expiry_kind.to_lowercase() == "ex" {
                                expiry_value * 1_000
                            } else if expiry_kind.to_lowercase() == "px" {
                                expiry_value
                            } else {
                                return Err("ERR wrong expiry type for 'set' command".into());
                            };

                            return Ok(Command::Set(key.clone(), value.clone(), Some(expiry_ms)));
                        } else {
                            return Err("ERR wrong number of arguments for 'set' command".into());
                        }
                    }

                    if name.to_lowercase() == "rpush" {
                        if items.len() <= 2 {
                            return Err("ERR wrong number of arguments for 'rpush' command".into());
                        }
                        let Some(key) = items[1].as_string() else {
                            return Err("ERR wrong key for 'rpush' command".into());
                        };

                        let mut values = vec![];
                        for i in 2..items.len() {
                            let Some(value) = items[i].as_string() else {
                                return Err("ERR wrong value for 'rpush' command".into());
                            };
                            values.push(value.clone());
                        }
                        return Ok(Command::Rpush(key.clone(), values));
                    }

                    if name.to_lowercase() == "lpush" {
                        if items.len() <= 2 {
                            return Err("ERR wrong number of arguments for 'lpush' command".into());
                        }
                        let Some(key) = items[1].as_string() else {
                            return Err("ERR wrong key for 'lpush' command".into());
                        };

                        let mut values = vec![];
                        for i in 2..items.len() {
                            let Some(value) = items[i].as_string() else {
                                return Err("ERR wrong value for 'lpush' command".into());
                            };
                            values.push(value.clone());
                        }
                        return Ok(Command::Lpush(key.clone(), values));
                    }

                    if name.to_lowercase() == "lrange" {
                        if items.len() != 4 {
                            return Err("ERR wrong number of arguments for 'lrange' command".into());
                        }
                        let Some(key) = items[1].as_string() else {
                            return Err("ERR wrong key for 'lrange' command".into());
                        };
                        let Some(start_raw) = items[2].as_string() else {
                            return Err("ERR wrong left range for 'lrange' command".into());
                        };
                        let Some(end_raw) = items[3].as_string() else {
                            return Err("ERR wrong right range for 'lrange' command".into());
                        };
                        let Ok(start) = i64::from_str_radix(&start_raw, 10) else {
                            return Err("ERR wrong left range value for 'lrange' command".into());
                        };
                        let Ok(end) = i64::from_str_radix(&end_raw, 10) else {
                            return Err("ERR wrong right range value for 'lrange' command".into());
                        };

                        return Ok(Command::LRange(key.clone(), start, end));
                    }

                    return Err(format!("ERR unknown command '{}'", name.to_lowercase()));
                } else {
                    return Err("ERR wrong command type".into());
                }
            }
            _ => {}
        }

        Err("ERR unknown command 'asdf'".into())
    }
}
