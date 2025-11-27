use std::vec;

use crate::{commands::Command, resp::RespValue};

macro_rules! to_number {
    ($t:ident, $v:expr, $name:literal) => {
        $t::from_str_radix($v, 10)
            .map_err(|_| format!("ERR wrong value for '{}' command", $name))?
    };
}

pub(crate) struct CommandParser;

impl CommandParser {
    pub(crate) fn parse(input: RespValue) -> Result<Command, String> {
        match input {
            RespValue::Array(items) => {
                if items.is_empty() {
                    return Err("ERR missing command".into());
                }

                if let Some(name) = items[0].as_string().cloned() {
                    if name.to_lowercase() == "ping" {
                        if items.len() != 1 {
                            return Err("ERR wrong number of arguments for 'ping' command".into());
                        }
                        return Ok(Command::Ping);
                    }

                    if name.to_lowercase() == "echo" {
                        let mut str_items = Self::get_strings_exact(items, 2, "echo")?;
                        return Ok(Command::Echo(str_items.remove(1)));
                    }

                    if name.to_lowercase() == "get" {
                        let mut str_items = Self::get_strings_exact(items, 2, "get")?;
                        return Ok(Command::Get(str_items.remove(1)));
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
                        let mut str_items = Self::get_strings_exact(items, 4, "lrange")?;
                        let start = to_number!(i64, &str_items[2], "lrange");
                        let end = to_number!(i64, &str_items[3], "lrange");

                        return Ok(Command::Lrange(str_items.remove(1), start, end));
                    }

                    if name.to_lowercase() == "llen" {
                        let mut str_items = Self::get_strings_exact(items, 2, "llen")?;
                        return Ok(Command::Llen(str_items.remove(1)));
                    }

                    if name.to_lowercase() == "lpop" {
                        if items.len() == 2 {
                            let mut str_items = Self::get_strings_exact(items, 2, "lpop")?;
                            return Ok(Command::Lpop(str_items.remove(1)));
                        }
                        if items.len() == 3 {
                            let mut str_items = Self::get_strings_exact(items, 3, "lpop")?;
                            let n = to_number!(usize, &str_items[2], "lpop");
                            return Ok(Command::Lpopn(str_items.remove(1), n));
                        }
                        return Err("ERR wrong number of arguments for 'lpop' command".into());
                    }

                    if name.to_lowercase() == "rpop" {
                        if items.len() == 2 {
                            let mut str_items = Self::get_strings_exact(items, 2, "rpop")?;
                            return Ok(Command::Rpop(str_items.remove(1)));
                        }
                        if items.len() == 3 {
                            let mut str_items = Self::get_strings_exact(items, 3, "rpop")?;
                            let n = to_number!(usize, &str_items[2], "rpop");
                            return Ok(Command::Rpopn(str_items.remove(1), n));
                        }
                        return Err("ERR wrong number of arguments for 'rpop' command".into());
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

    fn get_strings_exact(
        values: Vec<RespValue>,
        n: usize,
        command_name: &str,
    ) -> Result<Vec<String>, String> {
        if values.len() != n {
            return Err(format!(
                "ERR wrong number of arguments for '{}' command",
                command_name
            ));
        }

        let mut out = vec![];
        for value in values {
            let Some(s) = value.as_string_owned() else {
                return Err(format!(
                    "ERR wrong value type for '{}' command",
                    command_name
                ));
            };
            out.push(s);
        }

        Ok(out)
    }
}
