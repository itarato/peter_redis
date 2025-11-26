use crate::{commands::Command, resp::RespValue};

pub(crate) struct CommandParser;

impl CommandParser {
    pub(crate) fn parse(input: &RespValue) -> Option<Command> {
        match input {
            RespValue::Array(items) => {
                if items.is_empty() {
                    return None;
                }

                if let Some(name) = items[0].as_string() {
                    if name.to_lowercase() == "ping" && items.len() == 1 {
                        return Some(Command::Ping);
                    }

                    if name.to_lowercase() == "echo" && items.len() == 2 {
                        return Some(Command::Echo(items[1].clone()));
                    }

                    if name.to_lowercase() == "get" && items.len() == 2 {
                        if let Some(key) = items[1].as_string() {
                            return Some(Command::Get(key.clone()));
                        }
                    }

                    if name.to_lowercase() == "set" {
                        let Some(key) = items[1].as_string() else {
                            return None;
                        };

                        if items.len() == 3 {
                            return Some(Command::Set(key.clone(), items[2].clone(), None));
                        }

                        if items.len() == 5 {
                            let Some(expiry_kind) = items[3].as_string() else {
                                return None;
                            };
                            let Some(expiry_value_str) = items[4].as_string() else {
                                return None;
                            };
                            let Ok(expiry_value) = u128::from_str_radix(&expiry_value_str, 10)
                            else {
                                return None;
                            };
                            let expiry_ms = if expiry_kind.to_lowercase() == "ex" {
                                expiry_value * 1_000
                            } else if expiry_kind.to_lowercase() == "px" {
                                expiry_value
                            } else {
                                return None;
                            };

                            return Some(Command::Set(
                                key.clone(),
                                items[2].clone(),
                                Some(expiry_ms),
                            ));
                        }
                    }
                }
            }
            _ => {}
        }

        None
    }
}
