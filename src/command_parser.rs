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

                    if name.to_lowercase() == "set" && items.len() == 3 {
                        if let Some(key) = items[1].as_string() {
                            return Some(Command::Set(key.clone(), items[2].clone()));
                        }
                    }
                }
            }
            _ => {}
        }

        None
    }
}
