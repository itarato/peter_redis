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
                    } else if name.to_lowercase() == "echo" && items.len() == 2 {
                        return Some(Command::Echo(items[1].clone()));
                    }
                }
            }
            _ => {}
        }

        None
    }
}
