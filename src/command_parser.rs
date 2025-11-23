use std::borrow::Cow;

use crate::commands::Command;

pub(crate) struct CommandParser {}

impl CommandParser {
    pub(crate) fn new() -> Self {
        CommandParser {}
    }

    pub(crate) fn parse(&self, raw: &str) -> Option<Command> {
        if raw == "PING" {
            Some(Command::Ping)
        } else {
            None
        }
    }
}
