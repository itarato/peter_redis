use crate::commands::Command;

pub(crate) struct CommandParser {}

impl CommandParser {
    pub(crate) fn parse(raw: &str) -> Option<Command> {
        if raw == "PING" {
            Some(Command::Ping)
        } else {
            None
        }
    }
}
