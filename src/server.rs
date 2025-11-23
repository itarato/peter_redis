use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

use anyhow::Context;

use crate::{
    command_parser::CommandParser,
    commands::Command,
    common::{read_from_tcp_stream, Error},
};

pub(crate) struct Server {
    command_parser: CommandParser,
}

impl Server {
    pub(crate) fn new() -> Self {
        Self {
            command_parser: CommandParser::new(),
        }
    }

    pub(crate) fn run(&self) -> Result<(), Error> {
        let listener = TcpListener::bind("127.0.0.1:6379").context("tcp-bind")?;

        for stream in listener.incoming() {
            let mut stream = stream.context("incoming-stream")?;
            self.handle_request(&mut stream)?;
        }

        Ok(())
    }

    fn handle_request(&self, stream: &mut TcpStream) -> Result<(), Error> {
        let commands = read_from_tcp_stream(stream)?;

        for command in commands {
            match self.command_parser.parse(&command) {
                Some(Command::Ping) => {
                    stream
                        .write_all(b"+PONG\r\n")
                        .context("write-back-to-stream-at-ping")?;
                }
                None => error!("Unknown command: {}", command),
            }
        }

        Ok(())
    }
}
