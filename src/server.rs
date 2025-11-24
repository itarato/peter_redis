use anyhow::Context;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};

use crate::{
    command_parser::CommandParser,
    commands::Command,
    common::{read_from_tcp_stream, Error},
};

pub(crate) struct Server {}

impl Server {
    pub(crate) fn new() -> Self {
        Self {}
    }

    pub(crate) async fn run(&self) -> Result<(), Error> {
        let listener = TcpListener::bind("127.0.0.1:6379")
            .await
            .context("tcp-bind")?;

        loop {
            let (stream, _) = listener.accept().await.context("accept-tcp-connection")?;
            tokio::spawn(async move {
                match Self::handle_request(stream).await {
                    Ok(_) => debug!("Request completed"),
                    Err(err) => error!("Request has failed with reason: {}", err),
                }
            });
        }
    }

    async fn handle_request(mut stream: TcpStream) -> Result<(), Error> {
        loop {
            let commands = read_from_tcp_stream(&mut stream).await?;
            if commands.is_empty() {
                break;
            }

            for command in commands {
                match CommandParser::parse(&command) {
                    Some(Command::Ping) => {
                        stream
                            .write_all(b"+PONG\r\n")
                            .await
                            .context("write-back-to-stream-at-ping")?;
                    }
                    None => error!("Unknown command: {}", command),
                }
            }
        }

        Ok(())
    }
}
