use std::sync::Arc;

use anyhow::Context;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};

use crate::{
    command_parser::CommandParser,
    common::{read_from_tcp_stream, Error},
    engine::Engine,
    resp::RespValue,
};

pub(crate) struct Server {
    engine: Arc<Engine>,
}

impl Server {
    pub(crate) fn new() -> Self {
        Self {
            engine: Arc::new(Engine::new()),
        }
    }

    pub(crate) async fn run(&self) -> Result<(), Error> {
        let listener = TcpListener::bind("127.0.0.1:6379")
            .await
            .context("tcp-bind")?;

        loop {
            let (stream, _) = listener.accept().await.context("accept-tcp-connection")?;
            tokio::spawn({
                let engine = self.engine.clone();

                async move {
                    match Self::handle_request(stream, engine).await {
                        Ok(_) => debug!("Request completed"),
                        Err(err) => error!("Request has failed with reason: {}", err),
                    }
                }
            });
        }
    }

    async fn handle_request(mut stream: TcpStream, engine: Arc<Engine>) -> Result<(), Error> {
        loop {
            match read_from_tcp_stream(&mut stream).await? {
                Some(input) => match CommandParser::parse(input) {
                    Ok(command) => {
                        let result = engine.execute(&command).await?;
                        stream
                            .write_all(result.serialize().as_bytes())
                            .await
                            .context("write-simple-value-back-to-stream")?;
                    }
                    Err(err) => {
                        stream
                            .write_all(RespValue::SimpleError(err).serialize().as_bytes())
                            .await
                            .context("write-simple-value-back-to-stream")?;
                    }
                },
                None => break,
            }
        }

        Ok(())
    }
}
