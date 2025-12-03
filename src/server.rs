use std::{cell::Cell, sync::Arc};

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
    request_counter: Cell<u64>,
    port: u16,
}

impl Server {
    pub(crate) fn new(port: u16, replica_of: Option<(String, u16)>) -> Self {
        Self {
            engine: Arc::new(Engine::new(replica_of)),
            request_counter: Cell::new(0),
            port,
        }
    }

    pub(crate) async fn run(&self) -> Result<(), Error> {
        tokio::spawn({
            let engine = self.engine.clone();
            async move {
                engine.init().await.unwrap();
            }
        });

        let listener = TcpListener::bind(format!("127.0.0.1:{}", self.port))
            .await
            .context("tcp-bind")?;

        loop {
            let (stream, _) = listener.accept().await.context("accept-tcp-connection")?;

            let request_count = self.request_counter.take();
            self.request_counter.set(request_count + 1);

            tokio::spawn({
                let engine = self.engine.clone();

                async move {
                    match Self::handle_request(stream, engine, request_count).await {
                        Ok(_) => debug!("Request completed"),
                        Err(err) => error!("Request has failed with reason: {}", err),
                    }
                }
            });
        }
    }

    async fn handle_request(
        mut stream: TcpStream,
        engine: Arc<Engine>,
        request_count: u64,
    ) -> Result<(), Error> {
        loop {
            match read_from_tcp_stream(&mut stream).await? {
                Some(input) => match CommandParser::parse(input) {
                    Ok(command) => {
                        debug!("Received command: {:?}", command);
                        let result = engine.execute(&command, request_count).await?;
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
