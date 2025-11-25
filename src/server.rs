use anyhow::Context;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};

use crate::{
    common::{read_from_tcp_stream, Error},
    // engine::Engine,
    resp::RespValue,
};

pub(crate) struct Server {
    // engine: Engine,
}

impl Server {
    pub(crate) fn new() -> Self {
        Self {
            // engine: Engine::new(),
        }
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
            match read_from_tcp_stream(&mut stream).await? {
                Some(payload) => Self::handle_payload(payload, &mut stream).await?,
                None => break,
            }
        }

        Ok(())
    }

    async fn handle_payload(payload: RespValue, stream: &mut TcpStream) -> Result<(), Error> {
        match payload {
            RespValue::SimpleString(s) => unimplemented!(),
            RespValue::BulkString(s) => {
                if s == "PING" {
                    stream
                        .write_all(b"+PONG\r\n")
                        .await
                        .context("write-back-to-stream-at-ping")?;
                } else {
                    unimplemented!()
                }
            }
            RespValue::Array(items) => {
                for item in items {
                    Box::pin(Self::handle_payload(item, stream)).await?;
                }
            }
        }

        Ok(())
    }
}
