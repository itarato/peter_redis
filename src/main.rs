extern crate pretty_env_logger;
#[macro_use]
extern crate log;

mod command_parser;
mod commands;
mod common;
mod server;

use log::info;

use crate::{common::Error, server::*};

#[tokio::main]
async fn main() -> Result<(), Error> {
    pretty_env_logger::init();

    info!("Peter-Redis starting");

    let server = Server::new();
    server.run().await?;

    info!("Peter-Redis ending");

    Ok(())
}
