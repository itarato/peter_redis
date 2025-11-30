extern crate pretty_env_logger;
#[macro_use]
extern crate log;

mod command_parser;
mod commands;
mod common;
mod database;
mod engine;
mod resp;
mod server;

use log::info;

use crate::{common::Error, server::*};
use clap::Parser;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = 6379)]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    pretty_env_logger::init();

    info!("Peter-Redis starting");

    let args = Args::parse();

    let server = Server::new(args.port);
    server.run().await?;

    info!("Peter-Redis ending");

    Ok(())
}
