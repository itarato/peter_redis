#![allow(unused_imports)]
use std::net::TcpListener;

extern crate pretty_env_logger;
#[macro_use]
extern crate log;

mod command_parser;
mod commands;
mod common;
mod server;

use log::info;

use crate::{common::Error, server::*};

fn main() -> Result<(), Error> {
    pretty_env_logger::init();

    info!("Peter-Redis starting");

    let server = Server::new();
    server.run()?;

    info!("Peter-Redis ending");

    Ok(())
}
