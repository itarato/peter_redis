use std::{
    io::{BufRead, BufReader, Read},
    net::TcpStream,
};

use anyhow::Context;

pub(crate) type Error = Box<dyn std::error::Error + Send + Sync>;

pub(crate) fn read_from_tcp_stream(stream: &mut TcpStream) -> Result<Vec<String>, Error> {
    let mut buf_reader = std::io::BufReader::new(stream);

    let command_count = read_number_from_tcp_stream(&mut buf_reader, "*")?;
    debug!("Incoming command count: {}", command_count);

    let mut cmds = vec![];

    for _ in 0..command_count {
        let len = read_number_from_tcp_stream(&mut buf_reader, "$")?;
        let cmd = read_line_from_tcp_stream(&mut buf_reader)?
            .trim()
            .to_string();

        debug!("Reading command: '{}' of len '{}'", cmd, len);

        if cmd.len() != len {
            error!(
                "Command len mismatch. Expected {}, got {}. Line: {}",
                len,
                cmd.len(),
                &cmd
            );
        }

        cmds.push(cmd);
    }

    Ok(cmds)
}

pub(crate) fn read_number_from_tcp_stream(
    buf_reader: &mut BufReader<&mut TcpStream>,
    prefix: &str,
) -> Result<usize, Error> {
    let mut buf = String::new();
    buf_reader.read_line(&mut buf).context("number-read")?;

    let buf = buf.trim();
    if buf.starts_with(prefix) {
        Ok(usize::from_str_radix(&buf[prefix.len()..], 10).context("parse-number")?)
    } else if buf.is_empty() {
        return Ok(0);
    } else {
        Err(format!(
            "Unexpected line. Expected to start with {}, got: {}.",
            prefix, buf
        )
        .into())
    }
}

pub(crate) fn read_line_from_tcp_stream(
    buf_reader: &mut BufReader<&mut TcpStream>,
) -> Result<String, Error> {
    let mut buf = String::new();
    buf_reader.read_line(&mut buf).context("number-read")?;
    Ok(buf)
}
