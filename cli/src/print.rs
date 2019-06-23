//! Module with printing functions.

use std::io::{self, Write};

use coeus::response_to;

pub fn store(response: response_to::Store) -> io::Result<()> {
    use response_to::Store::*;
    match response {
        Success(key) => io::stdout().write_fmt(format_args!("{}\n", key)),
        UnexpectedResponse => io::stderr().write_all(b"got an unexpected reponse\n"),
    }
}

pub fn retrieve(response: response_to::Retrieve) -> io::Result<()> {
    use response_to::Retrieve::*;
    match response {
        Value(value) => {
            let stdout = io::stdout();
            let mut stdout = stdout.lock();
            stdout.write_all(value)?;
            stdout.write(b"\n")?;
            stdout.flush()
        },
        ValueNotFound => io::stderr().write_all(b"value not found\n"),
        UnexpectedResponse => io::stderr().write_all(b"got an unexpected reponse\n"),
    }
}

pub fn remove(response: response_to::Remove) -> io::Result<()> {
    use response_to::Remove::*;
    match response {
        Ok => io::stdout().write_all(b"value removed\n"),
        ValueNotFound => io::stderr().write_all(b"value not found\n"),
        UnexpectedResponse => io::stderr().write_all(b"got an unexpected reponse\n"),
    }
}
