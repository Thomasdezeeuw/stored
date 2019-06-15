//! Parses a command from a line of text.

use std::fmt;

use coeus::Key;

pub enum Request<'a> {
    Store(&'a [u8]),
    Retrieve(Key),
    Remove(Key),
}

pub enum ParseError {
    Empty,
    InvalidCommand,
    InvalidKey,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.pad(match self {
            ParseError::Empty => "no command provided",
            ParseError::InvalidCommand => "invalid command",
            ParseError::InvalidKey => "invalid key",
        })
    }
}

pub fn request(request: &str) -> Result<Request, ParseError> {
    let request_type = if let Some(index) = request.find(char::is_whitespace) {
        &request[..index]
    } else {
        return Err(ParseError::Empty);
    };

    match request_type {
        "store" => {
            let value = request[6..].trim();
            Ok(Request::Store(value.as_bytes()))
        },
        "retrieve" => {
            let key = request[9..].trim().parse()
                .map_err(|_| ParseError::InvalidKey)?;
            Ok(Request::Retrieve(key))
        },
        "remove" => {
            let key = request[7..].trim().parse()
                .map_err(|_| ParseError::InvalidKey)?;
            Ok(Request::Retrieve(key))
        },
        _ => Err(ParseError::InvalidCommand),
    }
}
