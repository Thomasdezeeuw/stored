use std::pin::Pin;

use stored::{parse, serialise, Key};

mod util;

use util::poll_wait;

#[test]
fn round_trip_request() {
    let key1: Key = "81381f1dacd4824a6c503fd07057763099c12b8309d0abcec4000c9060cbbfa67988b2ada669ab4837fcd3d4ea6e2b8db2b9da9197d5112fb369fd006da545de".parse().unwrap();
    let key2: Key = "e1c112ff908febc3b98b1693a6cd3564eaf8e5e6ca629d084d9f0eba99247cacdd72e369ff8941397c2807409ff66be64be908da17ad7b8a49a2a26c0e8086aa".parse().unwrap();

    let tests = &[
        (
            serialise::Request::Store(b"Hello world"),
            parse::Request::Store(b"Hello world"),
        ),
        (
            serialise::Request::Store(b"Hello"),
            parse::Request::Store(b"Hello"),
        ),
        (serialise::Request::Store(b""), parse::Request::Store(b"")),
        (
            serialise::Request::Retrieve(&key1),
            parse::Request::Retrieve(&key1),
        ),
        (
            serialise::Request::Retrieve(&key2),
            parse::Request::Retrieve(&key2),
        ),
        (
            serialise::Request::Remove(&key1),
            parse::Request::Remove(&key1),
        ),
        (
            serialise::Request::Remove(&key2),
            parse::Request::Remove(&key2),
        ),
    ];

    for test in tests {
        let mut buf = Vec::new();
        let mut future = test.0.write_to(&mut buf);
        poll_wait(Pin::new(&mut future)).expect("unexpected error polling future");

        let (got, n) = parse::request(&buf).expect("unexpected error parsing request");
        assert_eq!(n, buf.len());
        assert_eq!(got, test.1);
    }
}

#[test]
fn round_trip_reponse() {
    let key1: Key = "81381f1dacd4824a6c503fd07057763099c12b8309d0abcec4000c9060cbbfa67988b2ada669ab4837fcd3d4ea6e2b8db2b9da9197d5112fb369fd006da545de".parse().unwrap();
    let key2: Key = "e1c112ff908febc3b98b1693a6cd3564eaf8e5e6ca629d084d9f0eba99247cacdd72e369ff8941397c2807409ff66be64be908da17ad7b8a49a2a26c0e8086aa".parse().unwrap();

    let tests = &[
        (serialise::Response::Ok, parse::Response::Ok),
        (
            serialise::Response::Store(&key1),
            parse::Response::Store(&key1),
        ),
        (
            serialise::Response::Store(&key2),
            parse::Response::Store(&key2),
        ),
        (
            serialise::Response::Value(b"Hello world"),
            parse::Response::Value(b"Hello world"),
        ),
        (
            serialise::Response::Value(b"Hello"),
            parse::Response::Value(b"Hello"),
        ),
        (serialise::Response::Value(b""), parse::Response::Value(b"")),
        (
            serialise::Response::ValueNotFound,
            parse::Response::ValueNotFound,
        ),
        (
            serialise::Response::InvalidRequestType,
            parse::Response::InvalidRequestType,
        ),
    ];

    for test in tests {
        let mut buf = Vec::new();
        let mut future = test.0.write_to(&mut buf);
        poll_wait(Pin::new(&mut future)).expect("unexpected error polling future");

        let (got, n) = parse::response(&buf).expect("unexpected error parsing response");
        assert_eq!(n, buf.len());
        assert_eq!(got, test.1);
    }
}
