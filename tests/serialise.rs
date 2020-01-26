use std::pin::Pin;

use coeus::{serialise, Key};

mod util;

use util::poll_wait;

// TODO: test with bad writers:
// * One that only writes partial values, e.g. 1 byte at a time.
// * One that returns a lot of pending.
//   `-> see https://github.com/rust-lang-nursery/futures-rs/pull/1342.

#[test]
fn serialise_request() {
    let key1: Key = "81381f1dacd4824a6c503fd07057763099c12b8309d0abcec4000c9060cbbfa67988b2ada669ab4837fcd3d4ea6e2b8db2b9da9197d5112fb369fd006da545de".parse().unwrap();
    let key2: Key = "e1c112ff908febc3b98b1693a6cd3564eaf8e5e6ca629d084d9f0eba99247cacdd72e369ff8941397c2807409ff66be64be908da17ad7b8a49a2a26c0e8086aa".parse().unwrap();

    let tests: &[(_, (u8, &[u8]))] = &[
        (serialise::Request::Store(b"Hello world"), (1, b"Hello world")),
        (serialise::Request::Store(b"Hello"), (1, b"Hello")),
        (serialise::Request::Store(b""), (1, b"")),
        (serialise::Request::Retrieve(&key1), (2, key1.as_bytes())),
        (serialise::Request::Retrieve(&key2), (2, key2.as_bytes())),
        (serialise::Request::Remove(&key1), (3, key1.as_bytes())),
        (serialise::Request::Remove(&key2), (3, key2.as_bytes())),
    ];

    for test in tests {
        let mut got = Vec::new();
        let mut future = test.0.write_to(&mut got);
        poll_wait(Pin::new(&mut future)).expect("unexpected error polling future");

        let expected = create_output((test.1).0, (test.1).0 == 1, (test.1).1);
        assert_eq!(got, expected);
    }
}

#[test]
fn serialise_response() {
    let key1: Key = "81381f1dacd4824a6c503fd07057763099c12b8309d0abcec4000c9060cbbfa67988b2ada669ab4837fcd3d4ea6e2b8db2b9da9197d5112fb369fd006da545de".parse().unwrap();
    let key2: Key = "e1c112ff908febc3b98b1693a6cd3564eaf8e5e6ca629d084d9f0eba99247cacdd72e369ff8941397c2807409ff66be64be908da17ad7b8a49a2a26c0e8086aa".parse().unwrap();

    let tests: &[(_, (u8, &[u8]))] = &[
        (serialise::Response::Ok, (1, b"")),
        (serialise::Response::Store(&key1), (2, key1.as_bytes())),
        (serialise::Response::Store(&key2), (2, key2.as_bytes())),
        (serialise::Response::Value(b"Hello world"), (3, b"Hello world")),
        (serialise::Response::Value(b"Hello"), (3, b"Hello")),
        (serialise::Response::Value(b""), (3, b"")),
        (serialise::Response::ValueNotFound, (4, b"")),
        (serialise::Response::InvalidRequestType, (5, b"")),
    ];

    for test in tests {
        let mut got = Vec::new();
        let mut future = test.0.write_to(&mut got);
        poll_wait(Pin::new(&mut future)).expect("unexpected error polling future");

        let expected = create_output((test.1).0, (test.1).0 == 3, (test.1).1);
        assert_eq!(got, expected);
    }
}

fn create_output(request_type: u8, write_size: bool, data: &[u8]) -> Vec<u8> {
    let mut input = Vec::new();
    input.push(request_type);

    if write_size {
        input.extend_from_slice(&(data.len() as u32).to_be_bytes());
    }

    input.extend_from_slice(data);
    input
}
