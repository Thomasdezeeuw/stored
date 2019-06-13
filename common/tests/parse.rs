use coeus_common::{parse, Key};

use byteorder::{ByteOrder, NetworkEndian};

#[test]
fn parse_request() {
    let key1: Key = "81381f1dacd4824a6c503fd07057763099c12b8309d0abcec4000c9060cbbfa67988b2ada669ab4837fcd3d4ea6e2b8db2b9da9197d5112fb369fd006da545de".parse().unwrap();
    let key2: Key = "e1c112ff908febc3b98b1693a6cd3564eaf8e5e6ca629d084d9f0eba99247cacdd72e369ff8941397c2807409ff66be64be908da17ad7b8a49a2a26c0e8086aa".parse().unwrap();

    let tests: &[((u8, &[u8]), _, _)] = &[
        ((1, b"Hello world"), parse::Request::Store(b"Hello world"), 16),
        ((1, b"Hello"), parse::Request::Store(b"Hello"), 10),
        ((1, b""), parse::Request::Store(b""), 5),
        ((2, key1.as_bytes()), parse::Request::Retrieve(&key1), 65),
        ((2, key2.as_bytes()), parse::Request::Retrieve(&key2), 65),
        ((3, key1.as_bytes()), parse::Request::Remove(&key1), 65),
        ((3, key2.as_bytes()), parse::Request::Remove(&key2), 65),
    ];

    for test in tests {
        let input = create_input((test.0).0, (test.0).0 == 1, (test.0).1);
        let got = parse::request(&input).expect("unexpected error parsing request");
        assert_eq!(got.0, test.1, "unexpected result parsing request");
        assert_eq!(got.1, test.2, "unexpected number of bytes parsed");
    }
}

#[test]
fn parse_request_errors() {
    let key1: Key = "81381f1dacd4824a6c503fd07057763099c12b8309d0abcec4000c9060cbbfa67988b2ada669ab4837fcd3d4ea6e2b8db2b9da9197d5112fb369fd006da545de".parse().unwrap();
    let key2: Key = "e1c112ff908febc3b98b1693a6cd3564eaf8e5e6ca629d084d9f0eba99247cacdd72e369ff8941397c2807409ff66be64be908da17ad7b8a49a2a26c0e8086aa".parse().unwrap();

    let tests: &[(&[u8], _)] = &[
        (&[], parse::Error::Incomplete),
        (&[1], parse::Error::Incomplete),
        (&[2], parse::Error::Incomplete),
        (&[3], parse::Error::Incomplete),
        (&[4], parse::Error::InvalidType),
        (&[255], parse::Error::InvalidType),
    ];

    for test in tests {
        let got = parse::request(test.0).expect_err("unexpected valid result parsing request");
        assert_eq!(got, test.1, "unexpected result parsing request");
    }

    // Partial requests.
    let tests: &[((u8, &[u8]), _)] = &[
        ((1, b"Hello"), parse::Error::Incomplete),
        ((2, key1.as_bytes()), parse::Error::Incomplete),
        ((2, key2.as_bytes()), parse::Error::Incomplete),
        ((3, key1.as_bytes()), parse::Error::Incomplete),
        ((3, key2.as_bytes()), parse::Error::Incomplete),
    ];

    for test in tests {
        let input = create_input((test.0).0, (test.0).0 == 1, (test.0).1);

        let got = parse::request(&input[..input.len() - 2]).expect_err("unexpected valid result parsing request");
        assert_eq!(got, test.1, "unexpected result parsing request");
    }
}

#[test]
fn parse_response() {
    let key1: Key = "81381f1dacd4824a6c503fd07057763099c12b8309d0abcec4000c9060cbbfa67988b2ada669ab4837fcd3d4ea6e2b8db2b9da9197d5112fb369fd006da545de".parse().unwrap();
    let key2: Key = "e1c112ff908febc3b98b1693a6cd3564eaf8e5e6ca629d084d9f0eba99247cacdd72e369ff8941397c2807409ff66be64be908da17ad7b8a49a2a26c0e8086aa".parse().unwrap();

    let tests: &[((u8, &[u8]), _, _)] = &[
        ((1, b""), parse::Response::Ok, 1),
        ((2, key1.as_bytes()), parse::Response::Store(&key1), 65),
        ((2, key2.as_bytes()), parse::Response::Store(&key2), 65),
        ((3, b"Hello world"), parse::Response::Value(b"Hello world"), 16),
        ((3, b"Hello"), parse::Response::Value(b"Hello"), 10),
        ((3, b""), parse::Response::Value(b""), 5),
        ((4, b""), parse::Response::ValueNotFound, 1),
    ];

    for test in tests {
        let input = create_input((test.0).0, (test.0).0 == 3, (test.0).1);
        let got = parse::response(&input).expect("unexpected error parsing response");
        assert_eq!(got.0, test.1, "unexpected result parsing response");
        assert_eq!(got.1, test.2, "unexpected number of bytes parsed");
    }
}

#[test]
fn parse_response_errors() {
    let key1: Key = "81381f1dacd4824a6c503fd07057763099c12b8309d0abcec4000c9060cbbfa67988b2ada669ab4837fcd3d4ea6e2b8db2b9da9197d5112fb369fd006da545de".parse().unwrap();
    let key2: Key = "e1c112ff908febc3b98b1693a6cd3564eaf8e5e6ca629d084d9f0eba99247cacdd72e369ff8941397c2807409ff66be64be908da17ad7b8a49a2a26c0e8086aa".parse().unwrap();

    let tests: &[(&[u8], _)] = &[
        (&[], parse::Error::Incomplete),
        (&[1], parse::Error::Incomplete),
        (&[2], parse::Error::Incomplete),
        (&[3], parse::Error::Incomplete),
        (&[4], parse::Error::InvalidType),
        (&[255], parse::Error::InvalidType),
    ];

    for test in tests {
        let got = parse::request(test.0).expect_err("unexpected valid result parsing request");
        assert_eq!(got, test.1, "unexpected result parsing request");
    }

    // Partial requests.
    let tests: &[((u8, &[u8]), _)] = &[
        ((2, key1.as_bytes()), parse::Error::Incomplete),
        ((2, key2.as_bytes()), parse::Error::Incomplete),
        ((3, b"Hello"), parse::Error::Incomplete),
    ];

    for test in tests {
        let input = create_input((test.0).0, (test.0).0 == 1, (test.0).1);

        let got = parse::request(&input[..input.len() - 2]).expect_err("unexpected valid result parsing request");
        assert_eq!(got, test.1, "unexpected result parsing request");
    }
}

fn create_input(request_type: u8, write_size: bool, data: &[u8]) -> Vec<u8> {
    let mut input = Vec::new();
    input.push(request_type);

    if write_size {
        let mut buf = [0; 4];
        NetworkEndian::write_u32(&mut buf, data.len() as u32);
        input.extend_from_slice(&buf);
    }

    input.extend_from_slice(data);
    input
}
