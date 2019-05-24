use coeus_common::{parse, request, response};

#[test]
#[ignore = "TODO: add input"]
fn parse_request() {
    let tests = &[
        (&[1], parse::Request::Store(request::Store::new(b"Hello world")), 12),
        // 1 + size < 1024 + value.
        // 1 + size > 1024 + value.
        // 1 + size < 1024.
        // 1 + size > 1024.
        // 2 + hash.
        // 3 + hash.
    ];

    for test in tests {
        let got = parse::request(test.0)
            .expect("unexpected error parsing request");
        assert_eq!(got.0, test.1, "unexpected result parsing request");
        assert_eq!(got.1, test.2, "unexpected number of bytes parsed");
    }
}

#[test]
fn parse_request_errors() {
    let tests: &[(&[u8], _)] = &[
        (&[], parse::Error::Incomplete),
        (&[1], parse::Error::Incomplete),
        (&[2], parse::Error::Incomplete),
        (&[3], parse::Error::Incomplete),
    ];

    for test in tests {
        let got = parse::request(test.0)
            .expect_err("unexpected valid result parsing request");
        assert_eq!(got, test.1, "unexpected result parsing request");
    }
}

#[test]
fn parse_response() {
    let tests = &[
        (&[1], parse::Response::Ok(response::Ok), 1),
        // 2 + hash.
        // 3 + size < 1024 + value.
        // 3 + size < 1024.
        // 3 + size > 1024 + value.
        // 3 + size > 1024.
        (&[4], parse::Response::ValueNotFound(response::ValueNotFound), 1),
    ];

    for test in tests {
        let got = parse::response(test.0)
            .expect("unexpected error parsing response");
        assert_eq!(got.0, test.1, "unexpected result parsing response");
        assert_eq!(got.1, test.2, "unexpected number of bytes parsed");
    }
}

#[test]
fn parse_response_errors() {
    let tests: &[(&[u8], _)] = &[
        (&[], parse::Error::Incomplete),
        (&[2], parse::Error::Incomplete),
        (&[3], parse::Error::Incomplete),
        (&[5], parse::Error::InvalidType),
        (&[255], parse::Error::InvalidType),
    ];

    for test in tests {
        let got = parse::response(test.0)
            .expect_err("unexpected valid result parsing response");
        assert_eq!(got, test.1, "unexpected result parsing response");
    }
}
