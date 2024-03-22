//! Tests for the RESP protocol.

use heph_rt::test::block_on_future;

use stored::protocol::resp::{RequestError, Resp};
use stored::protocol::{Protocol, Request};

use crate::util::TestConn;

#[test]
fn no_request() {
    block_on_future(async {
        let mut protocol = Resp::new(TestConn::new(&[]));
        assert!(protocol.next_request().await.unwrap().is_none());
    });
}

#[test]
fn add_blob() {
    block_on_future(async {
        let mut protocol = Resp::new(TestConn::new(b"*2\r\n$3\r\nSET\r\n$3\r\nfoo\r\n"));
        let request = protocol.next_request().await.unwrap().unwrap();
        if let Request::AddBlob(b"foo") = request {
            // Ok.
        } else {
            panic!("unexpected request: {request:?}");
        }
        assert!(protocol.next_request().await.unwrap().is_none());
    });
}

#[test]
fn remove_blob() {
    block_on_future(async {
        let mut protocol = Resp::new(TestConn::new(b"*2\r\n$3\r\nDEL\r\n$128\r\n45546d4d71407e82ecda31eba5bf74b65bc092b0436a2409a6b615c1f78fdb2d3da371758f07a65b5d2b3ee8fa9ea0c772dd1eff884c4c77d4290177b002ccdc\r\n"));
        let request = protocol.next_request().await.unwrap().unwrap();
        if let Request::RemoveBlob(key) = request {
            assert_eq!(key.to_string(), "45546d4d71407e82ecda31eba5bf74b65bc092b0436a2409a6b615c1f78fdb2d3da371758f07a65b5d2b3ee8fa9ea0c772dd1eff884c4c77d4290177b002ccdc");
        } else {
            panic!("unexpected request: {request:?}");
        }
        assert!(protocol.next_request().await.unwrap().is_none());
    });
}

#[test]
fn get_blob() {
    block_on_future(async {
        let mut protocol = Resp::new(TestConn::new(b"*2\r\n$3\r\nGET\r\n$128\r\n45546d4d71407e82ecda31eba5bf74b65bc092b0436a2409a6b615c1f78fdb2d3da371758f07a65b5d2b3ee8fa9ea0c772dd1eff884c4c77d4290177b002ccdc\r\n"));
        let request = protocol.next_request().await.unwrap().unwrap();
        if let Request::GetBlob(key) = request {
            assert_eq!(key.to_string(), "45546d4d71407e82ecda31eba5bf74b65bc092b0436a2409a6b615c1f78fdb2d3da371758f07a65b5d2b3ee8fa9ea0c772dd1eff884c4c77d4290177b002ccdc");
        } else {
            panic!("unexpected request: {request:?}");
        }
        assert!(protocol.next_request().await.unwrap().is_none());
    });
}

#[test]
fn contains_blob() {
    block_on_future(async {
        let mut protocol = Resp::new(TestConn::new(b"*2\r\n$6\r\nEXISTS\r\n$128\r\n45546d4d71407e82ecda31eba5bf74b65bc092b0436a2409a6b615c1f78fdb2d3da371758f07a65b5d2b3ee8fa9ea0c772dd1eff884c4c77d4290177b002ccdc\r\n"));
        let request = protocol.next_request().await.unwrap().unwrap();
        if let Request::ContainsBlob(key) = request {
            assert_eq!(key.to_string(), "45546d4d71407e82ecda31eba5bf74b65bc092b0436a2409a6b615c1f78fdb2d3da371758f07a65b5d2b3ee8fa9ea0c772dd1eff884c4c77d4290177b002ccdc");
        } else {
            panic!("unexpected request: {request:?}");
        }
        assert!(protocol.next_request().await.unwrap().is_none());
    });
}

#[test]
fn blobs_stored() {
    block_on_future(async {
        let mut protocol = Resp::new(TestConn::new(b"*1\r\n$6\r\nDBSIZE\r\n"));
        let request = protocol.next_request().await.unwrap().unwrap();
        if let Request::BlobsStored = request {
            // Ok.
        } else {
            panic!("unexpected request: {request:?}");
        }
        assert!(protocol.next_request().await.unwrap().is_none());
    });
}

#[test]
fn unknown_request() {
    block_on_future(async {
        let mut protocol = Resp::new(TestConn::new(b"*1\r\n$4\r\nWHAT\r\n"));
        let request_err = protocol.next_request().await.unwrap_err();
        if let err @ RequestError::User(_, is_fatal) = request_err {
            assert_eq!(err.to_string(), "unknown command");
            assert!(!is_fatal);
        } else {
            panic!("unexpected error: {request_err:?}");
        }
        assert!(protocol.next_request().await.unwrap().is_none());
    });
}
