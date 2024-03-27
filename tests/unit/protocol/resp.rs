//! Tests for the RESP protocol.

use std::io;

use heph_rt::test::block_on_future;

use stored::key::key;
use stored::protocol::resp::{RequestError, Resp};
use stored::protocol::{Protocol, Request, Response};
use stored::storage::mem::Blob;

use crate::util::TestConn;

#[test]
fn no_request() {
    block_on_future(async {
        let mut protocol = Resp::new(TestConn::with_input(&[]));
        assert!(protocol.next_request().await.unwrap().is_none());
    });
}

#[test]
fn add_blob() {
    block_on_future(async {
        let mut protocol = Resp::new(TestConn::with_input(b"*2\r\n$3\r\nSET\r\n$3\r\nfoo\r\n"));
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
        let mut protocol = Resp::new(TestConn::with_input(b"*2\r\n$3\r\nDEL\r\n$128\r\n45546d4d71407e82ecda31eba5bf74b65bc092b0436a2409a6b615c1f78fdb2d3da371758f07a65b5d2b3ee8fa9ea0c772dd1eff884c4c77d4290177b002ccdc\r\n"));
        let request = protocol.next_request().await.unwrap().unwrap();
        if let Request::RemoveBlob(key) = request {
            assert_eq!(key, key!("45546d4d71407e82ecda31eba5bf74b65bc092b0436a2409a6b615c1f78fdb2d3da371758f07a65b5d2b3ee8fa9ea0c772dd1eff884c4c77d4290177b002ccdc"));
        } else {
            panic!("unexpected request: {request:?}");
        }
        assert!(protocol.next_request().await.unwrap().is_none());
    });
}

#[test]
fn get_blob() {
    block_on_future(async {
        let mut protocol = Resp::new(TestConn::with_input(b"*2\r\n$3\r\nGET\r\n$128\r\n45546d4d71407e82ecda31eba5bf74b65bc092b0436a2409a6b615c1f78fdb2d3da371758f07a65b5d2b3ee8fa9ea0c772dd1eff884c4c77d4290177b002ccdc\r\n"));
        let request = protocol.next_request().await.unwrap().unwrap();
        if let Request::GetBlob(key) = request {
            assert_eq!(key, key!("45546d4d71407e82ecda31eba5bf74b65bc092b0436a2409a6b615c1f78fdb2d3da371758f07a65b5d2b3ee8fa9ea0c772dd1eff884c4c77d4290177b002ccdc"));
        } else {
            panic!("unexpected request: {request:?}");
        }
        assert!(protocol.next_request().await.unwrap().is_none());
    });
}

#[test]
fn contains_blob() {
    block_on_future(async {
        let mut protocol = Resp::new(TestConn::with_input(b"*2\r\n$6\r\nEXISTS\r\n$128\r\n45546d4d71407e82ecda31eba5bf74b65bc092b0436a2409a6b615c1f78fdb2d3da371758f07a65b5d2b3ee8fa9ea0c772dd1eff884c4c77d4290177b002ccdc\r\n"));
        let request = protocol.next_request().await.unwrap().unwrap();
        if let Request::ContainsBlob(key) = request {
            assert_eq!(key, key!("45546d4d71407e82ecda31eba5bf74b65bc092b0436a2409a6b615c1f78fdb2d3da371758f07a65b5d2b3ee8fa9ea0c772dd1eff884c4c77d4290177b002ccdc"));
        } else {
            panic!("unexpected request: {request:?}");
        }
        assert!(protocol.next_request().await.unwrap().is_none());
    });
}

#[test]
fn blobs_stored() {
    block_on_future(async {
        let mut protocol = Resp::new(TestConn::with_input(b"*1\r\n$6\r\nDBSIZE\r\n"));
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
        let mut protocol = Resp::new(TestConn::with_input(b"*1\r\n$4\r\nWHAT\r\n"));
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

/* FIXME.
error: implementation of `Send` is not general enough
    | |______^ implementation of `Send` is not general enough
    |
    = note: `Send` would have to be implemented for the type `&stored::key::Key`
    = note: ...but `Send` is actually implemented for the type `&'0 stored::key::Key`, for some specific lifetime `'0`

#[test]
fn response_added_blob() {
    block_on_future(async {
        let (conn, recv) = TestConn::new();
        let mut protocol = Resp::new(conn);

        let key = key!("45546d4d71407e82ecda31eba5bf74b65bc092b0436a2409a6b615c1f78fdb2d3da371758f07a65b5d2b3ee8fa9ea0c772dd1eff884c4c77d4290177b002ccdc");
        let response = Response::<Blob>::Added(key);
        protocol.reply(response).await.unwrap();
        assert_eq!(&*recv.received(), b"TODO");
    });
}

#[test]
fn response_already_stores() {
    block_on_future(async {
        let (conn, recv) = TestConn::new();
        let mut protocol = Resp::new(conn);

        let key = key!("45546d4d71407e82ecda31eba5bf74b65bc092b0436a2409a6b615c1f78fdb2d3da371758f07a65b5d2b3ee8fa9ea0c772dd1eff884c4c77d4290177b002ccdc");
        let response = Response::<Blob>::AlreadyStored(key);
        protocol.reply(response).await.unwrap();
        assert_eq!(&*recv.received(), b"TODO");
    });
}

#[test]
fn response_blob_removed() {
    block_on_future(async {
        let (conn, recv) = TestConn::new();
        let mut protocol = Resp::new(conn);

        let response = Response::<Blob>::BlobRemoved;
        protocol.reply(response).await.unwrap();
        assert_eq!(&*recv.received(), b"TODO");
    });
}

#[test]
fn response_blob_not_removed() {
    block_on_future(async {
        let (conn, recv) = TestConn::new();
        let mut protocol = Resp::new(conn);

        let response = Response::<Blob>::BlobNotRemoved;
        protocol.reply(response).await.unwrap();
        assert_eq!(&*recv.received(), b"TODO");
    });
}

#[test]
fn response_blob() {
    block_on_future(async {
        let (conn, recv) = TestConn::new();
        let mut protocol = Resp::new(conn);

        let response = Response::<Blob>::Blob(Blob::from(b"Hello, World".as_slice()));
        protocol.reply(response).await.unwrap();
        assert_eq!(&*recv.received(), b"TODO");
    });
}

#[test]
fn response_blob_not_found() {
    block_on_future(async {
        let (conn, recv) = TestConn::new();
        let mut protocol = Resp::new(conn);

        let response = Response::<Blob>::BlobNotFound;
        protocol.reply(response).await.unwrap();
        assert_eq!(&*recv.received(), b"TODO");
    });
}

#[test]
fn response_contains_blob() {
    block_on_future(async {
        let (conn, recv) = TestConn::new();
        let mut protocol = Resp::new(conn);

        let response = Response::<Blob>::ContainsBlob;
        protocol.reply(response).await.unwrap();
        assert_eq!(&*recv.received(), b"TODO");
    });
}

#[test]
fn response_not_contains_blob() {
    block_on_future(async {
        let (conn, recv) = TestConn::new();
        let mut protocol = Resp::new(conn);

        let response = Response::<Blob>::NotContainBlob;
        protocol.reply(response).await.unwrap();
        assert_eq!(&*recv.received(), b"TODO");
    });
}

#[test]
fn response_contains_blobs() {
    block_on_future(async {
        let (conn, recv) = TestConn::new();
        let mut protocol = Resp::new(conn);

        let response = Response::<Blob>::ContainsBlobs(123);
        protocol.reply(response).await.unwrap();
        assert_eq!(&*recv.received(), b"TODO");
    });
}

#[test]
fn response_error() {
    block_on_future(async {
        let (conn, recv) = TestConn::new();
        let mut protocol = Resp::new(conn);

        let response = Response::<Blob>::Error;
        protocol.reply(response).await.unwrap();
        assert_eq!(&*recv.received(), b"TODO");
    });
}
*/

/* FIXME
error: implementation of `Buf` is not general enough
    | |______^ implementation of `Buf` is not general enough
    |
    = note: `&[u8]` must implement `Buf`
    = note: ...but `Buf` is actually implemented for the type `&'static [u8]`
#[test]
fn reply_to_conn_error() {
    block_on_future(async {
        let (conn, recv) = TestConn::new();
        let mut protocol = Resp::new(conn);

        let err = RequestError::Conn(io::ErrorKind::NotFound.into());
        protocol.reply_to_error(err).await.unwrap();
        assert_eq!(&*recv.received(), b"TODO");
    });
}
*/
