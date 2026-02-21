//! Module with the client operations.

use std::io;
use std::mem::take;
use std::net::SocketAddr;

use heph_rt::Access;
use heph_rt::net::tcp::stream::TcpStream;

use crate::{Blob, Client, Key, resp};

/// Create a new [`Client`].
pub struct Connect<'rt, RT> {
    rt: &'rt RT,
    address: SocketAddr,
}

impl<'rt, RT> Connect<'rt, RT> {
    pub(crate) const fn new(rt: &'rt RT, address: SocketAddr) -> Connect<'rt, RT> {
        Connect { rt, address }
    }
}

impl<'rt, RT: Access> IntoFuture for Connect<'rt, RT> {
    type Output = io::Result<Client>;
    type IntoFuture = impl Future<Output = Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let Connect { rt, address } = self;
        async move {
            let conn = TcpStream::connect(rt, address).await?;
            Ok(Client {
                conn,
                buf: Vec::with_capacity(512),
            })
        }
    }
}

/// Add a blob to the store.
pub struct Add<'c> {
    client: &'c mut Client,
    blob: Blob,
}

impl<'c> Add<'c> {
    pub(crate) const fn new(client: &'c mut Client, blob: Blob) -> Add<'c> {
        Add { client, blob }
    }
}

impl<'c> IntoFuture for Add<'c> {
    type Output = io::Result<Key>;
    type IntoFuture = impl Future<Output = Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let Add { client, blob } = self;
        async move {
            let mut buf = take(&mut client.buf);

            resp::encode::array(&mut buf, 2); // "SET" + blob.
            resp::encode::string(&mut buf, "SET");
            resp::encode::string_start(&mut buf, blob.len());

            let bufs = (buf, blob, resp::CRLF);
            let bufs = client.conn.send_vectored_all(bufs).await?;
            client.buf = bufs.0;
            client.buf.clear();

            client.read_key().await
        }
    }
}

/// Remove a blob from the store.
///
/// Returns true if the blob was removed, false if the blob was never
/// stored.
pub struct Remove<'c, 'k> {
    client: &'c mut Client,
    key: &'k Key,
}

impl<'c, 'k> Remove<'c, 'k> {
    pub(crate) const fn new(client: &'c mut Client, key: &'k Key) -> Remove<'c, 'k> {
        Remove { client, key }
    }
}

impl<'c, 'k> IntoFuture for Remove<'c, 'k> {
    type Output = io::Result<bool>;
    type IntoFuture = impl Future<Output = Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let Remove { client, key } = self;
        async move {
            let mut buf = take(&mut client.buf);

            resp::encode::array(&mut buf, 2); // "DEL" + key.
            resp::encode::string(&mut buf, "DEL");
            resp::encode::key(&mut buf, key);

            client.buf = client.conn.send_all(buf).await?;
            client.buf.clear();

            client.read_bool().await
        }
    }
}

/// Get blob from the store.
pub struct Get<'c, 'k> {
    client: &'c mut Client,
    key: &'k Key,
}

impl<'c, 'k> Get<'c, 'k> {
    pub(crate) const fn new(client: &'c mut Client, key: &'k Key) -> Get<'c, 'k> {
        Get { client, key }
    }
}

impl<'c, 'k> IntoFuture for Get<'c, 'k> {
    type Output = io::Result<Option<Blob>>;
    type IntoFuture = impl Future<Output = Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let Get { client, key } = self;
        async move {
            let mut buf = take(&mut client.buf);
            buf.clear();

            resp::encode::array(&mut buf, 2); // "GET" + key.
            resp::encode::string(&mut buf, "GET");
            resp::encode::key(&mut buf, key);

            client.buf = client.conn.send_all(buf).await?;
            client.buf.clear();

            client
                .read_opt_string(|blob| Ok(blob.map(Into::into)))
                .await
        }
    }
}

/// Check if a blob is stored.
pub struct Contains<'c, 'k> {
    client: &'c mut Client,
    key: &'k Key,
}

impl<'c, 'k> Contains<'c, 'k> {
    pub(crate) const fn new(client: &'c mut Client, key: &'k Key) -> Contains<'c, 'k> {
        Contains { client, key }
    }
}

impl<'c, 'k> IntoFuture for Contains<'c, 'k> {
    type Output = io::Result<bool>;
    type IntoFuture = impl Future<Output = Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let Contains { client, key } = self;
        async move {
            let mut buf = take(&mut client.buf);

            resp::encode::array(&mut buf, 2); // "EXISTS" + key.
            resp::encode::string(&mut buf, "EXISTS");
            resp::encode::key(&mut buf, key);

            client.buf = client.conn.send_all(buf).await?;
            client.buf.clear();

            client.read_bool().await
        }
    }
}

/// Check the number of blobs stored.
pub struct BlobsStored<'c> {
    client: &'c mut Client,
}

impl<'c> BlobsStored<'c> {
    pub(crate) const fn new(client: &'c mut Client) -> BlobsStored<'c> {
        BlobsStored { client }
    }
}

impl<'c> IntoFuture for BlobsStored<'c> {
    type Output = io::Result<usize>;
    type IntoFuture = impl Future<Output = Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let BlobsStored { client } = self;
        async move {
            let mut buf = take(&mut client.buf);

            resp::encode::array(&mut buf, 1); // "DBSIZE".
            resp::encode::string(&mut buf, "DBSIZE");

            client.buf = client.conn.send_all(buf).await?;
            client.buf.clear();

            client.read_integer().await
        }
    }
}
