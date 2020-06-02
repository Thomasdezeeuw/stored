//! Configuration file parsing.
//!
//! Main type is [`Config`].

use std::fmt;
use std::fs::File;
use std::io::{self, Read};
use std::iter::FromIterator;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::path::Path;
use std::str::FromStr;

use human_size::{Kibibyte, Mebibyte, SpecificSize};
use serde::de::{Deserializer, Error, SeqAccess, Visitor};
use serde::Deserialize;

/// Macro to include the configuration file for the `Config` docs.
macro_rules! doc {
    (#[doc = $doc1: expr], $file: expr, $( $tt2: tt )*) => {
        doc!($doc1, include_str!($file), $( $tt2 )*);
    };
    ($doc1: expr, $doc2: expr, $( $tt: tt )*) => {
        #[doc = $doc1]
        #[doc = $doc2]
        $( $tt )*
    };
}

doc!(
    #[doc = "Stored configuration.\n\nLook at `config.example.toml` example
    below to see what each option means.\n\n```toml"],
    "../config.example.toml",
    /// ```
    ///
    /// # Notes
    ///
    /// The `Deserialize` implement does a synchronous lookup of peer addresses,
    /// including when using [`Config::from_file`].
    #[derive(Deserialize, Debug)]
    pub struct Config {
        pub path: Box<Path>,
        #[serde(default = "default_max_blob_size")]
        pub max_blob_size: SpecificSize<Kibibyte>,
        pub max_store_size: Option<SpecificSize<Mebibyte>>,
        #[serde(default)]
        pub http: Http,
        #[serde(default)]
        pub distributed: Option<Distributed>,
    }
);

impl Config {
    /// Read `Config` from the file at `path`.
    pub fn from_file(path: &str) -> io::Result<Config> {
        let mut file = File::open(path)
            .map_err(|err| io::Error::new(err.kind(), "unable to open configuration file"))?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        toml::from_slice(&buf).map_err(|err| {
            let msg = format!("unable to parse configuration file: {}", err);
            io::Error::new(io::ErrorKind::Other, msg)
        })
    }
}

/// 1 GB.
fn default_max_blob_size() -> SpecificSize<Kibibyte> {
    SpecificSize::new(1024 * 1024 * 1024, Kibibyte).unwrap()
}

/// HTTP configuration.
#[derive(Deserialize, Debug)]
pub struct Http {
    #[serde(default = "default_address")]
    pub address: SocketAddr,
}

/// 127.0.0.1:8080.
fn default_address() -> SocketAddr {
    use std::net::{Ipv4Addr, SocketAddrV4};
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080))
}

impl Default for Http {
    fn default() -> Http {
        Http {
            address: default_address(),
        }
    }
}

/// Distributed configuration.
#[derive(Deserialize, Debug)]
pub struct Distributed {
    pub peer_address: SocketAddr,
    pub replicas: Replicas,
    pub peers: Peers,
}

/// The amount of replicas of the blob stored.
#[derive(Copy, Clone, Debug)]
pub enum Replicas {
    /// All nodes store all blobs (default).
    All,
    /// `(N/2)+1` nodes store a blob, where `N` is the total number of nodes in
    /// the system.
    Majority,
}

impl Default for Replicas {
    fn default() -> Replicas {
        Replicas::All
    }
}

impl fmt::Display for Replicas {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Replicas::All => f.write_str("all"),
            Replicas::Majority => f.write_str("majority"),
        }
    }
}

/// Error returned by `FromStr` implementation for [`Replicas`].
pub struct ParseReplicasErr(());

impl fmt::Display for ParseReplicasErr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("invalid replicas")
    }
}

impl FromStr for Replicas {
    type Err = ParseReplicasErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "all" => Ok(Replicas::All),
            "majority" => Ok(Replicas::Majority),
            _ => Err(ParseReplicasErr(())),
        }
    }
}

impl<'de> Deserialize<'de> for Replicas {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ReplicasVisitor;

        impl<'de> Visitor<'de> for ReplicasVisitor {
            type Value = Replicas;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string")
            }

            fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Replicas::from_str(s).map_err(Error::custom)
            }
        }

        deserializer.deserialize_str(ReplicasVisitor)
    }
}

/// Wrapper around `Vec<SocketAddr>` to use `ToSocketAddrs` to parse addresses.
#[derive(Debug)]
pub struct Peers(pub Vec<SocketAddr>);

impl<'de> Deserialize<'de> for Peers {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct PeerVisitor;

        impl<'de> Visitor<'de> for PeerVisitor {
            type Value = Peers;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a sequence")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut addresses = Vec::with_capacity(seq.size_hint().unwrap_or(0));

                while let Some(value) = seq.next_element::<&str>()? {
                    let addrs = value.to_socket_addrs().map_err(Error::custom)?;
                    addresses.extend(addrs);
                }

                Ok(Peers(addresses))
            }

            fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                let address = s.to_socket_addrs().map_err(Error::custom)?;
                let addresses = Vec::from_iter(address);
                Ok(Peers(addresses))
            }
        }

        deserializer.deserialize_seq(PeerVisitor)
    }
}

impl fmt::Display for Peers {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        struct DisplayDebug<'a>(&'a SocketAddr);

        impl<'a> fmt::Debug for DisplayDebug<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                std::fmt::Display::fmt(&self.0, f)
            }
        }

        f.debug_list()
            .entries(self.0.iter().map(DisplayDebug))
            .finish()
    }
}
