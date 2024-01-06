//! Configuration.

use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};

/// Configuration of the store.
pub struct Config {
    pub storage: Storage,
    /// Hypertext Transfer Protocol (HTTP).
    pub http: Option<Protocol>,
    /// Redis Protocol specification (RESP).
    pub resp: Option<Protocol>,
}

/// Storage type used.
pub enum Storage {
    /// In-memory only.
    InMemory,
    /// On-disk storage.
    OnDisk(PathBuf),
}

#[derive(Clone)]
pub struct Protocol {
    /// Address to accept connections on.
    pub address: SocketAddr,
}

impl Config {
    pub fn read_from_path(path: &Path) -> io::Result<Config> {
        todo!("Config::read_from_path");
        // TODO: check if it has at least a RESP or HTTP protocol section.
    }
}

impl Default for Config {
    fn default() -> Config {
        Config {
            storage: Storage::InMemory,
            http: None,
            resp: Some(Protocol {
                address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6378),
            }),
        }
    }
}
