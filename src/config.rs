//! Configuration.

use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};

/// Configuration of the store.
pub struct Config {
    pub storage: Storage,
    /// Hypertext Transfer Protocol (HTTP).
    pub http: Option<ProtocolConfig>,
    /// Redis Protocol specification (RESP).
    pub resp: Option<ProtocolConfig>,
}

/// Storage type used.
pub enum Storage {
    /// In-memory only.
    InMemory,
    /// On-disk storage.
    OnDisk(PathBuf),
}

pub struct ProtocolConfig {
    /// Address to accept connections on.
    pub address: SocketAddr,
}

impl Config {
    pub fn read_from_path(path: &Path) -> io::Result<Config> {
        todo!("Config::read_from_path");
    }

    /// Returns true if at least one protocol has been configured.
    pub fn has_protocol(&self) -> bool {
        self.http.is_some() || self.resp.is_some()
    }
}

impl Default for Config {
    fn default() -> Config {
        Config {
            storage: Storage::InMemory,
            http: None,
            resp: Some(ProtocolConfig {
                address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6378),
            }),
        }
    }
}
