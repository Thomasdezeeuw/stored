use std::fs::File;
use std::io::{self, Read};
use std::iter::FromIterator;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::path::Path;
use std::{env, fmt};

use heph::net::tcp;
use heph::rt::options::{ActorOptions, Priority};
use heph::{NewActor, Runtime, RuntimeError};
use human_size::{Kibibyte, Mebibyte, SpecificSize};
use log::info;
use serde::de::{Deserializer, Error, SeqAccess, Visitor};
use serde::Deserialize;

use stored::server::supervisors::{http_supervisor, DbSupervisor, ServerSupervisor};
use stored::storage::Storage;
use stored::{db, http};

/// stored Configuration.
///
/// Look at `example_config.toml` in to see what each option means.
///
/// # Notes
///
/// Does a synchronous lookup of peer addresses.
#[derive(Deserialize, Debug)]
struct Config {
    path: Box<Path>,
    #[serde(default = "default_max_blob_size")]
    max_blob_size: SpecificSize<Kibibyte>,
    max_store_size: Option<SpecificSize<Mebibyte>>,
    #[serde(default = "default_http_config")]
    http: HttpConfig,
    peer: Option<PeerConfig>,
}

fn default_max_blob_size() -> SpecificSize<Kibibyte> {
    // 1 GB.
    SpecificSize::new(1024 * 1024 * 1024, Kibibyte).unwrap()
}

fn default_http_config() -> HttpConfig {
    HttpConfig {
        address: default_address(),
    }
}

/// HTTP configuration.
#[derive(Deserialize, Debug)]
struct HttpConfig {
    #[serde(default = "default_address")]
    address: SocketAddr,
}

fn default_address() -> SocketAddr {
    use std::net::{Ipv4Addr, SocketAddrV4};
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080))
}

/// Peer configuration to run in distributed mode.
#[derive(Deserialize, Debug)]
struct PeerConfig {
    address: SocketAddr,
    peers: Peers,
}

/// Wrapper around `Vec<SocketAddr>` to use `ToSocketAddrs` to parse addresses.
#[derive(Debug)]
struct Peers(Vec<SocketAddr>);

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

fn main() -> Result<(), RuntimeError<io::Error>> {
    heph::log::init();

    let config_path = env::args().nth(1).ok_or_else(|| {
        eprintln!("Missing path to configuration file.\nUsage:\n\tstored <path_to_config>");
        io::Error::new(io::ErrorKind::Other, "missing path to configuration file")
    })?;

    let mut config_file = File::open(config_path)
        .map_err(|err| io::Error::new(err.kind(), "unable to open configuration file"))?;
    let mut buf = Vec::new();
    config_file.read_to_end(&mut buf)?;
    let config: Config = toml::from_slice(&buf).map_err(|err| {
        let msg = format!("unable to parse configuration file: {}", err);
        io::Error::new(io::ErrorKind::Other, msg)
    })?;

    let mut runtime = Runtime::new().use_all_cores();

    // Start our database actor.
    info!("opening database '{}'", config.path.display());
    let storage = Storage::open(&*config.path)?;
    let db_supervisor = DbSupervisor::new(config.path);
    let db_ref = runtime
        .spawn_sync_actor(db_supervisor, db::actor as fn(_, _) -> _, storage)
        .map_err(RuntimeError::map_type)?;

    // Setup our HTTP server.
    info!("listening on http://{}", config.http.address);
    let http_actor = (http::actor as fn(_, _, _, _) -> _)
        .map_arg(move |(stream, arg)| (stream, arg, db_ref.clone()));
    let http_listener = tcp::Server::setup(
        config.http.address,
        http_supervisor,
        http_actor,
        ActorOptions::default(),
    )?;

    if let Some(peer_config) = config.peer {
        info!("listening on {} for peer connections", peer_config.address);
        info!("connecting to peers: {}", peer_config.peers);
    }

    runtime
        .with_setup(move |mut runtime_ref| {
            // Start our HTTP server.
            let options = ActorOptions::default().with_priority(Priority::LOW);
            let server_ref = runtime_ref.try_spawn(ServerSupervisor, http_listener, (), options)?;
            runtime_ref.receive_signals(server_ref.try_map());

            Ok(())
        })
        .start()
}
