use std::env;
use std::fs::File;
use std::io::{self, Read};
use std::net::SocketAddr;
use std::path::Path;

use heph::net::tcp;
use heph::rt::options::{ActorOptions, Priority};
use heph::{NewActor, Runtime, RuntimeError};
use human_size::{Kibibyte, Mebibyte, SpecificSize};
use log::info;
use serde::Deserialize;

use stored::server::actors::{db, http};
use stored::server::storage::Storage;
use stored::server::supervisors::{http_supervisor, DbSupervisor, ServerSupervisor};

/// stored Configuration.
///
/// Look at `example_config.toml` in to see what each option means.
#[derive(Deserialize)]
struct Config {
    path: Box<Path>,
    #[serde(default = "default_max_blob_size")]
    #[allow(dead_code)] // TODO: use this field.
    max_blob_size: SpecificSize<Kibibyte>,
    #[allow(dead_code)] // TODO: use this field.
    max_store_size: Option<SpecificSize<Mebibyte>>,
    #[serde(default = "default_http_config")]
    http: HttpConfig,
    #[allow(dead_code)] // TODO: use this field.
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
#[derive(Deserialize)]
struct HttpConfig {
    #[serde(default = "default_address")]
    address: SocketAddr,
}

fn default_address() -> SocketAddr {
    use std::net::{Ipv4Addr, SocketAddrV4};
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080))
}

/// Peer configuration to run in distributed mode.
#[derive(Deserialize)]
#[allow(dead_code)] // TODO: use this.
struct PeerConfig {
    address: SocketAddr,
    peers: Vec<SocketAddr>,
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
