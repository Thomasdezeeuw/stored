use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::{env, io};

use heph::net::tcp;
use heph::rt::options::{ActorOptions, Priority};
use heph::{NewActor, Runtime, RuntimeError};
use log::info;

use stored::server::actors::{db, http};
use stored::server::storage::Storage;
use stored::server::supervisors::{http_supervisor, DbSupervisor, ServerSupervisor};

fn main() -> Result<(), RuntimeError<io::Error>> {
    heph::log::init();

    let mut runtime = Runtime::new().use_all_cores();

    // TODO: get this from a configuration file.
    let port: u16 = env::var("PORT")
        .map(|e| e.parse::<u16>().expect("invalid PORT env var"))
        .unwrap_or(8080);
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    let db_path: Box<Path> = env::var("DB_PATH")
        .map(|e| e.into())
        .unwrap_or_else(|_| PathBuf::from("./TMP_DB"))
        .into_boxed_path();

    // Start our database actor.
    info!("opening database '{}'", db_path.display());
    let storage = Storage::open(&*db_path)?;
    let db_supervisor = DbSupervisor::new(db_path);
    let db_ref = runtime
        .spawn_sync_actor(db_supervisor, db::actor as fn(_, _) -> _, storage)
        .map_err(RuntimeError::map_type)?;

    // Setup our HTTP server.
    info!("listening on http://{}", address);
    let http_actor = (http::actor as fn(_, _, _, _) -> _)
        .map_arg(move |(stream, arg)| (stream, arg, db_ref.clone()));
    let http_listener = tcp::Server::setup(
        address,
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
