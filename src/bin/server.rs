use std::{env, io};

use heph::net::tcp;
use heph::rt::options::{ActorOptions, Priority};
use heph::{NewActor, Runtime, RuntimeError};
use log::info;

use stored::config::Config;
use stored::storage::Storage;
use stored::{db, http};

fn main() -> Result<(), RuntimeError<io::Error>> {
    heph::log::init();

    let config_path = env::args().nth(1).ok_or_else(|| {
        eprintln!("Missing path to configuration file.\nUsage:\n\tstored <path_to_config>");
        io::Error::new(io::ErrorKind::Other, "missing path to configuration file")
    })?;
    let config = Config::from_file(&config_path)?;

    let mut runtime = Runtime::new().use_all_cores();

    // Start our database actor.
    info!("opening database '{}'", config.path.display());
    let storage = Storage::open(&*config.path)?;
    let db_supervisor = db::Supervisor::new(config.path);
    let db_ref = runtime
        .spawn_sync_actor(db_supervisor, db::actor as fn(_, _) -> _, storage)
        .map_err(RuntimeError::map_type)?;

    // Setup our HTTP server.
    info!("listening on http://{}", config.http.address);
    let http_actor = (http::actor as fn(_, _, _, _) -> _)
        .map_arg(move |(stream, arg)| (stream, arg, db_ref.clone()));
    let http_listener = tcp::Server::setup(
        config.http.address,
        http::supervisor,
        http_actor,
        ActorOptions::default(),
    )?;

    if let Some(distributed_config) = config.distributed {
        info!(
            "listening on {} for peer connections",
            distributed_config.peer_address
        );
        info!("connecting to peers: {}", distributed_config.peers);
        info!("synchronisation method: {}", distributed_config.sync);
    }

    runtime
        .with_setup(move |mut runtime_ref| {
            // Start our HTTP server.
            let options = ActorOptions::default().with_priority(Priority::LOW);
            let server_ref =
                runtime_ref.try_spawn(http::ServerSupervisor, http_listener, (), options)?;
            runtime_ref.receive_signals(server_ref.try_map());

            Ok(())
        })
        .start()
}
