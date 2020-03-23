use std::io;
use std::path::{Path, PathBuf};

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

    // Start our database actor.
    // FIXME: replace with proper path.
    let db_path: Box<Path> = PathBuf::from("./TMP_DB").into_boxed_path();
    info!("opening database '{}'", db_path.display());
    let storage = Storage::open(&*db_path)?;
    let db_supervisor = DbSupervisor::new(db_path);
    let db_ref = runtime
        .spawn_sync_actor(db_supervisor, db::actor as fn(_, _) -> _, storage)
        .map_err(RuntimeError::map_type)?;

    // Setup our HTTP server.
    // FIXME: replace with proper address.
    let address = "127.0.0.1:8080".parse().unwrap();
    let http_actor = (http::actor as fn(_, _, _, _) -> _)
        .map_arg(move |(stream, arg)| (stream, arg, db_ref.clone()));
    let http_listener = tcp::Server::setup(
        address,
        http_supervisor,
        http_actor,
        ActorOptions::default(),
    )?;

    info!("listening on http://{}", address);

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
