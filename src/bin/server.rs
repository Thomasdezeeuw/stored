use std::{env, io};

use heph::{Runtime, RuntimeError};
use log::info;

use stored::config::Config;
use stored::{db, http};

fn main() -> Result<(), RuntimeError<io::Error>> {
    heph::log::init();

    let config_path = env::args().nth(1).ok_or_else(|| {
        eprintln!("Missing path to configuration file.\nUsage:\n\tstored <path_to_config>");
        io::Error::new(io::ErrorKind::Other, "missing path to configuration file")
    })?;
    let config = Config::from_file(&config_path)?;

    let mut runtime = Runtime::new().use_all_cores();

    info!("opening database '{}'", config.path.display());
    let db_ref = db::start(&mut runtime, config.path)?;

    // Setup our HTTP server.
    info!("listening on http://{}", config.http.address);
    let start_listener = http::setup(config.http.address, db_ref)?;

    if let Some(distributed_config) = config.distributed {
        info!(
            "listening on {} for peer connections",
            distributed_config.peer_address
        );
        info!("connecting to peers: {}", distributed_config.peers);
        info!("synchronisation method: {}", distributed_config.sync);
    }

    runtime
        .with_setup(move |mut runtime_ref| start_listener(&mut runtime_ref))
        .start()
}
