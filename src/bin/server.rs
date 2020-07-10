#![feature(process_exitcode_placeholder)]

use std::env;
use std::process::ExitCode;

use heph::Runtime;
use log::{error, info};

use stored::config::Config;
use stored::peer::{self, Peers};
use stored::{db, http};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const USAGE: &str = concat!(
    "Stored v",
    env!("CARGO_PKG_VERSION"),
    "

Stored (pronounced store-daemon, or just stored) is a distributed immutable blob
store. Stored is not a key-value store, as the key isn't the decided by the user
but by the SHA-512 checksum of the stored blob.

It supports three operations: storing, retrieving and removing blobs. As the key
of a blob is its checksum it is not possible to modify blobs. If a blob needs to
be modified a new blob simply needs to be stored and the new key used. The
client can validate the correct delivery of the blob by using the returned key
(checksum). The blob themselves are unchanged by Stored.

Usage:
    stored <path_to_config>
    stored -v or --version
    stored -h or --help

Environment Variables
    LOG_LEVEL
      The severity level of messages to log. Can be one of: 'error', 'warn',
      'info', 'debug' or 'trace'. Note that 'debug' and 'trace' might not be
      available in release builds."
);

fn main() -> ExitCode {
    heph::log::init();
    let code = match try_main() {
        Ok(()) => ExitCode::SUCCESS,
        Err(code) => code,
    };
    log::logger().flush();
    code
}

/// Macro to log an error and map it to `ExitCode::FAILURE`.
macro_rules! map_err {
    ($format: expr) => {
        |err| {
            error!($format, err);
            ExitCode::FAILURE
        }
    };
}

fn try_main() -> Result<(), ExitCode> {
    let config_path = parse_args()?;
    let config = Config::from_file(&config_path)
        .map_err(map_err!("error reading configuration file: {}"))?;

    let mut runtime = Runtime::new().map_err(map_err!("error creating Heph runtime: {}"))?;

    info!("opening database '{}'", config.path.display());
    let db_ref =
        db::start(&mut runtime, config.path).map_err(map_err!("error opening database: {}"))?;

    let peers = if let Some(distributed_config) = config.distributed {
        info!(
            "listening on {} for peer connections",
            distributed_config.peer_address
        );
        info!("connecting to peers: {}", distributed_config.peers);
        info!("blob replication method: {}", distributed_config.replicas);
        peer::start(&mut runtime, distributed_config, db_ref.clone())
            .map_err(map_err!("error setting up peer actors: {}"))?
    } else {
        Peers::empty(db_ref.clone())
    };

    info!("listening on http://{}", config.http.address);
    let start_listener = http::setup(config.http.address, db_ref, peers)
        .map_err(map_err!("error binding HTTP server: {}"))?;

    // FIXME: only start the HTTP listener once we're synchronised.

    runtime
        .use_all_cores()
        .with_setup(move |mut runtime_ref| start_listener(&mut runtime_ref))
        .start()
        .map_err(map_err!("{}"))
}

/// Parses the arguments.
/// Handles `-v` & `--version`, `-h` & `--help` and unknown arguments.
fn parse_args() -> Result<String, ExitCode> {
    match env::args().nth(1) {
        Some(arg) if arg == "-v" || arg == "--version" => {
            println!("Stored v{}", VERSION);
            Err(ExitCode::SUCCESS)
        }
        Some(arg) if arg == "-h" || arg == "--help" => {
            println!("{}", USAGE);
            Err(ExitCode::SUCCESS)
        }
        Some(arg) if arg.starts_with('-') => {
            eprintln!("unknown argument '{}'.\n\n{}", arg, USAGE);
            Err(ExitCode::FAILURE)
        }
        Some(config_path) => Ok(config_path),
        None => {
            eprintln!("missing path to configuration file.\n\n{}", USAGE);
            Err(ExitCode::FAILURE)
        }
    }
}
