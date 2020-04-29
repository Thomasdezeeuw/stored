#![feature(process_exitcode_placeholder)]

use std::env;
use std::process::ExitCode;

use heph::Runtime;
use log::{error, info};

use stored::config::Config;
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
    \tstored <path_to_config>
    \tstored -v or --version
    \tstored -h or --help"
);

fn main() -> ExitCode {
    heph::log::init();
    let code = match try_main() {
        Ok(()) => ExitCode::SUCCESS,
        Err(code) => code,
    };
    log::logger().flush();
    return code;
}

fn try_main() -> Result<(), ExitCode> {
    let config_path = parse_args()?;
    let config = Config::from_file(&config_path).map_err(|err| {
        error!("error opening configuration file: {}", err);
        ExitCode::FAILURE
    })?;

    let mut runtime = Runtime::new().use_all_cores();

    info!("opening database '{}'", config.path.display());
    let db_ref = db::start(&mut runtime, config.path).map_err(|err| {
        error!("error opening database: {}", err);
        ExitCode::FAILURE
    })?;

    info!("listening on http://{}", config.http.address);
    let start_listener = http::setup(config.http.address, db_ref).map_err(|err| {
        error!("error binding HTTP server: {}", err);
        ExitCode::FAILURE
    })?;

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
        .map_err(|err| {
            error!("{}", err);
            ExitCode::FAILURE
        })
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
        Some(arg) if arg.starts_with("-") => {
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
