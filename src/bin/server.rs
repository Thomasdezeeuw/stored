#![feature(never_type, process_exitcode_placeholder)]

use std::env;
use std::process::ExitCode;
use std::sync::Arc;

use heph::actor;
use heph::actor::context::ThreadSafe;
use heph::actor_ref::ActorGroup;
use heph::rt::options::{ActorOptions, Priority};
use heph::rt::{Runtime, Signal};
use heph::supervisor::NoSupervisor;
use log::{error, info};
use parking_lot::RwLock;

use stored::config::Config;
use stored::passport::Uuid;
use stored::peer::{self, Peers};
use stored::util::CountDownLatch;
use stored::{db, http};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const COMMIT_VERSION: Option<&str> = option_env!("COMMIT_VERSION");

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
    // Make the generated `Uuid`s random.
    Uuid::initialise();

    let config_path = parse_args()?;
    let config = Config::from_file(&config_path)
        .map_err(map_err!("error reading configuration file: {}"))?;

    let mut runtime = Runtime::new()
        .map_err(map_err!("error creating Heph runtime: {}"))?
        .use_all_cores();

    let actor_ref = runtime.spawn(
        NoSupervisor,
        signal_handler as fn(_) -> _,
        (),
        ActorOptions::default().with_priority(Priority::HIGH),
    );
    runtime.receive_signals(actor_ref);

    info!("opening database: path=\"{}\"", config.path.display());
    let db_ref =
        db::start(&mut runtime, config.path).map_err(map_err!("error opening database: {}"))?;

    let start_refs = Arc::new(RwLock::new(ActorGroup::empty()));
    // Latch used by the synchronisation peer (see `peer::sync::actor`) to wait
    // until all worker threads are started and all peers are connected.
    let start_sync = Arc::new(CountDownLatch::new(runtime.get_threads()));
    let (peers, delay_start) = if let Some(distributed_config) = config.distributed {
        info!(
            "listening on {} for peer connections",
            distributed_config.peer_address
        );
        info!("connecting to peers: {}", distributed_config.peers);
        info!("blob replication method: {}", distributed_config.replicas);
        let peers = peer::start(
            &mut runtime,
            distributed_config,
            db_ref.clone(),
            start_refs.clone(),
            start_sync.clone(),
        )
        .map_err(map_err!("error setting up peer actors: {}"))?;
        (peers, true)
    } else {
        (Peers::empty(), false)
    };

    let start_listener = http::setup(config.http.address, db_ref, start_refs, peers, delay_start)
        .map_err(map_err!("error binding HTTP server: {}"))?;

    runtime
        .with_setup(move |mut runtime_ref| {
            let res = start_listener(&mut runtime_ref);
            start_sync.decrease();
            res
        })
        .start()
        .map_err(map_err!("{}"))
}

/// Actor that waits for a signal and prints a message.
async fn signal_handler(mut ctx: actor::Context<Signal, ThreadSafe>) -> Result<(), !> {
    let signal = ctx.receive_next().await;
    match signal {
        Signal::Interrupt | Signal::Terminate | Signal::Quit => {
            info!("received {:#} signal, shutting down", signal);
            Ok(())
        }
    }
}

/// Parses the arguments.
/// Handles `-v` & `--version`, `-h` & `--help` and unknown arguments.
fn parse_args() -> Result<String, ExitCode> {
    match env::args().nth(1) {
        Some(arg) if arg == "-v" || arg == "--version" => {
            if let Some(commit_version) = COMMIT_VERSION {
                println!("Stored v{} ({})", VERSION, commit_version);
            } else {
                println!("Stored v{}", VERSION);
            }
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
