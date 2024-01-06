#![feature(never_type)]

use std::process::ExitCode;
use std::{env, io};

use heph::actor::{self, actor_fn, NewActor};
use heph::supervisor::NoSupervisor;
use heph_rt::net::tcp;
use heph_rt::spawn::options::{ActorOptions, FutureOptions, Priority};
use heph_rt::{Runtime, Signal};
use log::{error, info};

use stored::controller;
use stored::protocol::Resp;
use stored::storage::{self, mem};

const VERSION: &str = env!("CARGO_PKG_VERSION");

// NOTE: keep in sync with below `USAGE` text.
const HELP: &str = concat!(
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

// NOTE: keep in sync with above `HELP` text.
const USAGE: &str = "Usage:
    stored <path_to_config>
    stored -v or --version
    stored -h or --help";

fn main() -> ExitCode {
    std_logger::Config::logfmt().init();

    let config_path = match parse_args() {
        Ok(config_path) => config_path,
        Err(code) => return code,
    };

    match try_main(config_path) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            error!("{err}");
            ExitCode::FAILURE
        }
    }
}

fn parse_args() -> Result<String, ExitCode> {
    match env::args().nth(1) {
        Some(arg) if arg == "-v" || arg == "--version" => {
            println!("Stored v{}", VERSION);
            Err(ExitCode::SUCCESS)
        }
        Some(arg) if arg == "-h" || arg == "--help" => {
            println!("{}", HELP);
            Err(ExitCode::SUCCESS)
        }
        Some(arg) if arg.starts_with('-') => {
            eprintln!("Unknown argument '{}'.\n\n{}", arg, USAGE);
            Err(ExitCode::FAILURE)
        }
        Some(config_path) => Ok(config_path),
        None => {
            eprintln!("Missing path to configuration file.\n\n{}", USAGE);
            Err(ExitCode::FAILURE)
        }
    }
}

fn try_main(_config_path: String) -> Result<(), heph_rt::Error> {
    // TODO: read and use configuration file.

    let mut runtime = Runtime::setup()
        .with_name("stored".to_owned())
        .use_all_cores()
        .auto_cpu_affinity()
        .build()?;

    let actor_ref = runtime.spawn(
        NoSupervisor,
        actor_fn(signal_handler),
        (),
        ActorOptions::default().with_priority(Priority::HIGH),
    );
    runtime.receive_signals(actor_ref);

    let (storage_handle, future) = storage::new_in_memory();
    runtime.spawn_future(
        future,
        FutureOptions::default().with_priority(Priority::HIGH),
    );

    // TODO: get from arguments/configuration.
    let address = "127.0.0.1:6378".parse().unwrap();

    runtime.run_on_workers(move |mut runtime_ref| -> io::Result<()> {
        let storage = mem::Storage::from(storage_handle);
        let new_actor = actor_fn(controller::actor).map_arg(move |stream| {
            let config = controller::DefaultConfig;
            let protocol = Resp::new(stream);
            (config, protocol, storage.clone())
        });
        let supervisor = controller::supervisor;
        let options = ActorOptions::default();
        let server = tcp::server::setup(address, supervisor, new_actor, options)?;

        let supervisor = ServerSupervisor::new();
        let options = ActorOptions::default().with_priority(Priority::LOW);
        let actor_ref = runtime_ref.spawn_local(supervisor, server, (), options);
        runtime_ref.receive_signals(actor_ref.try_map());
        Ok(())
    })?;

    info!(address = log::as_display!(address); "listening");
    runtime.start()
}

heph::restart_supervisor!(ServerSupervisor, "TCP server");

async fn signal_handler<RT>(mut ctx: actor::Context<Signal, RT>) -> Result<(), !> {
    while let Ok(signal) = ctx.receive_next().await {
        if signal.should_stop() {
            info!(signal = log::as_display!(signal); "received shut down signal, waiting on all connections to close before shutting down");
            break;
        }
    }
    Ok(())
}
