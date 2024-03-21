#![feature(never_type)]

use std::path::Path;
use std::process::ExitCode;
use std::{env, io};

use heph::actor::{self, actor_fn, NewActor};
use heph::supervisor::NoSupervisor;
use heph_rt::net::{tcp, TcpStream};
use heph_rt::spawn::options::{ActorOptions, FutureOptions, Priority};
use heph_rt::{Runtime, Signal, ThreadSafe};
use log::{error, info};

use stored::config::{self, Config};
use stored::controller;
use stored::protocol::{Http, Protocol, Resp};
use stored::storage::{self, disk, mem};

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

    let config = match parse_args() {
        Ok(Some(conf_path)) => match Config::read_from_path(Path::new(&conf_path)) {
            Ok(config) => config,
            Err(err) => {
                error!("failed to process configuration file '{conf_path}': {err}",);
                return ExitCode::FAILURE;
            }
        },
        Ok(None) => Config::default(),
        Err(code) => return code,
    };

    match run(config) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            error!("{err}");
            ExitCode::FAILURE
        }
    }
}

fn parse_args() -> Result<Option<String>, ExitCode> {
    match env::args().nth(1) {
        Some(arg) if arg == "-v" || arg == "--version" => {
            println!("Stored v{VERSION}");
            Err(ExitCode::SUCCESS)
        }
        Some(arg) if arg == "-h" || arg == "--help" => {
            println!("{HELP}");
            Err(ExitCode::SUCCESS)
        }
        Some(arg) if arg.starts_with('-') => {
            eprintln!("Unknown argument '{arg}'.\n\n{USAGE}");
            Err(ExitCode::FAILURE)
        }
        Some(config_path) => Ok(Some(config_path)),
        None => Ok(None),
    }
}

fn run(config: Config) -> Result<(), heph_rt::Error> {
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

    match config.storage {
        config::Storage::InMemory => {
            let (storage_handle, future) = storage::new_in_memory();
            runtime.spawn_future(
                future,
                FutureOptions::default().with_priority(Priority::HIGH),
            );
            start_listeners!(mem::Storage, config, runtime, storage_handle);
        }
        config::Storage::OnDisk(path) => {
            let rt = ThreadSafe::from(&runtime);
            let (storage_handle, future) = storage::open_on_disk(rt, path)
                .map_err(|err| heph_rt::Error::setup(format!("failed to open storage: {err}")))?;
            runtime.spawn_future(
                future,
                FutureOptions::default().with_priority(Priority::HIGH),
            );
            start_listeners!(disk::Storage, config, runtime, storage_handle);
        }
    };

    runtime.start()
}

async fn signal_handler<RT>(mut ctx: actor::Context<Signal, RT>) -> Result<(), !> {
    while let Ok(signal) = ctx.receive_next().await {
        if signal.should_stop() {
            info!(signal:% = signal; "received shut down signal, waiting on all connections to close before shutting down");
            break;
        }
    }
    Ok(())
}

// NOTE: this should have been a function, but the traits were too complex.
macro_rules! start_listeners (
    (
        $storage: ty, // Storage.
        $config: ident, // Config.
        $runtime: ident, // &mut heph_rt::Runtime.
        $storage_handle: ident $(,)? // Into<Storage> + Send.
    ) => {
        if let Some(config) = $config.resp {
            let storage_handle = $storage_handle.clone();
            #[rustfmt::skip]
            start_listener!(Resp<TcpStream>, $storage, $runtime, storage_handle, tcp::server::setup, config, RespSupervisor);
        }

        if let Some(config) = $config.http {
            let storage_handle = $storage_handle.clone();
            #[rustfmt::skip]
            start_listener!(Http, $storage, $runtime, storage_handle, heph_http::server::setup, config, HttpSupervisor);
        }
    }
);

// NOTE: this should have been a function, but the traits were too complex.
macro_rules! start_listener (
    (
        $protocol: ty, // Protocol.
        $storage: ty, // Storage.
        $runtime: ident, // &mut heph_rt::Runtime.
        $storage_handle: ident, // Into<Storage> + Send.
        $start_listener: path, // Function to start listener.
        $config: ident, // config::Protocol.
        $listener_supervisor: ident $(,)? // fn new() -> Supervisor<Listener>.
    ) => {
        let controller_config = controller::Config {
            read_timeout: $config.read_timeout,
            write_timeout: $config.write_timeout,
        };
        let new_actor = actor_fn(controller::actor::<_, $storage, _>).map_arg(move |conn| {
            let protocol = <$protocol>::new(conn);
            let storage = $storage_handle.clone().into();
            (controller_config.clone(), protocol, storage)
        });
        let options = ActorOptions::default();
        let server = $start_listener($config.address, controller::supervisor, new_actor, options).map_err(|err| {
            heph_rt::Error::setup(format!("failed to setup {} server: {err}", <$protocol>::NAME))
        })?;

        $runtime.run_on_workers(move |mut runtime_ref| -> io::Result<()> {
            let supervisor = $listener_supervisor::new();
            let options = ActorOptions::default().with_priority(Priority::LOW);
            let actor_ref = runtime_ref.spawn_local(supervisor, server, (), options);
            runtime_ref.receive_signals(actor_ref.try_map());
            Ok(())
        })?;

        info!(address:% = $config.address; "listening for {} requests", <$protocol>::NAME);
    }
);

use {start_listener, start_listeners};

heph::restart_supervisor!(RespSupervisor, "RESP server");
heph::restart_supervisor!(HttpSupervisor, "HTTP server");
