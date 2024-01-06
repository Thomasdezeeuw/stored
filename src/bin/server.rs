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

fn main() -> ExitCode {
    std_logger::Config::logfmt().init();

    match try_main() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            error!("{err}");
            ExitCode::FAILURE
        }
    }
}

fn try_main() -> Result<(), heph_rt::Error> {
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
