use std::io;
use std::process::ExitCode;

use heph::actor::NewActor;
use heph_rt::net::tcp::server::TcpServer;
use heph_rt::spawn::options::{ActorOptions, FutureOptions, Priority};
use heph_rt::Runtime;

use stored::controller;
use stored::protocol::Resp;
use stored::storage::{self, mem};

fn main() -> ExitCode {
    std_logger::Config::logfmt().init();

    match try_main() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            log::error!("{err}");
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

    let (storage_handle, future) = storage::new_in_memory();
    runtime.spawn_future(
        future,
        FutureOptions::default().with_priority(Priority::HIGH),
    );

    // TODO: get from arguments/configuration.
    let address = "127.0.0.1:6378".parse().unwrap();

    runtime.run_on_workers(move |mut runtime_ref| -> io::Result<()> {
        let storage = mem::Storage::from(storage_handle);
        let new_actor =
            (controller::actor as fn(_, _, _, _, _) -> _).map_arg(move |(stream, address)| {
                let config = controller::DefaultConfig;
                let protocol = Resp::new(stream);
                (config, protocol, storage.clone(), address)
            });
        let supervisor = controller::supervisor;
        let options = ActorOptions::default();
        let server = TcpServer::setup(address, supervisor, new_actor, options)?;

        let supervisor = ServerSupervisor::new();
        let options = ActorOptions::default().with_priority(Priority::LOW);
        let actor_ref = runtime_ref.try_spawn_local(supervisor, server, (), options)?;
        runtime_ref.receive_signals(actor_ref.try_map());
        Ok(())
    })?;

    log::info!(address = log::as_display!(address); "listening");
    runtime.start()
}

heph::restart_supervisor!(ServerSupervisor, "TCP server", ());
