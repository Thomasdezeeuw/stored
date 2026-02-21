#![feature(never_type)]

use std::net::{SocketAddr, ToSocketAddrs};
use std::pin::Pin;
use std::process::ExitCode;
use std::{env, io};

use heph::actor::{self, Actor, NewActor, actor_fn};
use heph::supervisor::{Supervisor, SupervisorStrategy};
use heph_rt::io::{Read, Write, stderr, stdin, stdout};
use heph_rt::spawn::options::{ActorOptions, InboxSize};
use heph_rt::util::either;
use heph_rt::{Access, Runtime, Signal};

use store::{Client, Key};

const VERSION: &str = env!("CARGO_PKG_VERSION");

// NOTE: keep in sync with below `USAGE` text.
const HELP: &str = concat!(
    "Store v",
    env!("CARGO_PKG_VERSION"),
    "

Store is a client for Stored.

Usage:
    stored <address>
    stored -v or --version
    stored -h or --help"
);

// NOTE: keep in sync with above `HELP` text.
const USAGE: &str = "Usage:
    stored <path_to_config>
    stored -v or --version
    stored -h or --help";

fn main() -> ExitCode {
    let address = match parse_args() {
        Ok(address) => address,
        Err(code) => return code,
    };

    match run(address) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err}");
            ExitCode::FAILURE
        }
    }
}

fn parse_args() -> Result<SocketAddr, ExitCode> {
    match env::args().nth(1) {
        Some(arg) if arg == "-v" || arg == "--version" => {
            println!("Store v{VERSION}");
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
        Some(address) => match address.to_socket_addrs() {
            Ok(mut addresses) => {
                if let Some(address) = addresses.next() {
                    Ok(address)
                } else {
                    eprintln!("Failed to resolve '{address}'");
                    Err(ExitCode::FAILURE)
                }
            }
            Err(err) => {
                eprintln!("Failed to resolve '{address}': {err}");
                Err(ExitCode::FAILURE)
            }
        },
        None => {
            eprintln!("Missing address.\n\n{USAGE}");
            Err(ExitCode::FAILURE)
        }
    }
}

fn run(address: SocketAddr) -> Result<(), heph_rt::Error> {
    let mut runtime = Runtime::setup()
        .with_name("store".to_owned())
        .auto_cpu_affinity()
        .num_threads(1)
        .build()?;

    runtime.run_on_workers(move |mut runtime_ref| {
        let actor_ref = runtime_ref.spawn_local(
            ActorSupervisor { address, count: 0 },
            actor_fn(actor),
            address,
            ActorOptions::default().with_inbox_size(InboxSize::ONE),
        );
        runtime_ref.receive_signals(actor_ref);
        Ok::<(), heph_rt::Error>(())
    })?;

    runtime.start()
}

struct ActorSupervisor {
    address: SocketAddr,
    count: usize,
}

impl<NA> Supervisor<NA> for ActorSupervisor
where
    NA: NewActor<Argument = SocketAddr, Error = !>,
    NA::Actor: Actor<Error = io::Error>,
{
    fn decide(&mut self, err: io::Error) -> SupervisorStrategy<NA::Argument> {
        self.count += 1;
        eprintln!("Failed to run client: {err}");

        match err.kind() {
            io::ErrorKind::ConnectionRefused
            | io::ErrorKind::HostUnreachable
            | io::ErrorKind::NetworkUnreachable
            | io::ErrorKind::NetworkDown => return SupervisorStrategy::Stop,
            _ => {}
        }

        if self.count >= 5 {
            SupervisorStrategy::Stop
        } else {
            SupervisorStrategy::Restart(self.address)
        }
    }

    fn decide_on_restart_error(&mut self, err: !) -> SupervisorStrategy<NA::Argument> {
        // This can't be called.
        err
    }

    fn second_restart_error(&mut self, err: !) {
        // This can't be called.
        err
    }
}

async fn actor<RT: Access>(
    mut ctx: actor::Context<Signal, RT>,
    address: SocketAddr,
) -> io::Result<()> {
    let mut client = Client::connect(ctx.runtime(), address).await?;

    let mut stdin = stdin(ctx.runtime());
    let mut stdout = stdout(ctx.runtime());
    let mut stderr = stderr(ctx.runtime());
    let mut read = stdin.read(Vec::with_capacity(512));
    let mut receive_signal = ctx.receive_next();

    loop {
        // SAFETY: working around an unpinned issue. Not moving `read`, so this
        // is safe.
        let pinned_read = unsafe { Pin::new_unchecked(&mut read) };
        match either(pinned_read, &mut receive_signal).await {
            Ok(Ok(mut buf)) => {
                let mut has_newline = false;
                loop {
                    if let Some(b) = buf.last() {
                        if *b == b'\r' || *b == b'\n' {
                            has_newline = true;
                            _ = buf.pop();
                            continue;
                        }
                    }
                    break;
                }

                if let Some(idx) = buf.iter().position(|b| *b == b' ') {
                    let (cmd, rest) = buf.split_at(idx);
                    let rest = &rest[1..]; // Skip space.

                    match cmd {
                        b"SET" | b"set" => {
                            let blob = rest.into();
                            let key = client.add(blob).await?;
                            stdout.write_vectored_all((key.to_string(), "\n")).await?;
                        }
                        b"DEL" | b"del" => {
                            let key = match Key::try_parse_bytes(rest) {
                                Ok(key) => key,
                                Err(err) => todo!("parse error: {err}"),
                            };
                            if client.remove(&key).await? {
                                stdout.write_all("Blob removed").await?;
                            } else {
                                stderr.write_all("Blob not found").await?;
                            }
                        }
                        b"GET" | b"get" => {
                            let key = match Key::try_parse_bytes(rest) {
                                Ok(key) => key,
                                Err(err) => todo!("parse error: {err}"),
                            };
                            if let Some(blob) = client.get(&key).await? {
                                stdout.write_vectored_all((blob, "\n")).await?;
                            } else {
                                stderr.write_all("Blob not found").await?;
                            }
                        }
                        b"EXISTS" | b"exists" => {
                            let key = match Key::try_parse_bytes(rest) {
                                Ok(key) => key,
                                Err(err) => todo!("parse error: {err}"),
                            };
                            if client.contains(&key).await? {
                                stdout.write_all("Blob exists").await?;
                            } else {
                                stderr.write_all("Blob does not exist").await?;
                            }
                        }
                        b"DBSIZE" | b"dbsize" => {
                            let n = client.blobs_stored().await?;
                            stdout.write_all(format!("{n} blobs stored")).await?;
                        }
                        _ => {
                            eprintln!("Unknown command");
                        }
                    }

                    buf.clear();
                } else {
                    if has_newline {
                        buf.push(b' '); // Let the new line act as space.
                    }
                }

                drop(read);
                read = stdin.read(buf);
            }
            Ok(Err(err)) => return Err(err),
            Err(Ok(signal)) => {
                if signal.should_stop() {
                    return Ok(());
                }
                receive_signal = ctx.receive_next();
            }
            Err(Err(_)) => return Ok(()),
        }
    }
}
