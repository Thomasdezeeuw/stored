use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::panic::Location;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Once;
use std::thread::sleep;
use std::time::{Duration, Instant};

use heph_rt::access::{Access, ThreadSafe};
use heph_rt::spawn::options::FutureOptions;
use serde_core::de::{self, Deserialize, Deserializer, MapAccess, Visitor};
use store::Client;

mod smoke;

fn main() -> Result<(), heph_rt::Error> {
    std_logger::Config::logfmt().init();

    let mut runtime = heph_rt::Runtime::setup()
        .with_name("stored".to_owned())
        .auto_cpu_affinity()
        .use_all_cores()
        .build()?;

    let rt = ThreadSafe::from(&runtime);
    runtime.spawn_future(smoke::run(rt), FutureOptions::default());

    runtime.start()
}

// NOTE: `#[track_caller]` doesn't work for `async` functions at the time of
// writing, hence we return `impl Future`.
#[track_caller]
fn start_process<RT: Access>(rt: &RT) -> impl Future<Output = (Client, Stored)> {
    let (config_path, address) = config(Location::caller());

    let mut stored = start_with_config(&config_path);
    wait_until_started(&mut stored, address);

    async move {
        let client = Client::connect(rt, address)
            .await
            .unwrap_or_else(|err| panic!("failed to connect to stored: {err}"));
        (client, stored)
    }
}

fn config(caller: &'static Location) -> (PathBuf, SocketAddr) {
    // Determine the config path based on the caller's file.
    let config_filename = Path::new(caller.file()).file_name().unwrap();
    let mut config_path = PathBuf::new();
    config_path.push("configs");
    config_path.push(Path::new(config_filename));
    config_path.set_extension("toml");

    // Read and parse the config to ensure it's correct and extract the HTTP
    // address from it.
    let config_file = std::fs::read(&config_path).unwrap_or_else(|err| {
        panic!(
            "failed to read configuration file at '{}': {err}",
            config_path.display()
        )
    });
    let config: ConfigHttpAddress = basic_toml::from_slice(&config_file).unwrap_or_else(|err| {
        panic!(
            "failed to parse configuration file at '{}': {err}",
            config_path.display()
        )
    });

    (config_path, config.0)
}

/// Config implementation that extracts the HTTP address.
// Not using the config type from stored so we don't have to recompile the tests
// whenever stored's code changes.
struct ConfigHttpAddress(SocketAddr);

// Manual implementation so we don't have to pull in the derive macro from
// serde, which is not fast to compile.
impl<'de> Deserialize<'de> for ConfigHttpAddress {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ConfigVisitor;

        impl<'de> Visitor<'de> for ConfigVisitor {
            type Value = ConfigHttpAddress;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct Config")
            }

            fn visit_map<V>(self, mut map: V) -> Result<ConfigHttpAddress, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut address = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        "resp" => {
                            let value: ProtocolAddress = map.next_value()?;
                            address = Some(ConfigHttpAddress(value.0));
                        }
                        _ => _ = map.next_value::<serde::de::IgnoredAny>(),
                    }
                }
                address.ok_or_else(|| de::Error::missing_field("resp"))
            }
        }

        struct ProtocolAddress(SocketAddr);

        impl<'de> Deserialize<'de> for ProtocolAddress {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct ProtocolVisitor;

                impl<'de> Visitor<'de> for ProtocolVisitor {
                    type Value = ProtocolAddress;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str("struct Protocol")
                    }

                    fn visit_map<V>(self, mut map: V) -> Result<ProtocolAddress, V::Error>
                    where
                        V: MapAccess<'de>,
                    {
                        let mut address = None;
                        while let Some(key) = map.next_key()? {
                            match key {
                                "address" => {
                                    address = Some(ProtocolAddress(map.next_value()?));
                                }
                                _ => _ = map.next_value::<serde::de::IgnoredAny>(),
                            }
                        }
                        address.ok_or_else(|| de::Error::missing_field("address"))
                    }
                }

                const PROTOCOL_FIELDS: &[&str] = &["address"];
                deserializer.deserialize_struct("Protocol", PROTOCOL_FIELDS, ProtocolVisitor)
            }
        }

        const CONFIG_FIELDS: &[&str] = &["resp"];
        deserializer.deserialize_struct("Config", CONFIG_FIELDS, ConfigVisitor)
    }
}

fn start_with_config(config_path: &Path) -> Stored {
    ensure_build();

    let process = Command::new("../target/debug/stored")
        .arg(config_path)
        .env("LOG", "WARN") // Less logging.
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap_or_else(|err| panic!("failed to start stored: {err}"));
    Stored { process }
}

fn ensure_build() {
    static BUILD: Once = Once::new();
    BUILD.call_once(|| {
        Command::new("cargo")
            .arg("build")
            .current_dir("../")
            .status()
            .unwrap_or_else(|err| panic!("failed to build stored: {err}"));
    });
}

struct Stored {
    process: Child,
}

impl Drop for Stored {
    fn drop(&mut self) {
        self.process
            .kill()
            .unwrap_or_else(|err| panic!("failed to stop stored process: {err}"));
        self.process
            .wait()
            .unwrap_or_else(|err| panic!("failed to wait on stored process: {err}"));
    }
}

fn wait_until_started(stored: &mut Stored, address: SocketAddr) {
    const MAX: Duration = Duration::from_secs(1);
    const SLEEP: Duration = Duration::from_millis(100);
    let start = Instant::now();
    loop {
        match std::net::TcpStream::connect(address) {
            Ok(_) => return,
            Err(ref err) if err.kind() == io::ErrorKind::ConnectionRefused => {
                if start.elapsed() + SLEEP > MAX {
                    match stored.process.try_wait() {
                        Ok(Some(_)) => panic!("failed to start stored"),
                        Ok(None) => panic!("waited too long for stored to start"),
                        Err(err) => panic!("error starting stored: {err}"),
                    }
                }
                sleep(SLEEP);
                continue;
            }
            Err(err) => panic!("unexpected error connecting to stored: {err}"),
        }
    }
}
