//! Configuration.
//!
//! See [`Config`].

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::{fmt, io};

use serde_core::de::{self, Deserialize, Deserializer, MapAccess, Visitor};

/// Configuration of the store.
#[derive(Debug)]
pub struct Config {
    /// Number of worker threads to use.
    pub worker_threads: WorkerThreads,
    /// Storage configuration.
    pub storage: Storage,
    /// Hypertext Transfer Protocol (HTTP).
    pub http: Option<Protocol>,
    /// Redis Serialization Protocol (RESP).
    pub resp: Option<Protocol>,
}

/// Number of worker threads to use.
#[derive(Debug)]
pub enum WorkerThreads {
    /// Uses one worker thread per available CPU core.
    Auto,
    /// Use a specific number of threads.
    Specific(usize),
}

/// Configuration related to storage.
#[derive(Debug)]
pub struct Storage {
    pub kind: StorageKind,
    /// Maximum size of a blob in bytes.
    pub max_blob_size: u64,
}

/// Storage kind used.
#[derive(Debug)]
pub enum StorageKind {
    /// In-memory only.
    InMemory,
    /// On-disk storage.
    OnDisk(PathBuf),
}

/// Protocol listeners.
#[derive(Clone, Debug)]
pub struct Protocol {
    /// Address to accept connections on.
    pub address: SocketAddr,
    /// Read timeout.
    pub read_timeout: Duration,
    /// Write timeout.
    pub write_timeout: Duration,
    /// Maximum size of a blob in bytes.
    pub max_blob_size: u64,
}

impl Config {
    /// Read a configuration from `path`.
    pub fn read_from_path(path: &Path) -> io::Result<Config> {
        let config = std::fs::read_to_string(path)?;
        let config: Config = basic_toml::from_str(&config)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
        if config.http.is_none() && config.resp.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "missing listener configuration, please configure `http` or `resp`",
            ))?;
        }
        Ok(config)
    }
}

const READ_TIMEOUT: Duration = Duration::from_secs(60);
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_MAX_BLOB_SIZE: u64 = 100 * 1000000; // 100 mb.

impl Default for Config {
    fn default() -> Config {
        Config {
            worker_threads: WorkerThreads::Specific(1),
            storage: Storage {
                kind: StorageKind::InMemory,
                max_blob_size: DEFAULT_MAX_BLOB_SIZE,
            },
            http: Some(Protocol {
                address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5080),
                read_timeout: READ_TIMEOUT,
                write_timeout: WRITE_TIMEOUT,
                max_blob_size: DEFAULT_MAX_BLOB_SIZE,
            }),
            resp: Some(Protocol {
                address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5378),
                read_timeout: READ_TIMEOUT,
                write_timeout: WRITE_TIMEOUT,
                max_blob_size: DEFAULT_MAX_BLOB_SIZE,
            }),
        }
    }
}

// NOTE: we manually implement Deserialize because deriving it means we have to
// add syn, quote and a couple of other crates that slow down builds times quite
// a bit.

impl<'de> Deserialize<'de> for Config {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ConfigVisitor;

        impl<'de> Visitor<'de> for ConfigVisitor {
            type Value = Config;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct Config")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Config, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut worker_threads = None;
                let mut storage: Option<Storage> = None;
                let mut http: Option<Protocol> = None;
                let mut resp: Option<Protocol> = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::WorkerThreads => {
                            if worker_threads.is_some() {
                                return Err(de::Error::duplicate_field("worker_threads"));
                            }
                            worker_threads = Some(map.next_value()?);
                        }
                        Field::Storage => {
                            if storage.is_some() {
                                return Err(de::Error::duplicate_field("storage"));
                            }
                            storage = Some(map.next_value()?);
                        }
                        Field::Http => {
                            if http.is_some() {
                                return Err(de::Error::duplicate_field("http"));
                            }
                            http = Some(map.next_value()?);
                        }
                        Field::Resp => {
                            if resp.is_some() {
                                return Err(de::Error::duplicate_field("resp"));
                            }
                            resp = Some(map.next_value()?);
                        }
                    }
                }
                let storage = storage.ok_or_else(|| de::Error::missing_field("storage"))?;
                for protocol in [&mut http, &mut resp] {
                    if let Some(protocol) = protocol.as_mut() {
                        protocol.max_blob_size = storage.max_blob_size;
                    }
                }
                Ok(Config {
                    worker_threads: worker_threads.unwrap_or(WorkerThreads::Specific(1)),
                    storage,
                    http,
                    resp,
                })
            }
        }

        const CONFIG_FIELDS: &[&str] = &["worker_threads", "storage", "http", "resp"];

        enum Field {
            WorkerThreads,
            Storage,
            Http,
            Resp,
        }

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str("`worker_threads`, `storage`, `http` or `resp`")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Field, E>
                    where
                        E: de::Error,
                    {
                        match value {
                            "worker_threads" => Ok(Field::WorkerThreads),
                            "storage" => Ok(Field::Storage),
                            "http" => Ok(Field::Http),
                            "resp" => Ok(Field::Resp),
                            _ => Err(de::Error::unknown_field(value, CONFIG_FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        deserializer.deserialize_struct("Config", CONFIG_FIELDS, ConfigVisitor)
    }
}

impl<'de> Deserialize<'de> for WorkerThreads {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct WorkerThreadsVisitor;

        impl<'de> Visitor<'de> for WorkerThreadsVisitor {
            type Value = WorkerThreads;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("an integer or `auto`")
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(WorkerThreads::Specific(v.try_into().unwrap_or(0)))
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(WorkerThreads::Specific(v as usize))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if v == "auto" {
                    Ok(WorkerThreads::Auto)
                } else {
                    Err(E::invalid_value(de::Unexpected::Str(v), &"`auto`"))
                }
            }
        }

        deserializer.deserialize_any(WorkerThreadsVisitor)
    }
}

impl<'de> Deserialize<'de> for Storage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StorageVisitor;

        impl<'de> Visitor<'de> for StorageVisitor {
            type Value = Storage;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct Storage")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Storage, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut kind = None;
                let mut path = None;
                let mut max_blob_size: Option<human_size::Size> = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Kind => {
                            if kind.is_some() {
                                return Err(de::Error::duplicate_field("kind"));
                            }
                            kind = Some(map.next_value()?);
                        }
                        Field::Path => {
                            if path.is_some() {
                                return Err(de::Error::duplicate_field("path"));
                            }
                            path = Some(map.next_value()?);
                        }
                        Field::MaxBlobSize => {
                            if max_blob_size.is_some() {
                                return Err(de::Error::duplicate_field("max_blob_size"));
                            }
                            max_blob_size = Some(map.next_value()?);
                        }
                    }
                }
                let kind = kind.ok_or_else(|| de::Error::missing_field("kind"))?;
                let kind = match kind {
                    Kind::InMemory => StorageKind::InMemory,
                    Kind::OnDisk => {
                        let path = path.ok_or_else(|| de::Error::missing_field("path"))?;
                        StorageKind::OnDisk(path)
                    }
                };
                let max_blob_size = match max_blob_size {
                    Some(size) => size.to_bytes(),
                    None => DEFAULT_MAX_BLOB_SIZE,
                };
                Ok(Storage {
                    kind,
                    max_blob_size,
                })
            }
        }

        const STORAGE_FIELDS: &[&str] = &["kind", "path", "max_blob_size"];

        enum Field {
            Kind,
            Path,
            MaxBlobSize,
        }

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str("`kind` or `path`")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Field, E>
                    where
                        E: de::Error,
                    {
                        match value {
                            "kind" => Ok(Field::Kind),
                            "path" => Ok(Field::Path),
                            "max_blob_size" => Ok(Field::MaxBlobSize),
                            _ => Err(de::Error::unknown_field(value, STORAGE_FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        const STORAGE_KINDS: &[&str] = &["memory", "disk"];

        enum Kind {
            InMemory,
            OnDisk,
        }

        impl<'de> Deserialize<'de> for Kind {
            fn deserialize<D>(deserializer: D) -> Result<Kind, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct StorageKindVisitor;

                impl<'de> Visitor<'de> for StorageKindVisitor {
                    type Value = Kind;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str("`memory` or `disk`")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Kind, E>
                    where
                        E: de::Error,
                    {
                        match value {
                            "memory" => Ok(Kind::InMemory),
                            "disk" => Ok(Kind::OnDisk),
                            _ => Err(de::Error::unknown_field(value, STORAGE_KINDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(StorageKindVisitor)
            }
        }

        deserializer.deserialize_struct("Storage", STORAGE_FIELDS, StorageVisitor)
    }
}

impl<'de> Deserialize<'de> for Protocol {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        const FIELDS: &[&str] = &["address"];

        struct ProtocolVisitor;

        impl<'de> Visitor<'de> for ProtocolVisitor {
            type Value = Protocol;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct Protocol")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Protocol, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut address = None;
                let mut read_timeout = None;
                let mut write_timeout = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Address => {
                            if address.is_some() {
                                return Err(de::Error::duplicate_field("address"));
                            }
                            address = Some(map.next_value()?);
                        }
                        Field::ReadTimeout => {
                            if read_timeout.is_some() {
                                return Err(de::Error::duplicate_field("read_timeout"));
                            }
                            read_timeout = Some(map.next_value::<DurationWrapper>()?.0);
                        }
                        Field::WriteTimeout => {
                            if write_timeout.is_some() {
                                return Err(de::Error::duplicate_field("write_timeout"));
                            }
                            write_timeout = Some(map.next_value::<DurationWrapper>()?.0);
                        }
                    }
                }
                let address = address.ok_or_else(|| de::Error::missing_field("address"))?;
                Ok(Protocol {
                    address,
                    read_timeout: read_timeout.unwrap_or(READ_TIMEOUT),
                    write_timeout: write_timeout.unwrap_or(WRITE_TIMEOUT),
                    // NOTE: overwritten based on the Storage config.
                    max_blob_size: 0,
                })
            }
        }

        enum Field {
            Address,
            ReadTimeout,
            WriteTimeout,
        }

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str("`address`")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Field, E>
                    where
                        E: de::Error,
                    {
                        match value {
                            "address" => Ok(Field::Address),
                            "read_timeout" => Ok(Field::ReadTimeout),
                            "write_timeout" => Ok(Field::WriteTimeout),
                            _ => Err(de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        deserializer.deserialize_struct("Protocol", FIELDS, ProtocolVisitor)
    }
}

/// Wrapper around [`Duration`] to ensure that serde/toml reports the line and column.
struct DurationWrapper(Duration);

impl<'de> Deserialize<'de> for DurationWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DurationVisitor;

        impl<'de> Visitor<'de> for DurationVisitor {
            type Value = Duration;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("`address`")
            }

            fn visit_str<E>(self, value: &str) -> Result<Duration, E>
            where
                E: de::Error,
            {
                const EXPECTED: &str = "a duration";
                let (digits, multipler) = if let Some(ms_duration) = value.strip_suffix("ms") {
                    (ms_duration.trim(), 1) // Number of milliseconds in a millisecond, so 1.
                } else if let Some(sec_duration) = value.strip_suffix('s') {
                    (sec_duration.trim(), 1_000) // Number of milliseconds in a second.
                } else {
                    return Err(E::invalid_value(de::Unexpected::Str(value), &EXPECTED));
                };
                let digits: u64 = digits
                    .parse()
                    .map_err(|_| E::invalid_value(de::Unexpected::Str(value), &EXPECTED))?;
                Ok(Duration::from_secs(digits.saturating_mul(multipler)))
            }
        }

        deserializer
            .deserialize_str(DurationVisitor)
            .map(DurationWrapper)
    }
}
