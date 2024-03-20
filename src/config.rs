//! Configuration.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::{fmt, io};

use serde::de::{self, Deserialize, Deserializer, MapAccess, Visitor};

/// Configuration of the store.
pub struct Config {
    pub storage: Storage,
    /// Hypertext Transfer Protocol (HTTP).
    pub http: Option<Protocol>,
    /// Redis Serialization Protocol (RESP).
    pub resp: Option<Protocol>,
}

/// Storage type used.
pub enum Storage {
    /// In-memory only.
    InMemory,
    /// On-disk storage.
    OnDisk(PathBuf),
}

#[derive(Clone)]
pub struct Protocol {
    /// Address to accept connections on.
    pub address: SocketAddr,
}

impl Config {
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

impl Default for Config {
    fn default() -> Config {
        Config {
            storage: Storage::InMemory,
            http: Some(Protocol {
                address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5080),
            }),
            resp: Some(Protocol {
                address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5378),
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
                let mut storage = None;
                let mut http = None;
                let mut resp = None;
                while let Some(key) = map.next_key()? {
                    match key {
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
                Ok(Config {
                    storage,
                    http,
                    resp,
                })
            }
        }

        const CONFIG_FIELDS: &[&str] = &["storage", "http", "resp"];

        enum Field {
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
                        formatter.write_str("`storage`, `http` or `resp`")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Field, E>
                    where
                        E: de::Error,
                    {
                        match value {
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
                    }
                }
                let kind = kind.ok_or_else(|| de::Error::missing_field("kind"))?;
                match kind {
                    StorageKind::InMemory => Ok(Storage::InMemory),
                    StorageKind::OnDisk => {
                        let path = path.ok_or_else(|| de::Error::missing_field("path"))?;
                        Ok(Storage::OnDisk(path))
                    }
                }
            }
        }

        const STORAGE_FIELDS: &[&str] = &["kind", "path"];

        enum Field {
            Kind,
            Path,
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
                            _ => Err(de::Error::unknown_field(value, STORAGE_FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        const STORAGE_KINDS: &[&str] = &["memory", "disk"];

        enum StorageKind {
            InMemory,
            OnDisk,
        }

        impl<'de> Deserialize<'de> for StorageKind {
            fn deserialize<D>(deserializer: D) -> Result<StorageKind, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct StorageKindVisitor;

                impl<'de> Visitor<'de> for StorageKindVisitor {
                    type Value = StorageKind;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str("`memory` or `disk`")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<StorageKind, E>
                    where
                        E: de::Error,
                    {
                        match value {
                            "memory" => Ok(StorageKind::InMemory),
                            "disk" => Ok(StorageKind::OnDisk),
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
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Address => {
                            if address.is_some() {
                                return Err(de::Error::duplicate_field("address"));
                            }
                            address = Some(map.next_value()?);
                        }
                    }
                }
                let address = address.ok_or_else(|| de::Error::missing_field("address"))?;
                Ok(Protocol { address })
            }
        }

        enum Field {
            Address,
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
