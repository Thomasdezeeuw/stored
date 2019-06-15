#![feature(never_type, async_await)]

use heph::system::{ActorSystem, ActorSystemRef, RuntimeError};
use std::{fmt, io};

use coeus_common::Key;

mod buffer;
mod cache;
mod listener;

use cache::CacheRef;

fn main() -> Result<(), DisplayAsDebug<RuntimeError<io::Error>>> {
    heph::log::init();

    let mut system = ActorSystem::new();

    let cache_ref = cache::start(&mut system).map_err(RuntimeError::map_type)?;
    let options = Options { cache_ref };

    system
        .with_setup(move |system_ref| setup(system_ref, options))
        .run()
        .map_err(|err| err.into())
}

#[derive(Clone)]
struct Options {
    cache_ref: CacheRef,
}

fn setup(mut system_ref: ActorSystemRef, options: Options) -> io::Result<()> {
    let listener_options = listener::Options {
        cache: options.cache_ref,
        // TODO: read this from a config file or something.
        address: "127.0.0.1:8080".parse().unwrap(),
    };

    listener::setup(&mut system_ref, listener_options)?;

    Ok(())
}

// This is needed because the stupid `Termination` trait used the `fmt::Debug`
// implementation for whatever reason.
struct DisplayAsDebug<T>(T);

impl<T> From<T> for DisplayAsDebug<T> {
    fn from(t: T) -> DisplayAsDebug<T> {
        DisplayAsDebug(t)
    }
}

impl<T> fmt::Debug for DisplayAsDebug<T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}
