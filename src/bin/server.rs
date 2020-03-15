#![feature(never_type)]

use std::io;

use heph::{Runtime, RuntimeError, RuntimeRef};
use log::info;

use stored::server::{cache, listener};

use cache::Cache;

fn main() -> Result<(), RuntimeError<io::Error>> {
    heph::log::init();

    let mut runtime = Runtime::new();

    let cache = cache::start(&mut runtime).map_err(RuntimeError::map_type)?;
    let options = Options { cache };

    runtime
        .with_setup(move |runtime_ref| setup(runtime_ref, options))
        //.use_all_cores() // FIXME: need to create listener before setup.
        .start()
        .map_err(|err| err.into())
}

#[derive(Clone)]
struct Options {
    cache: Cache,
}

fn setup(mut runtime_ref: RuntimeRef, options: Options) -> io::Result<()> {
    // TODO: read this from a config file or something.
    let address = "127.0.0.1:1234".parse().unwrap();
    let listener_options = listener::Options {
        cache: options.cache,
        address,
    };

    listener::setup(&mut runtime_ref, listener_options)?;
    info!("listening on address: {}", address);

    Ok(())
}

/*
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
*/
