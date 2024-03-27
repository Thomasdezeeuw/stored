//! Unit tests.

#![feature(noop_waker)]

mod unit {
    pub(super) mod util;

    mod key;
    mod protocol;
    mod storage;
}

pub(crate) use unit::util;
