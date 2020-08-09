//! Tests of the `store`, `retrieve` and `remove` CLI tools.
//!
//! Single port 9000.

#![feature(bool_to_option)]

use std::process::{Command, Output};
use std::str;
use std::sync::Once;

use log::LevelFilter;

#[macro_use]
mod util;

#[allow(dead_code)] // FIXME: use this along with the tests.
const DB_PORT: u16 = 9000;
const DB_PATH: &str = "/tmp/stored/cli_tests.db";
const CONF_PATH: &str = "tests/config/cli.toml";
const FILTER: LevelFilter = LevelFilter::Warn;

start_stored_fn!(&[CONF_PATH], &[DB_PATH], FILTER);

fn test(bin: &str, args: &[&str], want: &[u8]) {
    let output = run(bin, args);
    assert!(
        output.status.success(),
        "unexpected exit status: {}, output: {:?}",
        output.status,
        str::from_utf8(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "unexpected output on standard err: {:?}",
        str::from_utf8(&output.stderr)
    );
    assert_eq!(output.stdout, want);
}

/// Build and run `bin`.
fn run(bin: &str, args: &[&str]) -> Output {
    build(bin);
    let bin = format!("target/debug/{}", bin);
    Command::new(bin).args(args).output().unwrap()
}

/// Build `bin`ary using `cargo build --bin bin`.
fn build(bin: &str) {
    static BUILDS: [(Once, &str); 3] = [
        (Once::new(), "store"),
        (Once::new(), "retrieve"),
        (Once::new(), "remove"),
    ];
    static mut BUILD_SUCCESS: [bool; 3] = [false; 3];

    assert_eq!(BUILDS.len(), unsafe { BUILD_SUCCESS.len() });
    let (index, build) = BUILDS
        .iter()
        .enumerate()
        .find_map(|(i, (b, name))| (*name == bin).then_some((i, b)))
        .expect("couldn't find binary");

    build.call_once(|| {
        let output = Command::new("cargo")
            .args(&["build", "--bin", bin])
            .output()
            .expect("unable to build binary");

        if !output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            panic!("failed to build example: {}\n\n{}", stdout, stderr);
        }

        unsafe { BUILD_SUCCESS[index] = true }
    });
    assert!(unsafe { BUILD_SUCCESS[index] }, "build failed");
}

#[test]
#[ignore = "pass the server's address to the binary"]
fn store_argument() {
    let _p = start_stored();

    let want = b"b7f783baed8297f0db917462184ff4f08e69c2d5e5f79a942600f9725f58ce1f29c18139bf80b06c0fff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47";
    test("store", &["Hello world"], want);

    let want = b"b09bcc84b88e440dad90bb19baf0c0216d8929baebc785fa0e387a17c46fe131f45109b5f06a632781c5ecf1bf1257c205bbea6d3651a9364a7fc6048cdc155c";
    test("store", &["Hello mars"], want);
}
