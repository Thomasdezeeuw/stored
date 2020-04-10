//! Validate a store
//!
//! Usage:
//!
//! ```bash
//! $ validate_store path/to/store.db
//! ```

#![feature(bool_to_option)]

use std::num::NonZeroUsize;
use std::{env, process};

use stored::storage::validate;

fn main() {
    let db_path = env::args().nth(1).unwrap_or_else(|| {
        eprintln!(
            "Missing path to configuration file.\nUsage:\n\tvalidate_store <path_to_database>"
        );
        process::exit(1);
    });

    let corruptions = validate(&db_path, NonZeroUsize::new(num_cpus::get()).unwrap())
        .unwrap_or_else(|err| {
            eprintln!("Error validating database: {}", err);
            process::exit(1);
        });

    if !corruptions.is_empty() {
        eprintln!(
            "Found {} corruption(s) in '{}'. Corrupted keys:",
            corruptions.len(),
            db_path
        );
        for corruption in corruptions {
            eprintln!("{}", corruption.key());
        }
        process::exit(1);
    } else {
        println!("OK.");
    }
}
