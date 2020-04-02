use std::hash::{Hash, Hasher};

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use stored::Key;

/// Empty key, all zeros.
const EMPTY_KEY: Key = Key::new([0; 64]);
/// Key for "hello world".
const HELLO_KEY: Key = Key::new([
    48, 158, 204, 72, 156, 18, 214, 235, 76, 196, 15, 80, 201, 2, 242, 180, 208, 237, 119, 238, 81,
    26, 124, 122, 155, 205, 60, 168, 109, 76, 216, 111, 152, 157, 211, 91, 197, 255, 73, 150, 112,
    218, 52, 37, 91, 69, 176, 207, 216, 48, 232, 31, 96, 93, 207, 125, 197, 84, 46, 147, 174, 156,
    215, 111,
]);

pub fn ahash(c: &mut Criterion) {
    use ahash::AHasher;
    let mut group = c.benchmark_group("ahash");
    group.bench_function("AHasher/empty", |b| b.iter(|| bench_empty::<AHasher>()));
    group.bench_function("AHasher/hello", |b| b.iter(|| bench_hello::<AHasher>()));
    group.finish();
}

pub fn fxhash(c: &mut Criterion) {
    use fxhash::{FxHasher, FxHasher32, FxHasher64};
    let mut group = c.benchmark_group("fxhash");

    group.bench_function("FxHasher/empty", |b| b.iter(|| bench_empty::<FxHasher>()));
    group.bench_function("FxHasher/hello", |b| b.iter(|| bench_hello::<FxHasher>()));

    group.bench_function("FxHasher32/empty", |b| {
        b.iter(|| bench_empty::<FxHasher32>())
    });
    group.bench_function("FxHasher32/hello", |b| {
        b.iter(|| bench_hello::<FxHasher32>())
    });

    group.bench_function("FxHasher64/empty", |b| {
        b.iter(|| bench_empty::<FxHasher64>())
    });
    group.bench_function("FxHasher64/hello", |b| {
        b.iter(|| bench_hello::<FxHasher64>())
    });

    group.finish();
}

pub fn rustc_hash(c: &mut Criterion) {
    use rustc_hash::FxHasher;
    let mut group = c.benchmark_group("rustc_hash");
    group.bench_function("FxHasher/empty", |b| b.iter(|| bench_empty::<FxHasher>()));
    group.bench_function("FxHasher/hello", |b| b.iter(|| bench_hello::<FxHasher>()));
    group.finish();
}

pub fn seahash(c: &mut Criterion) {
    use seahash::SeaHasher;
    let mut group = c.benchmark_group("seahash");
    group.bench_function("SeaHasher/empty", |b| b.iter(|| bench_empty::<SeaHasher>()));
    group.bench_function("SeaHasher/hello", |b| b.iter(|| bench_hello::<SeaHasher>()));
    group.finish();
}

macro_rules! make_bench {
    ($name: ident, $key: ident) => {
        #[inline(always)]
        fn $name<H>() -> u64
        where
            H: Hasher + Default,
        {
            let mut hasher = H::default();
            $key.hash(&mut hasher);
            black_box(hasher.finish())
        }
    };
}

make_bench!(bench_empty, EMPTY_KEY);
make_bench!(bench_hello, HELLO_KEY);

criterion_group!(hash_benches, ahash, fxhash, rustc_hash, seahash);
criterion_main!(hash_benches);
