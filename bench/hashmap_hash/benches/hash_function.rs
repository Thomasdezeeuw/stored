use std::hash::{Hash, Hasher};

use criterion::measurement::Measurement;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkGroup, Criterion};
use stored::key::{Key, KeyHasher};

/// Empty key, all zeros.
const EMPTY_KEY: Key = Key::new([0; 64]);
/// Key for "hello world".
const HELLO_KEY: Key = Key::new([
    48, 158, 204, 72, 156, 18, 214, 235, 76, 196, 15, 80, 201, 2, 242, 180, 208, 237, 119, 238, 81,
    26, 124, 122, 155, 205, 60, 168, 109, 76, 216, 111, 152, 157, 211, 91, 197, 255, 73, 150, 112,
    218, 52, 37, 91, 69, 176, 207, 216, 48, 232, 31, 96, 93, 207, 125, 197, 84, 46, 147, 174, 156,
    215, 111,
]);

criterion_main!(hash_benches);
criterion_group!(hash_benches, group1, group2);

fn group1(c: &mut Criterion) {
    let mut group = c.benchmark_group("Hash Key/group1");
    // FxHash is literally on a different time scale.
    fxhash(&mut group);
    keyhash(&mut group);
    group.finish();
}

fn group2(c: &mut Criterion) {
    let mut group = c.benchmark_group("Hash Key/group2");
    std(&mut group);
    ahash(&mut group);
    fnv(&mut group);
    rustc_hash(&mut group);
    seahash(&mut group);
    group.finish();
}

pub fn std<M: Measurement>(group: &mut BenchmarkGroup<M>) {
    use std::collections::hash_map::DefaultHasher;
    group.bench_function("DefaultHasher/empty", |b| {
        b.iter(|| bench_empty::<DefaultHasher>())
    });
    group.bench_function("DefaultHasher/hello", |b| {
        b.iter(|| bench_hello::<DefaultHasher>())
    });
}

pub fn ahash<M: Measurement>(group: &mut BenchmarkGroup<M>) {
    use ahash::AHasher;
    group.bench_function("AHasher/empty", |b| b.iter(|| bench_empty::<AHasher>()));
    group.bench_function("AHasher/hello", |b| b.iter(|| bench_hello::<AHasher>()));
}

pub fn fnv<M: Measurement>(group: &mut BenchmarkGroup<M>) {
    use fnv::FnvHasher;
    group.bench_function("FnvHasher/empty", |b| b.iter(|| bench_empty::<FnvHasher>()));
    group.bench_function("FnvHasher/hello", |b| b.iter(|| bench_hello::<FnvHasher>()));
}

pub fn fxhash<M: Measurement>(group: &mut BenchmarkGroup<M>) {
    use fxhash::{FxHasher, FxHasher32, FxHasher64};

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
}

pub fn rustc_hash<M: Measurement>(group: &mut BenchmarkGroup<M>) {
    use rustc_hash::FxHasher;
    group.bench_function("rustc/FxHasher/empty", |b| {
        b.iter(|| bench_empty::<FxHasher>())
    });
    group.bench_function("rustc/FxHasher/hello", |b| {
        b.iter(|| bench_hello::<FxHasher>())
    });
}

pub fn seahash<M: Measurement>(group: &mut BenchmarkGroup<M>) {
    use seahash::SeaHasher;
    group.bench_function("SeaHasher/empty", |b| b.iter(|| bench_empty::<SeaHasher>()));
    group.bench_function("SeaHasher/hello", |b| b.iter(|| bench_hello::<SeaHasher>()));
}

pub fn keyhash<M: Measurement>(group: &mut BenchmarkGroup<M>) {
    group.bench_function("KeyHasher/empty", |b| b.iter(|| bench_empty::<KeyHasher>()));
    group.bench_function("KeyHasher/hello", |b| b.iter(|| bench_hello::<KeyHasher>()));
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
