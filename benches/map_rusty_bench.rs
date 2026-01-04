use criterion::{Criterion, criterion_group, criterion_main};
use rusty_kv::store::{MapRustyKV, RustyKV};

fn bench_map_kv(c: &mut Criterion) {
    let mut map_kv = MapRustyKV::new();

    c.bench_function("put 1000 keys", |b| {
        b.iter(|| {
            for i in 0..1000 {
                map_kv.save(&format!("key{}", i), format!("value{}", i));
            }
        });
    });

    c.bench_function("get 1000 keys", |b| {
        b.iter(|| {
            for i in 0..1000 {
                map_kv.get(&format!("key{}", i));
            }
        });
    });

    c.bench_function("delete 1000 keys", |b| {
        b.iter(|| {
            for i in 0..1000 {
                map_kv.delete(&format!("key{}", i));
            }
        });
    });
}

criterion_group!(benches, bench_map_kv);
criterion_main!(benches);
