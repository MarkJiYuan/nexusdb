// 在 `benches/benchmark_tests.rs`
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use nexusdb::storage::nffile::NFFile;

fn criterion_benchmark(c: &mut Criterion) {
    let mut nf_file = NFFile::new(0, 1000, 4);

    c.bench_function("NFFile add_data", |b| {
        b.iter(|| {
            for i in 0..1000 {
                nf_file.add_data(i, &i);
            }
        })
    });

    c.bench_function("NFFile query_data", |b| {
        b.iter(|| {
            for i in 0..1000 {
                let _ = nf_file.query_data::<i32>(i);
            }
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);