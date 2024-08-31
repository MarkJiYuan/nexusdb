// 在 `benches/benchmark_tests.rs`
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use nexusdb::data_processing::workerpool::WorkerPool;
use nexusdb::storage::nffile::{flush_nffile, NFFile};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

fn criterion_benchmark(c: &mut Criterion) {
    // let mut nf_file = NFFile::new(0, 1000, 4, None);
    let file_name = "temp_data.bin".to_string();
    let nf_file = Arc::new(Mutex::new(NFFile::new(0, 1000, 4, Some(PathBuf::from(&file_name)))));

    let pool = WorkerPool::new(13);

    c.bench_function("NFFile add_data", |b| {
        b.iter(|| {
            let nf_file_clone = nf_file.clone();
            for i in 0..100 {
                let mut nf_file = nf_file_clone.lock().unwrap();
                nf_file.add_data(i, &i);
                nf_file.add_data(i, &i);
                nf_file.add_data(i, &i);
                nf_file.add_data(i, &i);
                nf_file.add_data(i, &i);
                nf_file.add_data(i, &i);
                nf_file.add_data(i, &i);
                nf_file.add_data(i, &i);
                nf_file.add_data(i, &i);
                nf_file.add_data(i, &i);
                nf_file.add_data(i, &i);
                nf_file.add_data(i, &i);
                nf_file.add_data(i, &i);
            }
        })
    });

    // c.bench_function("NFFile query_data", |b| {
    //     b.iter(|| {
    //         let nf_file = nf_file.lock().unwrap();
    //         for i in 0..100 {
    //             let _ = nf_file.query_data::<i32>(i);
    //         }
    //     })
    // });

    c.bench_function("worker add_data", |b| {
        b.iter(|| {
            for i in 0..100 {
                let nf_file_clone = nf_file.clone();
                pool.execute(move || {
                    let mut nf_file = nf_file_clone.lock().unwrap();
                    nf_file.add_data(i, &i);
                    nf_file.add_data(i, &i);
                    nf_file.add_data(i, &i);
                    nf_file.add_data(i, &i);
                    nf_file.add_data(i, &i);
                    nf_file.add_data(i, &i);
                    nf_file.add_data(i, &i);
                    nf_file.add_data(i, &i);
                    nf_file.add_data(i, &i);
                    nf_file.add_data(i, &i);
                    nf_file.add_data(i, &i);
                    nf_file.add_data(i, &i);
                    nf_file.add_data(i, &i);
                });
            }
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
