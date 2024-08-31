use nexusdb::data_processing::workerpool::WorkerPool;
use nexusdb::index_manager::index_manager::IndexManager;
use nexusdb::storage::nffile::{flush_nffile, NFFile};
use std::path::PathBuf;
use std::time::Instant;
use std::time::SystemTime;

// 测试现在的插入、查询速度，测试多核是不是真的能提速
// 改index_manager，用rsqlite能持久化meta表内容，现在demo里，filename是写死的，改回自动生成的uuid模式

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // let tag = "temperature".to_string();
    // let file_name = uuid::Uuid::new_v4().to_string();

    // let pool = WorkerPool::new(4);

    // let start_time = Instant::now();

    // // for i in 0..100 {
    // //     let mut nf_file = NFFile::new(0, 1000, 4, Some(PathBuf::from(&file_name)));
    // //     nf_file.add_data(i, &i);
    // // }

    // for i in 0..100 {
    //     let mut nf_file = NFFile::new(0, 1000, 4, Some(PathBuf::from(&file_name)));
    //     pool.execute(move || {
    //         nf_file.add_data(i, &(i as i32));
    //     });
    // }
    // // 主线程等待一段时间，确保所有任务完成
    // std::thread::sleep(std::time::Duration::from_secs(5));

    // let duration = start_time.elapsed();
    println!("Operation completed in: {:?}", duration);
    let mut index_manager = IndexManager::new();
    index_manager.add_index_entry("index1".to_string(), "12345".to_string(), SystemTime::now());

    let conn = IndexManager::init_db()?;
    index_manager.save_indices_to_db(&conn)?;

    let mut loaded_manager = IndexManager::new();
    loaded_manager.load_indices_from_db(&conn)?;

    // Display loaded indices
    for (tag, entry) in loaded_manager.indices.iter() {
        println!("Loaded: {} -> UUID: {}, Last Modified: {:?}", tag, entry.uuid, entry.last_modified);
    }

    Ok(())
}
