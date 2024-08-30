use nexusdb::storage::nffile::{NFFile, flush_nffile};
use nexusdb::index_manager::index_manager::IndexManager;
use nexusdb::data_processing::workerpool::WorkerPool;
use std::path::PathBuf;

// 测试现在的插入、查询速度，测试多核是不是真的能提速
// 改index_manager，用rsqlite能持久化meta表内容，现在demo里，filename是写死的，改回自动生成的uuid模式

fn main() {
    // 创建 IndexManager 并添加索引
    let mut index_manager = IndexManager::new();
    let tag = "temperature".to_string();
    let file_name = "temp_data.bin".to_string();
    
    println!("Adding index entry with tag: {} and file_name: {}", tag, file_name);
    index_manager.add_index_entry(tag.clone(), file_name.clone());

    // 创建文件并插入数据
    let mut nf_file = NFFile::new(0, 1000, 4, Some(PathBuf::from(&file_name)));
    let value: i32 = 42;
    nf_file.add_data(1000, &value);

    // 刷新文件，将数据写入磁盘
    flush_nffile(&mut nf_file).expect("Failed to flush data to file");

    println!("Created file and inserted data successfully.");
    // 创建线程池
    let pool = WorkerPool::new(4);

    // 模拟一个请求任务
    pool.execute(move || {
        // 查询 header 信息
        if let Ok(header) = index_manager.get_header_by_tag("temperature") {
            println!("Header: {:?}", header);

            // 创建 NFFile
            let mut nf_file = NFFile::new(header.start_ts, header.interval, header.data_length, Some(PathBuf::from("temp_data.bin")));

            // 执行写入操作
            let value: i32 = 4;
            nf_file.add_data(1000, &value);

            // 查询写入的数据
            let result: Option<&i32> = nf_file.query_data(1000);
            println!("Queried data: {:?}", result);
        } else {
            println!("Tag not found.");
        }
    });

    // 主线程等待一段时间，确保所有任务完成
    std::thread::sleep(std::time::Duration::from_secs(5));
}