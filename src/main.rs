use nexusdb::storage::nffile::{flush_nffile, load_nffile, NFFile};
use nexusdb::index_manager::index_manager::IndexManager;
use std::io;
use std::mem;
use std::path;
use uuid::Uuid;
use std::sync::Mutex;
use once_cell::sync::Lazy;

// TODO: 把这个跑通
// 修改NFFile的new函数，使其可以选择以file_path来初始化，file_path是可选参数
// 研究下Rust的可选参数、默认参数
// 安装questDB在shenyang_office上  ------ questDB在 /opt/questdb-7.3.7
// benchmark测试一下持久化的性能
static INDEX_MANAGER: Lazy<Mutex<IndexManager>> = Lazy::new(|| Mutex::new(IndexManager::new()));

fn main() -> io::Result<()> {
    // // 创建 NFFile 实例，自动生成随机文件路径
    // let file_name = Uuid::new_v4().to_string() + ".bin"; // 生成一个唯一的文件名
    // let file_path = path::PathBuf::from(file_name);

    // let mut nf_file = NFFile::new(0, 1000, 4, Some(file_path.clone()));

    // // 添加数据
    // nf_file.add_data(1000, &42i32);

    // // 查询数据
    // if let Some(value) = nf_file.query_data::<i32>(1000) {
    //     println!("Queried value from memory: {}", value);
    // }

    // // 将数据刷新到磁盘并清空内存
    // flush_nffile(&mut nf_file)?;
    // mem::drop(nf_file);

    // let mut loaded_nf_file = NFFile::new(0, 1000, 4, Some(file_path));

    // // 从磁盘加载数据
    // load_nffile(&mut loaded_nf_file)?;

    // // 再次查询数据
    // if let Some(value) = loaded_nf_file.query_data::<i32>(1000) {
    //     println!("Queried value from file: {}", value);
    // }

    // let file_name = Uuid::new_v4().to_string() + ".bin"; // 生成一个唯一的文件名
    let file_name = "1eecf02d-1685-49be-9f44-ffc17d899bb0.bin";
    let file_path = path::PathBuf::from(file_name);
    let mut nf_file = NFFile::new(0, 1000, 4, Some(file_path.clone()));

    nf_file.add_data(0, &41i32);
    nf_file.add_data(1000, &42i32);
    nf_file.add_data(2000, &43i32);
    flush_nffile(&mut nf_file)?;

    let tags: &str = "Region1.O.temperature";

    let mut manager = INDEX_MANAGER.lock().unwrap();
    manager.add_index_entry(tags.to_string(), file_name.to_string());

    let header = manager.get_header_by_tag(tags);
    println!("{:?}", header);
    let position = manager.get_position(tags, 100);
    println!("{:?}", position);

    Ok(())
}
