use std::io;
use std::mem;
use std::path;
use nexusdb::storage::nffile::{flush_nffile, load_nffile, NFFile};

// TODO: 把这个跑通
// 修改NFFile的new函数，使其可以选择以file_path来初始化，file_path是可选参数
// 研究下Rust的可选参数、默认参数
// 安装questDB在shenyang_office上  ------ questDB在 /opt/questdb-7.3.7
// benchmark测试一下持久化的性能

fn main() -> io::Result<()> {
    // 创建 NFFile 实例，自动生成随机文件路径
    let mut nf_file = NFFile::new(0, 1000, 4);

    // 添加数据
    nf_file.add_data(1000, &42i32);

    // 查询数据
    if let Some(value) = nf_file.query_data::<i32>(1000) {
        println!("Queried value from memory: {}", value);
    }

    // 将数据刷新到磁盘并清空内存
    flush_nffile(&mut nf_file)?;
    mem::drop(nf_file);

    let mut loaded_nf_file = NFFile::new(0, 1000, 4);

    // 从磁盘加载数据
    load_nffile(&mut loaded_nf_file)?;

    // 再次查询数据
    if let Some(value) = loaded_nf_file.query_data::<i32>(1000) {
        println!("Queried value from file: {}", value);
    }

    Ok(())
}