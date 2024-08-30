use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, Read};
use std::path::PathBuf;
use crate::storage::nffile::Header;

#[derive(Debug)]
pub struct IndexManager {
    indices: BTreeMap<String, String>,
}

impl IndexManager {
    pub fn new() -> Self {
        IndexManager {
            indices: BTreeMap::new(),
        }
    }

    pub fn add_index_entry(&mut self, tag: String, file_name: String) {
        println!("Adding index entry: tag={}, file_name={}", tag, file_name);  // 添加调试打印
        self.indices.insert(tag, file_name);
    }

    pub fn get_header_by_tag(&self, tag: &str) -> io::Result<Header> {
        println!("Getting header for tag: {}", tag);  // 添加调试打印

        if let Some(file_name) = self.indices.get(tag) {
            println!("Found file name: {}", file_name);  // 打印找到的文件名
            let file_path = PathBuf::from(file_name);
            self.read_header_from_file(&file_path)
        } else {
            println!("Tag not found: {}", tag);  // 打印找不到 tag 的信息
            Err(io::Error::new(io::ErrorKind::NotFound, format!("Tag '{}' not found", tag)))
        }
    }

    // 读取文件头
    fn read_header_from_file(&self, file_path: &PathBuf) -> io::Result<Header> {
        println!("Reading header from file: {:?}", file_path);  // 打印文件路径
        let mut file = File::open(file_path)?;

        let mut start_ts_bytes = [0u8; 8];
        let mut interval_bytes = [0u8; 4];
        let mut data_length_bytes = [0u8; 2];

        file.read_exact(&mut start_ts_bytes)?;
        file.read_exact(&mut interval_bytes)?;
        file.read_exact(&mut data_length_bytes)?;

        println!("Read header successfully");  // 打印读取成功信息

        Ok(Header {
            start_ts: i64::from_le_bytes(start_ts_bytes),
            interval: u32::from_le_bytes(interval_bytes),
            data_length: u16::from_le_bytes(data_length_bytes),
        })
    }
}