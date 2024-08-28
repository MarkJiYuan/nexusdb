use crate::storage::nffile::Header;
use once_cell::sync::Lazy;
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::fs::File;
use std::io::{self, Read};
use std::path::PathBuf;

#[derive(Debug)]
pub struct IndexManager {
    indices: BTreeMap<String, String>,
}

pub fn read_header_from_file(file_path: &PathBuf) -> io::Result<Header> {
    let mut file = File::open(file_path)?;

    let mut start_ts_bytes = [0u8; 8];
    let mut interval_bytes = [0u8; 4];
    let mut data_length_bytes = [0u8; 2];

    file.read_exact(&mut start_ts_bytes)?;
    file.read_exact(&mut interval_bytes)?;
    file.read_exact(&mut data_length_bytes)?;

    Ok(Header {
        start_ts: i64::from_le_bytes(start_ts_bytes),
        interval: u32::from_le_bytes(interval_bytes),
        data_length: u16::from_le_bytes(data_length_bytes),
    })
}

impl IndexManager {
    pub fn new() -> Self {
        IndexManager {
            indices: BTreeMap::new(),
        }
    }

    pub fn add_index_entry(&mut self, tag: String, uuid: String) {
        self.indices.insert(tag.clone(), uuid.clone());
    }

    pub fn find_by_timestamp(&mut self, tag: String) -> Option<&String> {
        self.indices.get(&tag)
    }


    pub fn get_header_by_tag(&mut self, tag: &str) -> io::Result<Header> {
        if let Some(file_name) = self.indices.get(tag) {
            let file_path = PathBuf::from(file_name);
            read_header_from_file(&file_path)
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "Tag not found"))
        }
    }

    pub fn get_all_indices(&self) -> &BTreeMap<String, String> {
        &self.indices
    }
}

static INDEX_MANAGER: Lazy<Mutex<IndexManager>> = Lazy::new(|| Mutex::new(IndexManager::new()));

fn main() {
}

#[cfg(test)]
mod tests {
    use super::*;

    // 测试初始化是否成功，特别是从不存在的文件初始化
    #[test]
    fn test_init_with_nonexistent_file() {
        let file_name = Uuid::new_v4().to_string() + ".bin"; // 生成一个唯一的文件名
        let file_path = path::PathBuf::from(file_name.clone());
        let mut nf_file = NFFile::new(0, 1000, 4, Some(file_path.clone()));

        nf_file.add_data(1000, &42i32);

        let tags: &str = "Region1.O.temperature";
        let mut manager = INDEX_MANAGER.lock().unwrap();
        manager.add_index_entry(tags.to_string(), file_name);
        assert_eq!(manager.get_all_indices());
    }
}
