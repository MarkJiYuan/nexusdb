use crate::storage::nffile::{Header, NFFile, flush_nffile};
use once_cell::sync::Lazy;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, Read};
use std::path;
use std::path::PathBuf;
use std::sync::Mutex;
use uuid::Uuid;

#[derive(Debug)]
pub struct IndexManager {
    indices: BTreeMap<String, String>,
}
pub fn calculate_position(start_ts: i64, interval: u32, data_length: u16, timestamp: i64) -> usize {
    let delta_ts = timestamp - start_ts;
    let half_interval = interval as i64 / 2;

    // 使用局部计算的半个间隔来四舍五入
    let index = (delta_ts + half_interval) / interval as i64;
    let position = index as usize * data_length as usize;
    position
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

    pub fn get_header_by_tag(&mut self, tag: &str) -> io::Result<Header> {
        if let Some(file_name) = self.indices.get(tag) {
            let file_path = PathBuf::from(file_name);
            read_header_from_file(&file_path)
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "Tag not found"))
        }
    }

    pub fn get_position(&mut self, tag: &str, timestamp: i64) -> io::Result<usize> {
        if let Some(file_name) = self.indices.get(tag) {
            let file_path = PathBuf::from(file_name);
            let header = read_header_from_file(&file_path)?;
            Ok(calculate_position(
                header.start_ts,
                header.interval,
                header.data_length,
                timestamp,
            ))
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "Tag not found"))
        }
    }

    pub fn get_all_indices(&self) -> &BTreeMap<String, String> {
        &self.indices
    }
}

static INDEX_MANAGER: Lazy<Mutex<IndexManager>> = Lazy::new(|| Mutex::new(IndexManager::new()));

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_with_nonexistent_file() {
        let file_name = Uuid::new_v4().to_string() + ".bin"; // 生成一个唯一的文件名
        let file_path = path::PathBuf::from(file_name.clone());
        let mut nf_file = NFFile::new(0, 1000, 4, Some(file_path.clone()));

        nf_file.add_data(1000, &42i32);

        let tags: &str = "Region1.O.temperature";
        let mut manager = INDEX_MANAGER.lock().unwrap();
        manager.add_index_entry(tags.to_string(), file_name.clone());
        let mut expected_map = BTreeMap::new();
        expected_map.insert("Region1.O.temperature".to_string(), file_name);

        assert_eq!(manager.get_all_indices(), &expected_map);
    }

    #[test]
    fn test_get_position() {
        let file_name = "1eecf02d-1685-49be-9f44-ffc17d899bb0.bin";
        let file_path = path::PathBuf::from(file_name);
        let mut nf_file = NFFile::new(0, 1000, 4, Some(file_path.clone()));

        nf_file.add_data(0, &41i32);
        nf_file.add_data(1000, &42i32);
        nf_file.add_data(2000, &43i32);
        flush_nffile(&mut nf_file).unwrap();

        // 创建IndexManager并添加索引
        let tags: &str = "Region1.O.temperature";
        let mut manager = INDEX_MANAGER.lock().unwrap();
        manager.add_index_entry(tags.to_string(), file_name.to_string());

        // 调用get_position并检查结果
        let position = manager.get_position(tags, 100).unwrap();
        assert_eq!(position, 0); 

        let position = manager.get_position(tags, 1100).unwrap();
        assert_eq!(position, 4);

        let position = manager.get_position(tags, 2100).unwrap();
        assert_eq!(position, 8);

    }
}
