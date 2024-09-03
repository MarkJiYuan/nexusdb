use crate::storage::nffile::Header;
use crate::utils::abs_path::get_absolute_path;
use rusqlite::{params, Connection, Result};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, Read};
use std::path::PathBuf;
use std::time::SystemTime;

#[derive(Debug)]
pub struct IndexEntry {
    pub uuid: String,
    pub last_modified: SystemTime,
}

#[derive(Debug)]
pub struct IndexManager {
    pub indices: BTreeMap<String, IndexEntry>,
}

impl IndexManager {
    pub fn new() -> Self {
        IndexManager {
            indices: BTreeMap::new(),
        }
    }

    // 初始化数据库
    pub fn init_db() -> Result<Connection> {
        let conn = Connection::open("index.db")?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS indices (
                tag TEXT PRIMARY KEY,
                uuid TEXT NOT NULL,
                last_modified INTEGER NOT NULL
            )",
            [],
        )?;
        Ok(conn)
    }

    // 保存索引到数据库
    pub fn save_indices_to_db(&self, conn: &Connection) -> Result<()> {
        for (tag, entry) in &self.indices {
            conn.execute(
                "INSERT OR REPLACE INTO indices (tag, uuid, last_modified) VALUES (?1, ?2, ?3)",
                params![
                    tag,
                    &entry.uuid,
                    entry
                        .last_modified
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as i64
                ],
            )?;
        }
        Ok(())
    }

    // 从数据库加载索引
    pub fn load_indices_from_db(&mut self, conn: &Connection) -> Result<()> {
        let mut stmt = conn.prepare("SELECT tag, uuid, last_modified FROM indices")?;
        let index_iter = stmt.query_map([], |row| {
            Ok((
                row.get(0)?,
                IndexEntry {
                    uuid: row.get(1)?,
                    last_modified: SystemTime::UNIX_EPOCH
                        + std::time::Duration::from_secs(row.get::<_, i64>(2)? as u64),
                },
            ))
        })?;

        for index in index_iter {
            let (tag, entry) = index?;
            self.indices.insert(tag, entry);
        }
        Ok(())
    }

    pub fn add_index_entry(&mut self, tag: String, uuid: String, last_modified: SystemTime) {
        println!("Adding index entry: tag={}, file_name={}", tag, uuid); // 添加调试打印
        self.indices.insert(
            tag,
            IndexEntry {
                uuid,
                last_modified,
            },
        );
    }

    // 获取索引条目
    pub fn get_index_entry(&self, tag: &str) -> Option<&IndexEntry> {
        self.indices.get(tag)
    }

    pub fn get_header_by_tag(&self, tag: &str) -> io::Result<Header> {
        println!("Getting header for tag: {}", tag); // 添加调试打印

        if let Some(index_entry) = self.indices.get(tag) {
            let uuid = &index_entry.uuid;
            let file_name = format!("{}.bin", uuid);
            println!("Found file name: {}", file_name); // 打印找到的文件名
            let file_path = get_absolute_path(&file_name); // 获取文件路径
            self.read_header_from_file(&file_path)
        } else {
            println!("Tag not found: {}", tag); // 打印找不到 tag 的信息
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Tag '{}' not found", tag),
            ))
        }
    }

    // 读取文件头
    fn read_header_from_file(&self, file_path: &PathBuf) -> io::Result<Header> {
        println!("Reading header from file: {:?}", file_path); // 打印文件路径
        let mut file = File::open(file_path)?;

        let mut start_ts_bytes = [0u8; 8];
        let mut interval_bytes = [0u8; 4];
        let mut data_length_bytes = [0u8; 2];

        file.read_exact(&mut start_ts_bytes)?;
        file.read_exact(&mut interval_bytes)?;
        file.read_exact(&mut data_length_bytes)?;

        println!("Read header successfully"); // 打印读取成功信息

        Ok(Header {
            start_ts: i64::from_le_bytes(start_ts_bytes),
            interval: u32::from_le_bytes(interval_bytes),
            data_length: u16::from_le_bytes(data_length_bytes),
        })
    }
}
