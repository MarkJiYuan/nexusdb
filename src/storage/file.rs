use std::option::Option;
use std::vec::Vec;

#[derive(Debug)]
pub struct Header {
    version: u8,
    start_ts: i64,
    interval: u32,
    data_length: u16
}

#[derive(Debug)]
pub struct NFFile {
    header: Header,
    data: Vec<u8>,
    cursor: usize,
}

impl NFFile {
    pub fn new(header: Header) -> Self {
        NFFile {
            header,
            data: Vec::new(),
            cursor: 0,
        }
    }

    // 计算数据在data中的下标位置
    fn calculate_position(&self, timestamp: i64) -> usize {
        let position = (timestamp - self.header.start_ts) as usize / self.header.interval as usize * self.header.data_length as usize;
        position
    }

    // 添加数据，数据由时间戳和实际数据组成
    pub fn add_data(&mut self, timestamp: i64, value: &[u8]) {
        // 使用calculate_position函数计算数据位置
        let position = self.calculate_position(timestamp);
        // 确保data有足够的空间存储新数据
        if position + self.header.data_length as usize > self.data.len() {
            // 如果位置超出当前数据长度，扩展data
            self.data.resize(position + self.header.data_length as usize, 0);
        }
        // 存储新数据到计算出的位置
        for (i, &byte) in value.iter().enumerate() {
            self.data[position + i] = byte;
        }
        // 更新cursor为最后数据位置的末尾
        self.cursor = position + self.header.data_length as usize;
    }

    // 查询某个时间点的数据
    pub fn query_data(&self, query_timestamp: i64) -> Option<&[u8]> {
        let position = self.calculate_position(query_timestamp);
        if position + self.header.data_length as usize > self.data.len() {
            // 如果查询位置超出了数据范围，返回None
            None
        } else {
            // 返回查询时间点对应的数据片段
            Some(&self.data[position..position + self.header.data_length as usize])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initialization() {
        let header = Header {
            version: 1,
            start_ts: 1609459200000,  // 2021-01-01 00:00:00 UTC in milliseconds
            interval: 1000,          // 1 second
            data_length: 4,          // 4 bytes per record
        };
        let nf_file = NFFile::new(header);
        assert_eq!(nf_file.header.version, 1);
        assert_eq!(nf_file.data.len(), 0);
        assert_eq!(nf_file.cursor, 0);
    }

    #[test]
    fn test_add_data() {
        let header = Header {
            version: 1,
            start_ts: 1609459200000,
            interval: 1000,
            data_length: 4,
        };
        let mut nf_file = NFFile::new(header);
        let data = [0, 1, 2, 3];
        nf_file.add_data(1609459200000, &data);
        assert_eq!(nf_file.data, vec![0, 1, 2, 3]);
        assert_eq!(nf_file.cursor, 4);

        // Add another record 1 second later
        let new_data = [4, 5, 6, 7];
        nf_file.add_data(1609459201000, &new_data);
        assert_eq!(nf_file.data, vec![0, 1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(nf_file.cursor, 8);
    }

    #[test]
    fn test_query_data() {
        let header = Header {
            version: 1,
            start_ts: 1609459200000,
            interval: 1000,
            data_length: 4,
        };
        let mut nf_file = NFFile::new(header);
        let data = [0, 1, 2, 3];
        nf_file.add_data(1609459200000, &data);
        let result = nf_file.query_data(1609459200000);
        assert_eq!(result, Some(&[0, 1, 2, 3][..]));

        // Query non-existent data
        let missing_result = nf_file.query_data(1609459201000);
        assert_eq!(missing_result, None);
    }
}