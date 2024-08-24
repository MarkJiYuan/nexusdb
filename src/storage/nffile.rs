use std::option::Option;
use std::vec::Vec;
use std::mem;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Debug)]
pub struct Header {
    start_ts: i64,
    interval: u32,
    data_length: u16,
}

#[derive(Debug)]
pub struct NFFile {
    header: Header,
    data: Vec<u8>,
    pub file_path: PathBuf,
    cursor: usize,
}

impl NFFile {
    pub fn new(start_ts: i64, interval: u32, data_length: u16) -> Self {
        // 使用 UUID 生成唯一的文件路径
        let file_name = Uuid::new_v4().to_string();
        let file_path = PathBuf::from(format!("{}.bin", file_name));
        
        NFFile {
            header: Header {
                start_ts,
                interval,
                data_length,
            },
            data: Vec::new(),
            file_path,
            cursor: 0,
        }
    }

    // 计算数据在data中的下标位置
    fn calculate_position(&self, timestamp: i64) -> usize {
        let delta_ts = timestamp - self.header.start_ts;
        let half_interval = self.header.interval as i64 / 2;

        // 使用局部计算的半个间隔来四舍五入
        let index = (delta_ts + half_interval) / self.header.interval as i64;
        let position = index as usize * self.header.data_length as usize;
        position
    }

    pub fn add_data<T>(&mut self, timestamp: i64, value: &T) {
        let position = self.calculate_position(timestamp);
        let value_ptr = value as *const T as *const u8;
        let value_size = mem::size_of::<T>();

        // 确保有足够的空间
        if position + value_size > self.data.len() {
            self.data.resize(position + value_size, 0);
        }

        unsafe {
            let data_ptr = self.data.as_mut_ptr().add(position);
            std::ptr::copy(value_ptr, data_ptr, value_size);
        }
        self.cursor = position + value_size;
    }

    pub fn query_data<T>(&self, query_timestamp: i64) -> Option<&T> {
        let position = self.calculate_position(query_timestamp);
        let value_size = mem::size_of::<T>();

        if position + value_size > self.data.len() {
            None
        } else {
            unsafe {
                let data_ptr = self.data.as_ptr().add(position) as *const T;
                Some(&*data_ptr)
            }
        }
    }

    // 范围写：写入一段数据到指定时间范围
    pub fn range_write<T>(&mut self, start_timestamp: i64, values: &[T]) {
        let mut current_position = self.calculate_position(start_timestamp);
        let value_size = mem::size_of::<T>();

        // 确保有足够的空间
        let required_size = current_position + values.len() * value_size;
        if required_size > self.data.len() {
            self.data.resize(required_size, 0);
        }

        for value in values {
            let value_ptr = value as *const T as *const u8;
            unsafe {
                let data_ptr = self.data.as_mut_ptr().add(current_position);
                std::ptr::copy(value_ptr, data_ptr, value_size);
            }
            current_position += value_size;
        }
        self.cursor = current_position;
    }

    // 范围读：读取指定时间范围内的数据
    pub fn range_read<T>(&self, start_timestamp: i64, count: usize) -> Vec<&T> {
        let mut current_position = self.calculate_position(start_timestamp);
        let value_size = mem::size_of::<T>();

        let mut results = Vec::with_capacity(count);

        for _ in 0..count {
            if current_position + value_size > self.data.len() {
                break;
            }

            unsafe {
                let data_ptr = self.data.as_ptr().add(current_position) as *const T;
                results.push(&*data_ptr);
            }

            current_position += value_size;
        }

        results
    }
}

// Public functions to flush and load data
pub fn flush_nffile(nf_file: &mut NFFile) -> io::Result<()> {
    let mut file = OpenOptions::new().write(true).create(true).open(&nf_file.file_path)?;

    // Write the header
    file.write_all(&nf_file.header.start_ts.to_le_bytes())?;
    file.write_all(&nf_file.header.interval.to_le_bytes())?;
    file.write_all(&nf_file.header.data_length.to_le_bytes())?;

    // Write the data
    file.write_all(&nf_file.data)?;
    file.flush()?;

    Ok(())
}

pub fn load_nffile(nf_file: &mut NFFile) -> io::Result<()> {
    let mut file = File::open(&nf_file.file_path)?;

    // Read the header
    let mut start_ts_bytes = [0u8; 8];
    let mut interval_bytes = [0u8; 4];
    let mut data_length_bytes = [0u8; 2];

    file.read_exact(&mut start_ts_bytes)?;
    file.read_exact(&mut interval_bytes)?;
    file.read_exact(&mut data_length_bytes)?;

    nf_file.header.start_ts = i64::from_le_bytes(start_ts_bytes);
    nf_file.header.interval = u32::from_le_bytes(interval_bytes);
    nf_file.header.data_length = u16::from_le_bytes(data_length_bytes);

    // Read the data
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;
    nf_file.data = buffer;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_write_and_read_i32() {
        let mut nf_file = NFFile::new(0, 1000, 4);

        // 写入一组 i32 数据
        let values: [i32; 3] = [10, 20, 30];
        nf_file.range_write(1000, &values);

        // 读取并验证写入的数据
        let result: Vec<&i32> = nf_file.range_read(1000, 3);
        assert_eq!(result.len(), 3);
        assert_eq!(*result[0], 10);
        assert_eq!(*result[1], 20);
        assert_eq!(*result[2], 30);
    }

    #[test]
    fn test_range_write_and_read_f64() {
        let header = Header {
            start_ts: 0,
            interval: 1000,
            data_length: 8, // 对于 f64 类型，通常占用 8 个字节
        };
        let mut nf_file = NFFile::new(0, 1000, 4);

        // 写入一组 f64 数据
        let values: [f64; 2] = [3.14, 6.28];
        nf_file.range_write(2000, &values);

        // 读取并验证写入的数据
        let result: Vec<&f64> = nf_file.range_read(2000, 2);
        assert_eq!(result.len(), 2);
        assert_eq!(*result[0], 3.14);
        assert_eq!(*result[1], 6.28);
    }

    #[test]
    fn test_range_read_out_of_bounds() {
        let mut nf_file = NFFile::new(0, 1000, 4);

        // 写入一组 i32 数据
        let values: [i32; 3] = [10, 20, 30];
        nf_file.range_write(1000, &values);

        // 尝试读取超出范围的数据
        let result: Vec<&i32> = nf_file.range_read(4000, 3);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_range_write_multiple_types() {
        let mut nf_file = NFFile::new(0, 1000, 4);

        // 写入不同类型的数据
        let int_values: [i64; 2] = [100, 200];
        nf_file.range_write(1000, &int_values);

        let float_values: [f64; 2] = [1.23, 4.56];
        nf_file.range_write(3000, &float_values);

        // 验证整数数据
        let int_result: Vec<&i64> = nf_file.range_read(1000, 2);
        assert_eq!(int_result.len(), 2);
        assert_eq!(*int_result[0], 100);
        assert_eq!(*int_result[1], 200);

        // 验证浮点数数据
        let float_result: Vec<&f64> = nf_file.range_read(3000, 2);
        assert_eq!(float_result.len(), 2);
        assert_eq!(*float_result[0], 1.23);
        assert_eq!(*float_result[1], 4.56);
    }
}