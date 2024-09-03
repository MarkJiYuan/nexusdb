use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use nexusdb::storage::nffile::{NFFile, flush_nffile, load_nffile};
use nexusdb::index_manager::index_manager::IndexManager;
use nexusdb::utils::abs_path::get_absolute_path_for_data_file;
use std::time::SystemTime;

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:8080")?;
    println!("Server listening on port 8080");

    let index_manager = Arc::new(Mutex::new(IndexManager::new()));
    let file_name = "temp_data.bin".to_string();

    // 初始化数据库文件和索引
    initialize_database(&index_manager, &file_name);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let index_manager = Arc::clone(&index_manager);
                thread::spawn(move || handle_client(stream, index_manager));
            }
            Err(e) => {
                println!("Connection failed: {}", e);
            }
        }
    }

    Ok(())
}

fn initialize_database(index_manager: &Arc<Mutex<IndexManager>>, file_name: &str) {
    let tag = "temperature".to_string();
    index_manager.lock().unwrap().add_index_entry(tag.clone(), file_name.to_string(), SystemTime::now());

    let mut nf_file = NFFile::new(0, 1000, 4, Some(get_absolute_path_for_data_file(file_name)));
    let value: i32 = 42;
    nf_file.add_data(1000, &value);
    flush_nffile(&mut nf_file).expect("Failed to flush data to file");
}

fn handle_client(mut stream: TcpStream, index_manager: Arc<Mutex<IndexManager>>) {
    let mut buffer = [0; 512];

    loop {
        let bytes_read = match stream.read(&mut buffer) {
            Ok(0) => {
                println!("Client disconnected");
                return;
            }
            Ok(bytes_read) => bytes_read,
            Err(e) => {
                println!("Failed to read from socket: {}", e);
                return;
            }
        };

        let request = String::from_utf8_lossy(&buffer[..bytes_read]);
        let response = process_request(request.trim(), &index_manager);
        if let Err(e) = stream.write_all(response.as_bytes()) {
            println!("Failed to write to socket: {}", e);
            return;
        }
    }
}

fn process_request(request: &str, index_manager: &Arc<Mutex<IndexManager>>) -> String {
    let parts: Vec<&str> = request.split_whitespace().collect();
    if parts.len() < 2 {
        return "Error: Invalid request format".to_string();
    }

    let command = parts[0];
    let tag = parts[1];

    match command {
        "INSERT" => {
            if parts.len() < 4 {
                return "Error: Invalid INSERT format".to_string();
            }
            let timestamp: i64 = match parts[2].parse() {
                Ok(ts) => ts,
                Err(_) => return "Error: Invalid timestamp".to_string(),
            };
            let value: i32 = match parts[3].parse() {
                Ok(v) => v,
                Err(_) => return "Error: Invalid value".to_string(),
            };

            insert_data(index_manager, tag, timestamp, value)
        }
        "QUERY" => {
            if parts.len() < 3 {
                return "Error: Invalid QUERY format".to_string();
            }
            let timestamp: i64 = match parts[2].parse() {
                Ok(ts) => ts,
                Err(_) => return "Error: Invalid timestamp".to_string(),
            };

            query_data(index_manager, tag, timestamp)
        }
        _ => "Error: Unknown command".to_string(),
    }
}

fn insert_data(index_manager: &Arc<Mutex<IndexManager>>, tag: &str, timestamp: i64, value: i32) -> String {
    let manager = index_manager.lock().unwrap();
    let file_name = {
        match manager.get_index_entry(tag) {
            Some(entry) => &entry.uuid,
            None => return "Error: Tag not found".to_string(),
        }
    };

    let mut nf_file = NFFile::new(0, 1000, 4, Some(get_absolute_path_for_data_file(file_name)));
    let _ = load_nffile(&mut nf_file).expect("Failed to load data from file");
    nf_file.add_data(timestamp, &value);
    flush_nffile(&mut nf_file).expect("Failed to flush data to file");
    

    "Data inserted successfully".to_string()
}

fn query_data(index_manager: &Arc<Mutex<IndexManager>>, tag: &str, timestamp: i64) -> String {
    let manager = index_manager.lock().unwrap();
    let file_name = {
        match manager.get_index_entry(tag) {
            Some(entry) => &entry.uuid,
            None => return "Error: Tag not found".to_string(),
        }
    };

    let current_dir = std::env::current_dir().expect("Failed to get current directory");
    let file_path = current_dir.join("data").join(file_name);
    println!("File path: {:?}", file_path);

    let mut nf_file = NFFile::new(0, 1000, 4, Some(file_path));
    let _ = load_nffile(&mut nf_file).expect("Failed to load data from file");

    println!("Querying data for tag: {}, timestamp: {}", tag, timestamp);
    match nf_file.query_data::<i32>(timestamp) {
        Some(value) => format!("Data at {}: {}", timestamp, value),
        None => "Error: Data not found".to_string(),
    }
}