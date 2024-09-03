use std::path::PathBuf;

pub fn get_absolute_path(relative_path: &str) -> PathBuf {
    let current_dir = std::env::current_dir().expect("Failed to get current directory");
    current_dir.join(relative_path)
}

pub fn get_absolute_path_for_data_file(file_name: &str) -> PathBuf {
    let current_dir = std::env::current_dir().expect("Failed to get current directory");
    current_dir.join("data").join(file_name)
}
