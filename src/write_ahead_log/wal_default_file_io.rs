use std::path::PathBuf;

pub(in crate::write_ahead_log) fn create_file_with_permissions(path: &PathBuf) -> std::fs::File {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .expect("Failed to create one of the WAL files")
}


pub(in crate::write_ahead_log) fn open_file_with_permissions(path: &PathBuf) -> std::fs::File {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("Failed to open one of the WAL files")
}