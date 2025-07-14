use core_data::write_ahead_log::{simple_wal::SimpleWal, write_ahead_log::WriteAheadLog};
use std::{io::SeekFrom, path::Path};

fn _print_all_file_content(temp_path: &Path){
    let tick_content = std::fs::read(temp_path.join("wal.tick")).expect("failed to read wal.tick");
    let tock_content = std::fs::read(temp_path.join("wal.tock")).expect("failed to read wal.tock");
    let log_content = std::fs::read(temp_path.join("wal.log")).expect("failed to read wal.log");
    let meta_content = std::fs::read(temp_path.join("wal.meta")).expect("failed to read wal.meta");
    println!("wal.tick content: {:?}", tick_content);
    println!("wal.tock content: {:?}", tock_content);
    println!("wal.log content: {:?}", log_content);
    println!("wal.meta content: {:?}", meta_content);
}

#[test]
fn test_new_wal_at_directory() {
    let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
    let temp_path = temp_dir.path();
    let _ = SimpleWal::new_wal_at_directory(temp_path.to_path_buf());
    assert!(temp_path.join("wal.tick").exists());
    assert!(temp_path.join("wal.tock").exists());
    assert!(temp_path.join("wal.log").exists());
    assert!(temp_path.join("wal.meta").exists());

    let tick_content = std::fs::read(temp_path.join("wal.tick")).expect("failed to read wal.tick");
    let tock_content = std::fs::read(temp_path.join("wal.tock")).expect("failed to read wal.tock");
    let log_content = std::fs::read(temp_path.join("wal.log")).expect("failed to read wal.log");
    let meta_content = std::fs::read(temp_path.join("wal.meta")).expect("failed to read wal.meta");

    assert!(tick_content.is_empty(), "wal.tick should be empty");
    assert!(tock_content.is_empty(), "wal.tock should be empty");
    assert!(log_content.is_empty(), "wal.log should be empty");
    assert_eq!(meta_content.len(), 32, "wal.meta should contain 32 bytes of zeros");
    assert!(meta_content.iter().all(|&x| x == 0), "wal.meta should contain 32 bytes of zeros");
}

#[test]
fn test_read() {
    let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
    let temp_path = temp_dir.path();

    let mut wal = SimpleWal::new_wal_at_directory(temp_path.to_path_buf());

    wal.write(vec![1, 2, 3, 4]).expect("write failed");
    wal.seek(SeekFrom::Start(0)).expect("seek failed");
    let data = wal.read(4).expect("read failed");

    assert_eq!(data, vec![1, 2, 3, 4]);
}

#[test]
fn test_write() {
    let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
    let temp_path = temp_dir.path();

    let mut wal = SimpleWal::new_wal_at_directory(temp_path.to_path_buf());

    wal.write(vec![5, 6, 7, 8]).expect("write failed");
    wal.seek(SeekFrom::Start(0)).expect("seek failed");
    let data = wal.read(4).expect("read failed");

    assert_eq!(data, vec![5, 6, 7, 8]);
}

#[test]
fn test_seek() {
    let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
    let temp_path = temp_dir.path();

    let mut wal = SimpleWal::new_wal_at_directory(temp_path.to_path_buf());

    wal.write(vec![9, 10, 11, 12]).expect("write failed");
    wal.seek(SeekFrom::Start(2)).expect("seek failed");
    let data = wal.read(2).expect("read failed");

    assert_eq!(data, vec![11, 12]);
}

#[test]
fn test_stream_len() {
    let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
    let temp_path = temp_dir.path();

    let mut wal = SimpleWal::new_wal_at_directory(temp_path.to_path_buf());

    wal.write(vec![13, 14, 15, 16]).expect("write failed");
    let len = wal.stream_len().expect("stream_len failed");

    assert_eq!(len, 4);
}

#[test]
fn test_stream_position() {
    let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
    let temp_path = temp_dir.path();

    let mut wal = SimpleWal::new_wal_at_directory(temp_path.to_path_buf());

    wal.write(vec![17, 18, 19, 20]).expect("write failed");
    wal.seek(SeekFrom::Start(2)).expect("seek failed");
    let pos = wal.stream_position().expect("stream_position failed");

    assert_eq!(pos, 2);
}

#[test]
fn test_atomic_checkpoint() {
    let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
    let temp_path = temp_dir.path();

    let mut wal = SimpleWal::new_wal_at_directory(temp_path.to_path_buf());
    wal.write(vec![21, 22, 23, 24]).expect("write failed");
    wal.atomic_checkpoint().expect("atomic_checkpoint failed");
    wal.seek(SeekFrom::Start(0)).expect("seek failed");
    let data = wal.read(4).expect("read failed");

    assert_eq!(data, vec![21, 22, 23, 24]);
}

#[test]
fn test_set_len() {
    let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
    let temp_path = temp_dir.path();

    let mut wal = SimpleWal::new_wal_at_directory(temp_path.to_path_buf());

    wal.write(vec![25, 26, 27, 28]).expect("write failed");
    let len = wal.stream_len().expect("stream_len failed");
    assert_eq!(len, 4);
    wal.set_len(3).expect("set_len failed");
    let len = wal.stream_len().expect("stream_len failed");
    assert_eq!(len, 3);
    wal.atomic_checkpoint().expect("atomic_checkpoint failed");
    let len = wal.stream_len().expect("stream_len failed");
    assert_eq!(len, 3);
    wal.set_len(2).expect("set_len failed");
    let len = wal.stream_len().expect("stream_len failed");
    assert_eq!(len, 2);
    wal.atomic_checkpoint().expect("atomic_checkpoint failed");
    let len = wal.stream_len().expect("stream_len failed");
    assert_eq!(len, 2);
}
