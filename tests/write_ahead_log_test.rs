use core_data::write_ahead_log::{simple_wal::SimpleWal, write_ahead_log::WriteAheadLog};
use std::io::{SeekFrom};

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
    wal.set_len(2).expect("set_len failed");
    let len = wal.stream_len().expect("stream_len failed");

    assert_eq!(len, 2);
}
