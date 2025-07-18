use core_data::write_ahead_log::{wal_default::WriteAheadLogDefault, wal_trait::WriteAheadLog};
use rand::SeedableRng;
use std::{io::SeekFrom, path::Path};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};

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

fn health_check(temp_path: &Path) {
    // Check if the log file is empty
    let log_content = std::fs::read(temp_path.join("wal.log")).expect("failed to read wal.log");
    assert!(log_content.is_empty(), "wal.log should be empty after recovery");
    // Check if the meta file is valid
    let meta_content = std::fs::read(temp_path.join("wal.meta")).expect("failed to read wal.meta");
    assert_eq!(meta_content.len(), 32, "wal.meta should contain 32 bytes");
    assert!(meta_content.iter().all(|&x| x == 1) || meta_content.iter().all(|&x| x == 0), "wal.meta should contain all ones or all zeros after recovery");
    // Check if tick and tock files are identical
    let tick_content = std::fs::read(temp_path.join("wal.tick")).expect("failed to read wal.tick");
    let tock_content = std::fs::read(temp_path.join("wal.tock")).expect("failed to read wal.tock");
    assert_eq!(tick_content, tock_content, "wal.tick and wal.tock should be identical after recovery");
}

#[test]
fn test_new_wal_at_directory() {
    let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
    let temp_path = temp_dir.path();
    let _ = WriteAheadLogDefault::new_wal_at_directory(temp_path.to_path_buf());
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

    let mut wal = WriteAheadLogDefault::new_wal_at_directory(temp_path.to_path_buf());

    wal.write(vec![1, 2, 3, 4]);
    wal.seek(SeekFrom::Start(0));
    let data = wal.read(4);

    assert_eq!(data, vec![1, 2, 3, 4]);
}

#[test]
fn test_write() {
    let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
    let temp_path = temp_dir.path();

    let mut wal = WriteAheadLogDefault::new_wal_at_directory(temp_path.to_path_buf());

    wal.write(vec![5, 6, 7, 8]);
    wal.seek(SeekFrom::Start(0));
    let data = wal.read(4);

    assert_eq!(data, vec![5, 6, 7, 8]);
}

#[test]
fn test_seek() {
    let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
    let temp_path = temp_dir.path();

    let mut wal = WriteAheadLogDefault::new_wal_at_directory(temp_path.to_path_buf());

    wal.write(vec![9, 10, 11, 12]);
    wal.seek(SeekFrom::Start(2));
    let data = wal.read(2);

    assert_eq!(data, vec![11, 12]);
}

#[test]
fn test_stream_len() {
    let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
    let temp_path = temp_dir.path();

    let mut wal = WriteAheadLogDefault::new_wal_at_directory(temp_path.to_path_buf());

    wal.write(vec![13, 14, 15, 16]);
    let len = wal.stream_len();

    assert_eq!(len, 4);
}

#[test]
fn test_stream_position() {
    let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
    let temp_path = temp_dir.path();

    let mut wal = WriteAheadLogDefault::new_wal_at_directory(temp_path.to_path_buf());

    wal.write(vec![17, 18, 19, 20]);
    wal.seek(SeekFrom::Start(2));
    let pos = wal.stream_position();

    assert_eq!(pos, 2);
}

#[test]
fn test_atomic_checkpoint() {
    let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
    let temp_path = temp_dir.path();

    let mut wal = WriteAheadLogDefault::new_wal_at_directory(temp_path.to_path_buf());
    wal.write(vec![21, 22, 23, 24]);
    wal.atomic_checkpoint();
    wal.seek(SeekFrom::Start(0));
    let data = wal.read(4);

    assert_eq!(data, vec![21, 22, 23, 24]);
}

#[test]
fn test_set_len() {
    let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
    let temp_path = temp_dir.path();

    let mut wal = WriteAheadLogDefault::new_wal_at_directory(temp_path.to_path_buf());

    wal.write(vec![25, 26, 27, 28]);
    let len = wal.stream_len();
    assert_eq!(len, 4);
    wal.set_len(3);
    let len = wal.stream_len();
    assert_eq!(len, 3);
    wal.atomic_checkpoint();
    let len = wal.stream_len();
    assert_eq!(len, 3);
    wal.set_len(2);
    let len = wal.stream_len();
    assert_eq!(len, 2);
    wal.atomic_checkpoint();
    let len = wal.stream_len();
    assert_eq!(len, 2);
}

#[test]
fn test_open_wal_at_directory() {
    let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
    let temp_path = temp_dir.path();
    let mut wal = WriteAheadLogDefault::new_wal_at_directory(temp_path.to_path_buf());
    wal.write(vec![29, 30, 31, 32]);
    wal.atomic_checkpoint();
    drop(wal);
    let mut wal = WriteAheadLogDefault::open_wal_at_directory(temp_path.to_path_buf());
    wal.seek(SeekFrom::Start(0));
    let data = wal.read(4);
    assert_eq!(data, vec![29, 30, 31, 32]);
}

#[test]
fn test_recovery_type_a() {
    let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
    let temp_path = temp_dir.path();
    let mut wal = WriteAheadLogDefault::new_wal_at_directory(temp_path.to_path_buf());
    wal.write(vec![33, 34, 35, 36]);
    wal.atomic_checkpoint();
    wal.seek(SeekFrom::Start(0));
    wal.write(vec![37, 38, 39, 40]);

    // Simulate a crash by dropping the wal before the atomic checkpoint wich results in a non empty log file and a recovery type A
    drop(wal);
    let mut wal = WriteAheadLogDefault::open_wal_at_directory(temp_path.to_path_buf());
    health_check(temp_path);
    // Check if the content matches the checkpoint
    wal.seek(SeekFrom::Start(0));
    let data = wal.read(4);
    assert_eq!(data, vec![33, 34, 35, 36]);
}

#[test]
fn test_recovery_type_b() {
    let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
    let temp_path = temp_dir.path();
    let mut wal = WriteAheadLogDefault::new_wal_at_directory(temp_path.to_path_buf());
    wal.write(vec![41, 42, 43, 44]);
    wal.atomic_checkpoint();
    wal.seek(SeekFrom::Start(0));
    wal.write(vec![45, 46, 47, 48]);
    wal.atomic_checkpoint();
    drop(wal);

    //now we simulate a crash during the atomic checkpoint by writing the meta file to half ones and half zeros
    //and the log file to a non empty state
    std::fs::write(temp_path.join("wal.meta"), [1u8; 16].iter().chain([0u8; 16].iter()).cloned().collect::<Vec<u8>>()).expect("failed to write wal.meta");
    std::fs::write(temp_path.join("wal.log"), [49u8, 50u8, 51u8, 52u8]).expect("failed to write wal.log");

    let mut wal = WriteAheadLogDefault::open_wal_at_directory(temp_path.to_path_buf());
    health_check(temp_path);
    // Check if the content matches one of the checkpoints
    wal.seek(SeekFrom::Start(0));
    let data = wal.read(4);
    assert!(data == vec![41, 42, 43, 44] || data == vec![45, 46, 47, 48], "data should match one of the checkpoints");
}


//a function that is testing every operation by repeatedly writing and reading data between checkpoints while keeping a pseudo file in memory for comparison
#[test]
fn test_all_operations() {
    let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
    let temp_path = temp_dir.path();
    let mut wal = WriteAheadLogDefault::new_wal_at_directory(temp_path.to_path_buf());

    let comp_file_path = temp_path.join("comp.test");
    let mut comp_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&comp_file_path)
        .expect("failed to create comp.test");

    let seed: [u8; 32] = [
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
        25, 26, 27, 28, 29, 30, 31, 32,
    ];

    let mut rng = rand::rngs::StdRng::from_seed(seed);

    for _ in 0..100 {
        for _ in 0..10 {
            let operation = rand::Rng::random_range(&mut rng, 0..7);
            match operation {
                0 => {
                    // Write
                    let data: Vec<u8> = (0..10).map(|_| rand::Rng::random_range(&mut rng, 0..255)).collect();
                    wal.write(data.clone());
                    comp_file.write_all(&data).expect("write failed");
                }
                1 => {
                    // Read
                    let wal_size = wal.stream_len();
                    let mut wal_stream_pos = wal.stream_position();
                    if wal_stream_pos >= wal_size {
                        //seek to the beginning of the file
                        wal.seek(SeekFrom::Start(0));
                        comp_file.seek(SeekFrom::Start(0)).expect("seek failed");
                        wal_stream_pos = 0;
                    }
                    let max_read_size = wal_size - wal_stream_pos;
                    let percentage: f64 = rand::Rng::random_range(&mut rng, 0.0..100.0);
                    let size = ((percentage as u64) * max_read_size) / 100;
                    let data = wal.read(size);
                    let mut pseudo_data = vec![0u8; size as usize];
                    comp_file.read_exact(&mut pseudo_data).expect("read failed");
                    assert_eq!(data, pseudo_data, "data should match comp.test data");
                }
                2 => {
                    // Seek
                    let comp_file_len = comp_file.metadata().expect("metadata failed").len();
                    let pos = rand::Rng::random_range(&mut rng, 0..=comp_file_len);
                    wal.seek(SeekFrom::Start(pos));
                    comp_file.seek(SeekFrom::Start(pos)).expect("seek failed");
                    let comp_file_seek_pos = comp_file.stream_position().expect("stream_position failed");
                    assert_eq!(wal.stream_position(), comp_file_seek_pos, "stream_position should match comp.test seek position");
                }
                3 => {
                    // StreamLen
                    let len = wal.stream_len();
                    let comp_file_len = comp_file.metadata().expect("metadata failed").len();
                    assert_eq!(len, comp_file_len, "stream_len should match comp.test length");
                }
                4 => {
                    // StreamPosition
                    let pos = wal.stream_position();
                    let comp_file_seek_pos = comp_file.stream_position().expect("stream_position failed");
                    assert_eq!(pos, comp_file_seek_pos, "stream_position should match comp.test position");
                }
                5 => {
                    // AtomicCheckpoint
                    wal.atomic_checkpoint();

                    health_check(temp_path);
                    let file_content = std::fs::read(temp_path.join("wal.tick")).expect("failed to read wal.tick");
                    let mut comp_content = Vec::new();
                    let mut comp_file_check = File::open(&comp_file_path).expect("failed to open comp.test");
                    comp_file_check.read_to_end(&mut comp_content).expect("failed to read comp.test");
                    assert_eq!(file_content, comp_content, "wal.tick should match comp.test content after atomic checkpoint");
                    //check if the stream positions and lengths are still equal
                    let wal_stream_pos = wal.stream_position();
                    let comp_file_seek_pos = comp_file.stream_position().expect("stream_position failed");
                    assert_eq!(wal_stream_pos, comp_file_seek_pos, "stream_position should match comp.test position after atomic checkpoint");
                    let wal_len = wal.stream_len();
                    let comp_file_len = comp_file.metadata().expect("metadata failed").len();
                    assert_eq!(wal_len, comp_file_len, "stream_len should match comp.test length after atomic checkpoint");
                }
                6 => {
                    // SetLen
                    let comp_file_len = comp_file.metadata().expect("metadata failed").len();
                    let new_len = rand::Rng::random_range(&mut rng, 0..=comp_file_len);
                    wal.set_len(new_len);
                    comp_file.set_len(new_len).expect("set_len failed");
                }
                _ => unreachable!(),
            }
        }
    }
    wal.atomic_checkpoint();
    health_check(temp_path);
    let file_content = std::fs::read(temp_path.join("wal.tick")).expect("failed to read wal.tick");
    let mut comp_content = Vec::new();
    let mut comp_file_check = File::open(&comp_file_path).expect("failed to open comp.test");
    comp_file_check.read_to_end(&mut comp_content).expect("failed to read comp.test");
    assert_eq!(file_content, comp_content, "wal.tick should match comp.test content after all operations");
}