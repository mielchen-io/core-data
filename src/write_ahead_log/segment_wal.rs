use std::{io::{Error, SeekFrom}, path::PathBuf};
use crate::write_ahead_log::write_ahead_log::WriteAheadLog;

const WAL_SIGNATURE: [u8; 24] = *b"SegmentWal_2025.7.12____";
const SEGMENT_FILE_SIGNATURE: [u8; 8] = *b"Log_File";
const DATA_FILE_SIGNATURE: [u8; 8] = *b"Data____";

pub struct SegmentWal {
    dir_path: PathBuf,
    data_file: std::fs::File,
}

enum LogOperation {
    Read(u64),
    Write(Vec<u8>),
    Seek(SeekFrom),
    StreamLen(u64),
    StreamPosition(u64),
    SetLen(u64),
}

struct LogEntry {
    log_sequence_number: u64,
    operation: LogOperation,
    checksum: u64,
    persistent: bool,
}


impl SegmentWal {
    pub fn new_wal_at_directory(dir_path: PathBuf) -> Self {
        assert!(dir_path.exists(), "Directory does not exist: {:?}", dir_path);
        assert!(dir_path.is_dir(), "Path is not a directory: {:?}", dir_path);
        assert!(dir_path.read_dir().expect("Could not read directory").next().is_none(), "Directory is not empty: {:?}", dir_path);

        let data_file_path = dir_path.join("wal.data");
        let data_file = std::fs::File::create(&data_file_path)
            .expect("Failed to create data file");
        
        //get current system time for the log file name
        let log_file_name = format!("segment_{}.log", 1);

        Self {
            dir_path,
            data_file,
        }
    }
}

impl WriteAheadLog for SegmentWal {
    fn read(&mut self, size: u64) -> Result<Vec<u8>, Error> {
        // Boilerplate implementation
        Ok(vec![0; size as usize]) // Return a vector of zeros of the requested size
    }

    fn write(&mut self, buf: Vec<u8>) -> Result<(), Error> {
        // Boilerplate implementation
        Ok(())
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<(), Error> {
        // Boilerplate implementation
        Ok(())
    }

    fn stream_len(&mut self) -> Result<u64, Error> {
        // Boilerplate implementation
        Ok(0)
    }

    fn stream_position(&mut self) -> Result<u64, Error> {
        // Boilerplate implementation
        Ok(0)
    }

    fn atomic_checkpoint(&mut self) -> Result<(), Error> {
        // Boilerplate implementation
        Ok(())
    }

    fn set_len(&mut self, size: u64) -> Result<(), Error> {
        // Boilerplate implementation
        Ok(())
    }
}
