use std::{io::{Read, Seek, Write}, path::PathBuf};

use crate::write_ahead_log::write_ahead_log::WriteAheadLog;

const WAL_SIGNATURE: [u8; 24] = *b"COPY_WAL_2025_7_12______";
const DATA_FILE_SIGNATURE: [u8; 8] = *b"DATA____";
const COPY_FILE_SIGNATURE: [u8; 8] = *b"COPY____";
const LOG_FILE_SIGNATURE: [u8; 8] = *b"LOG_____";

pub struct CopyWal {
    dir_path: PathBuf,
    data_file: std::fs::File,
    copy_file: std::fs::File,
    log_file: std::fs::File,
}

impl CopyWal {
    pub fn new_wal_at_directory(dir_path: PathBuf) -> Self {
        assert!(dir_path.exists(), "Directory does not exist: {:?}", dir_path);
        assert!(dir_path.is_dir(), "Path is not a directory: {:?}", dir_path);
        assert!(dir_path.read_dir().expect("Could not read directory").next().is_none(), "Directory is not empty: {:?}", dir_path);

        let data_file_path = dir_path.join("wal.data");
        let mut data_file = std::fs::File::create(&data_file_path)
            .expect("Failed to create data file");

        let copy_file_path = dir_path.join("wal.copy");
        let mut copy_file = std::fs::File::create(&copy_file_path)
            .expect("Failed to create copy file");

        let log_file_path = dir_path.join("wal.log");
        let mut log_file = std::fs::File::create(&log_file_path)
            .expect("Failed to create log file");

        data_file.write_all(&WAL_SIGNATURE)
            .expect("Failed to write data file signature");
        data_file.write_all(&DATA_FILE_SIGNATURE)
            .expect("Failed to write data file signature");

        copy_file.write_all(&WAL_SIGNATURE)
            .expect("Failed to write copy file signature");
        copy_file.write_all(&COPY_FILE_SIGNATURE)
            .expect("Failed to write copy file signature");

        log_file.write_all(&WAL_SIGNATURE)
            .expect("Failed to write log file signature");
        log_file.write_all(&LOG_FILE_SIGNATURE)
            .expect("Failed to write log file signature");

        Self {
            dir_path,
            data_file,
            copy_file,
            log_file,
        }
    }
}

impl WriteAheadLog for CopyWal {
    fn read(&mut self, size: u64) -> Result<Vec<u8>, std::io::Error> {
        let mut buffer = Vec::with_capacity(size as usize);
        self.copy_file
            .read_exact(&mut buffer)?;
        Ok(buffer)
    }

    fn write(&mut self, buf: Vec<u8>) -> Result<(), std::io::Error> {
        self.copy_file
            .write_all(buf.as_slice())?;
        Ok(())
    }

    fn seek(&mut self, pos: std::io::SeekFrom) -> Result<(), std::io::Error> {
        self.copy_file
            .seek(pos)?;
        Ok(())
    }

    fn stream_len(&mut self) -> Result<u64, std::io::Error> {
        let len = self.copy_file
            .metadata()?
            .len();
        Ok(len)
    }

    fn stream_position(&mut self) -> Result<u64, std::io::Error> {
        let pos = self.copy_file
            .stream_position()?;
        Ok(pos)
    }

    fn set_len(&mut self, size: u64) -> Result<(), std::io::Error> {
        self.copy_file
            .set_len(size)?;
        Ok(())
    }

    fn atomic_checkpoint(&mut self) -> Result<(), std::io::Error> {
        todo!()
    }
}