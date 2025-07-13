//! This WAL implementation consists of 4 files:
//! - wal.tick and wal.tock contain the persistent data and always one of them is the fallback and the other is the operational file.
//! - wal.log contains log entries since the last checkpoint
//! - wal.meta contains a signature that indicates the current operational file
//! 
//! On every write operation, the following steps are performed:
//! 1. A log entry is created and written to the log file. Recovery Type A
//! 2. Then the data is written to the current operational file. Recovery Type A
//! 
//! On checkpoint, the following steps are performed:
//! 1. The current operational file in wal.meta is switched to the fallback. Recovery Type B
//! 2. Iterate over the log file and apply all log entries to the new operational file. Recovery Type A
//! 3. Erase all log entries in the log file. Recovery Type A
//! 
//! After every numbered step, fsync syscall is used to ensure durability.
//! 
//! The Recovery Type for each step specifies how a faulty state is detected if the system crashes during this step and how the system can recover from it:
//! - Recovery Type A: A faulty state is detected when wal.meta contains a valid signature but wal.log is not empty and the operational file might be missing. The system can recover by deleting the current operational file, generating a new one as a copy of the fallback and afterwards erasing the log file. If this process is interrupted by a crash, the same recovery can be performed again.
//! - Recovery Type B: A faulty state is detected when wal.meta does not contain a valid signature and wal.tick might be missing. The system can recover by deleting wal.tick and generating it again as a copy of wal.tock. Then the wal.meta is updated with a new valid signature pointing to wal.tock. If this process is interrupted by a crash, the same recovery can be performed again.

use std::{io::{Read, Seek, Write}, path::PathBuf};

use crate::write_ahead_log::write_ahead_log::WriteAheadLog;

const WAL_SIGNATURE: [u8; 24] = *b"SIMPLE_WAL_2025_7_13____";
const TOCK_FILE_SIGNATURE: [u8; 8] = *b"TOCK____";
const TICK_FILE_SIGNATURE: [u8; 8] = *b"TICK____";
const LOG_FILE_SIGNATURE: [u8; 8] = *b"LOG_____";
const META_FILE_SIGNATURE: [u8; 8] = *b"META____";

pub struct SimpleWal {
    tick_file: std::fs::File,
    tock_file: std::fs::File,
    log_file: std::fs::File,
    meta_file: std::fs::File,
}

impl SimpleWal {

    pub fn new_wal_at_directory(dir_path: PathBuf) -> Self {
        assert!(dir_path.exists(), "Directory does not exist: {:?}", dir_path);
        assert!(dir_path.is_dir(), "Path is not a directory: {:?}", dir_path);
        assert!(dir_path.read_dir().expect("Could not read directory").next().is_none(), "Directory is not empty: {:?}", dir_path);

        let tick_file_path = dir_path.join("wal.tick");
        let mut tick_file = std::fs::File::create(&tick_file_path)
            .expect("Failed to create tick file");

        let tock_file_path = dir_path.join("wal.tock");
        let mut tock_file = std::fs::File::create(&tock_file_path)
            .expect("Failed to create tock file");

        let log_file_path = dir_path.join("wal.log");
        let mut log_file = std::fs::File::create(&log_file_path)
            .expect("Failed to create log file");

        let meta_file_path = dir_path.join("wal.meta");
        let mut meta_file = std::fs::File::create(&meta_file_path)
            .expect("Failed to create meta file");

        tick_file.write_all(&WAL_SIGNATURE)
            .expect("Failed to write tick file signature");
        tick_file.write_all(&TICK_FILE_SIGNATURE)
            .expect("Failed to write tick file signature");

        tock_file.write_all(&WAL_SIGNATURE)
            .expect("Failed to write tock file signature");
        tock_file.write_all(&TOCK_FILE_SIGNATURE)
            .expect("Failed to write tock file signature");

        log_file.write_all(&WAL_SIGNATURE)
            .expect("Failed to write log file signature");
        log_file.write_all(&LOG_FILE_SIGNATURE)
            .expect("Failed to write log file signature");

        meta_file.write_all(&WAL_SIGNATURE)
            .expect("Failed to write meta file signature");
        meta_file.write_all(&META_FILE_SIGNATURE)
            .expect("Failed to write meta file signature");

        meta_file.write_all(&[0; 32]) // 32 bytes of zeros for the operational file indicator
            .expect("Failed to write operational file indicator");

        Self {
            tick_file,
            tock_file,
            log_file,
            meta_file,
        }
    }
    
    fn get_current_operational_file(&mut self) -> &std::fs::File {
        // Seek to the beginning of the meta file
        self.meta_file
            .seek(std::io::SeekFrom::Start(0))
            .expect("Failed to seek in meta file");

        // Read 32 bytes to check if it is a real meta file
        let mut signature : [u8; 32] = [0; 32];
        self.meta_file
            .read_exact(&mut signature)
            .expect("Failed to read meta file signature");
        // Check if the signature matches
        assert!(signature.starts_with(&WAL_SIGNATURE) && signature[24..32] == META_FILE_SIGNATURE,
            "Invalid meta file signature");
        // Read the next 32 bytes to determine the current operational file: all zeros means tick file, all ones means tock file
        let mut operational_file_indicator: [u8; 32] = [0; 32];
        self.meta_file
            .read_exact(&mut operational_file_indicator)
            .expect("Failed to read operational file indicator");
        if operational_file_indicator.iter().all(|&x| x == 0) {
            return &self.tick_file;
        } else if operational_file_indicator.iter().all(|&x| x == 1) {
            return &self.tock_file;
        } else {
            panic!("Invalid operational file indicator");
        }
    }

    fn write_log_entry(&mut self, stream_pos: u64, data: &Vec<u8>){
        self.log_file
            .write_all(&stream_pos.to_le_bytes())
            .expect("Failed to write stream position to log file");
        self.log_file
            .write_all(data.len().to_le_bytes().as_slice())
            .expect("Failed to write data length to log file");
        self.log_file
            .write_all(&data)
            .expect("Failed to write data to log file");
    }

    fn read_log_entry(&mut self) -> (u64, Vec<u8>) {
        let mut stream_pos_bytes = [0u8; 8];
        self.log_file
            .read_exact(&mut stream_pos_bytes)
            .expect("Failed to read stream position");
        let stream_pos = u64::from_le_bytes(stream_pos_bytes);

        let mut length_bytes = [0u8; 8];
        self.log_file
            .read_exact(&mut length_bytes)
            .expect("Failed to read length");
        let length = usize::from_le_bytes(length_bytes);

        let mut data = vec![0u8; length];
        self.log_file
            .read_exact(&mut data)
            .expect("Failed to read data");

        (stream_pos, data)
    }

}

impl WriteAheadLog for SimpleWal {
    fn read(&mut self, size: u64) -> Result<Vec<u8>, std::io::Error> {
        let mut buffer = Vec::with_capacity(size as usize);
        self.get_current_operational_file()
            .read_exact(&mut buffer)?;
        Ok(buffer)
    }

    fn write(&mut self, buf: Vec<u8>) -> Result<(), std::io::Error> {
        // 1. A log entry is created and written to the log file.
        let stream_pos = self.get_current_operational_file()
            .stream_position()
            .expect("Failed to get stream position");
        self.write_log_entry(stream_pos, &buf);
        self.log_file
            .sync_all()
            .expect("Failed to sync log file");
        // 2. Then the data is written to the current operational file.
        self.get_current_operational_file()
            .write_all(buf.as_slice())
            .expect("Failed to write data to operational file");
        self.get_current_operational_file()
            .sync_all()
            .expect("Failed to sync operational file");
        Ok(())
    }

    fn seek(&mut self, pos: std::io::SeekFrom) -> Result<(), std::io::Error> {
        self.get_current_operational_file()
            .seek(pos)?;
        Ok(())
    }

    fn stream_len(&mut self) -> Result<u64, std::io::Error> {
        let len = self.get_current_operational_file()
            .metadata()?
            .len();
        Ok(len)
    }

    fn stream_position(&mut self) -> Result<u64, std::io::Error> {
        let pos = self.get_current_operational_file()
            .stream_position()?;
        Ok(pos)
    }

    fn set_len(&mut self, size: u64) -> Result<(), std::io::Error> {
        self.get_current_operational_file()
            .set_len(size)?;
        Ok(())
    }

    fn atomic_checkpoint(&mut self) -> Result<(), std::io::Error> {
        // 1. The current operational file in wal.meta is switched to the fallback.
        self.meta_file
            .seek(std::io::SeekFrom::Start(0))
            .expect("Failed to seek in meta file");
        // first read the signature to check if it is a real meta file
        let mut signature: [u8; 32] = [0; 32];
        self.meta_file
            .read_exact(&mut signature)
            .expect("Failed to read meta file signature");
        // Check if the signature matches
        assert!(signature.starts_with(&WAL_SIGNATURE) && signature[24..32] == META_FILE_SIGNATURE,
            "Invalid meta file signature");
        // then read the current operational file indicator and switch it
        let mut operational_file_indicator = [0u8; 32];
        self.meta_file
            .read_exact(&mut operational_file_indicator)
            .expect("Failed to read operational file indicator");
        if operational_file_indicator.iter().all(|&x| x == 0) {
            operational_file_indicator.fill(1);
        } else if operational_file_indicator.iter().all(|&x| x == 1) {
            operational_file_indicator.fill(0);
        } else {
            panic!("Invalid operational file indicator");
        }
        self.meta_file
            .seek(std::io::SeekFrom::Start(0))
            .expect("Failed to seek in meta file");
        self.meta_file
            .write_all(&WAL_SIGNATURE)
            .expect("Failed to write meta file signature");
        self.meta_file
            .write_all(&META_FILE_SIGNATURE)
            .expect("Failed to write meta file signature");
        self.meta_file
            .write_all(&operational_file_indicator)
            .expect("Failed to write operational file indicator");
        self.meta_file
            .sync_all()
            .expect("Failed to sync meta file");
        // 2. Iterate over the log file and apply all log entries to the new operational file.
        self.log_file
            .seek(std::io::SeekFrom::Start(0))
            .expect("Failed to seek in log file");
        while self.log_file.stream_position().unwrap() < self.log_file.metadata().unwrap().len() {
            let (stream_pos, data) = self.read_log_entry();
            // Write the data to the current operational file at the correct position
            self.get_current_operational_file()
                .seek(std::io::SeekFrom::Start(stream_pos))
                .expect("Failed to seek in operational file");
            self.get_current_operational_file()
                .write_all(&data)
                .expect("Failed to write data to operational file");
        }
        self.get_current_operational_file()
            .sync_all()
            .expect("Failed to sync operational file");
        // 3. Erase all log entries in the log file.
        self.log_file
            .set_len(0)
            .expect("Failed to erase log file");
        self.log_file
            .sync_all()
            .expect("Failed to sync log file");
        Ok(())
    }
}