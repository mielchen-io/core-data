//! This WAL implementation consists of 4 files:
//! - wal.tick and wal.tock contain the persistent data and always one of them is the fallback and the other is the operational file.
//! - wal.log contains log entries since the last checkpoint
//! - wal.meta contains a signature that indicates the current operational file
//! 
//! On every write operation or set_len, the following steps are performed:
//! 1. A log entry is created and written to the log file. Recovery Type A
//! 2. Then the operation is persited on the operational file. Recovery Type A
//! 
//! On checkpoint, the following steps are performed:
//! 1. The current operational file in wal.meta is switched to the fallback. Recovery Type B
//! 2. Iterate over the log file and apply all log entries to the new operational file. Recovery Type A
//! 3. Erase all log entries in the log file. Recovery Type A
//! 
//! After every numbered step, fsync syscall is used to ensure durability.
//! 
//! The Recovery Type for each step specifies how a faulty state is detected if the system crashes during this step and how the system can recover from it:
//! - Recovery Type A: A faulty state is detected when wal.meta contains a valid signature but wal.log is not empty. The system can recover by erasing the current operational file, generating it again as a copy of the fallback and afterwards erasing the log file. If this process is interrupted by a crash, the same recovery can be performed again.
//! - Recovery Type B: A faulty state is detected when wal.meta does not contain a valid signature. The system can recover by erasing wal.tick and generating it again as a copy of wal.tock. Then the log file is erased. Then the wal.meta is updated with a new valid signature pointing to wal.tock. If this process is interrupted by a crash, recovery is still possible.

use std::{fs::File, io::{Read, Seek, SeekFrom, Write}, path::PathBuf};

use crate::write_ahead_log::{wal_default_log_entry::LogEntry, wal_trait::WriteAheadLog};
use crate::write_ahead_log::wal_default_file_io::create_file_with_permissions;

pub struct WriteAheadLogDefault {
    pub(in crate::write_ahead_log) tick_file: std::fs::File,
    pub(in crate::write_ahead_log) tock_file: std::fs::File,
    pub(in crate::write_ahead_log) log_file: std::fs::File,
    pub(in crate::write_ahead_log) meta_file: std::fs::File,
}

impl WriteAheadLogDefault {

    pub fn new_wal_at_directory(dir_path: PathBuf) -> Self {
        assert!(dir_path.exists(), "Directory does not exist: {:?}", dir_path);
        assert!(dir_path.is_dir(), "Path is not a directory: {:?}", dir_path);
        assert!(dir_path.read_dir().expect("Could not read directory").next().is_none(), "Directory is not empty: {:?}", dir_path);

        let tick_file_path = dir_path.join("wal.tick");
        let tick_file = create_file_with_permissions(&tick_file_path);

        let tock_file_path = dir_path.join("wal.tock");
        let tock_file = create_file_with_permissions(&tock_file_path);

        let log_file_path = dir_path.join("wal.log");
        let log_file = create_file_with_permissions(&log_file_path);

        let meta_file_path = dir_path.join("wal.meta");
        let mut meta_file = create_file_with_permissions(&meta_file_path);
        meta_file.write_all(&[0; 32]).expect("Failed to write operational file indicator"); // 32 bytes of zeros for the operational file indicator
        meta_file.sync_all().expect("Failed to sync meta file");

        Self {
            tick_file,
            tock_file,
            log_file,
            meta_file,
        }
    }

    fn get_current_operational_file(&mut self) -> &File {
        self.meta_file
            .seek(std::io::SeekFrom::Start(0))
            .expect("Failed to seek in meta file");
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
    
}

impl WriteAheadLog for WriteAheadLogDefault {
    fn read(&mut self, size: u64) -> Vec<u8> {
        let mut buffer = vec![0u8; size as usize];
        self.get_current_operational_file()
            .read_exact(&mut buffer).expect("Failed to read data from operational file during a WAL read operation");
        buffer
    }

    fn write(&mut self, buf: Vec<u8>){
        // 1. A log entry is created and written to the log file
        let stream_pos = self.get_current_operational_file()
            .stream_position()
            .expect("Failed to get stream position");
        let log_entry = LogEntry::Write(stream_pos, buf);
        self.write_log_entry(&log_entry);
        self.log_file
            .sync_all()
            .expect("Failed to sync log file");
        // 2. Then the data is written to the current operational file
        self.get_current_operational_file()
            .write_all(log_entry.get_data())
            .expect("Failed to write data to operational file");
        self.get_current_operational_file()
            .sync_all()
            .expect("Failed to sync operational file");
    }

    fn seek(&mut self, pos: std::io::SeekFrom){
        self.get_current_operational_file()
            .seek(pos)
            .expect("Failed to seek in operational file during a WAL seek operation");
    }

    fn stream_len(&mut self) -> u64{
        let len = self.get_current_operational_file()
            .metadata()
            .expect("Failed to get metadata of operational file during a WAL stream_len operation")
            .len();
        return len;
    }

    fn stream_position(&mut self) -> u64{
        let pos = self.get_current_operational_file()
            .stream_position()
            .expect("Failed to get stream position of operational file during a WAL stream_position operation");
        return pos;
    }

    fn set_len(&mut self, size: u64){
        // 1. A log entry is created and written to the log file
        let log_entry = LogEntry::SetLen(size);
        self.write_log_entry(&log_entry);
        self.log_file
            .sync_all()
            .expect("Failed to sync log file");
        // 2. Then the size is set in the current operational file
        self.get_current_operational_file()
            .set_len(size)
            .expect("Failed to set length of operational file");
        self.get_current_operational_file()
            .sync_all()
            .expect("Failed to sync operational file");
    }

    fn atomic_checkpoint(&mut self){
        // save the currrent seek position of the operational file
        let current_seek_pos = self.get_current_operational_file()
            .stream_position()
            .expect("Failed to get current seek position");
        // 1. The current operational file in wal.meta is switched to the fallback
        let mut operational_file_indicator = [0u8; 32];
        self.meta_file
            .seek(std::io::SeekFrom::Start(0))
            .expect("Failed to seek in meta file");
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
            .write_all(&operational_file_indicator)
            .expect("Failed to write operational file indicator");
        self.meta_file
            .sync_all()
            .expect("Failed to sync meta file");
        // 2. Iterate over the log file and apply all log entries to the new operational file
        self.log_file
            .seek(std::io::SeekFrom::Start(0))
            .expect("Failed to seek in log file");
        while self.log_file.stream_position().unwrap() < self.log_file.metadata().unwrap().len() {
            let log_entry = self.read_log_entry();
            match log_entry {
                LogEntry::Write(stream_pos, data) => {
                    self.get_current_operational_file()
                        .seek(std::io::SeekFrom::Start(stream_pos))
                        .expect("Failed to seek in operational file");
                    self.get_current_operational_file()
                        .write_all(&data)
                        .expect("Failed to write data to operational file");
                }
                LogEntry::SetLen(size) => {
                    self.get_current_operational_file()
                        .set_len(size)
                        .expect("Failed to set length of operational file");
                }
            }
        }
        self.get_current_operational_file()
            .sync_all()
            .expect("Failed to sync operational file");
        // 3. Erase all log entries in the log file
        self.log_file
            .seek(SeekFrom::Start(0))
            .expect("Failed to seek in log file");
        self.log_file
            .set_len(0)
            .expect("Failed to erase log file");
        self.log_file
            .sync_all()
            .expect("Failed to sync log file");
        // Restore the seek position of the operational file
        self.get_current_operational_file()
            .seek(SeekFrom::Start(current_seek_pos))
            .expect("Failed to restore seek position in operational file");
    }
}





