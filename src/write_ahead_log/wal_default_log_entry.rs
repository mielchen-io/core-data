use std::io::{Read, Write};

use crate::write_ahead_log::wal_default::WriteAheadLogDefault;

pub(in crate::write_ahead_log) enum LogEntry{
    Write(u64, Vec<u8>),
    SetLen(u64),
}

impl LogEntry {
    pub(in crate::write_ahead_log) fn get_data(&self) -> &Vec<u8> {
        if let LogEntry::Write(_, data) = self {
            data
        } else {
            panic!("LogEntry does not contain data");
        }
    }
}

impl WriteAheadLogDefault {

    pub(in crate::write_ahead_log) fn write_log_entry(&mut self, log_entry: &LogEntry) {
        match log_entry {
            LogEntry::Write(stream_pos, data) => {
                self.log_file
                    .write_all(b"WR") 
                    .expect("Failed to write log entry opcode");
                self.log_file
                    .write_all(&stream_pos.to_le_bytes())
                    .expect("Failed to write stream position to log file");
                self.log_file
                    .write_all(data.len().to_le_bytes().as_slice())
                    .expect("Failed to write data length to log file");
                self.log_file
                    .write_all(data)
                    .expect("Failed to write data to log file");
                    }
            LogEntry::SetLen(size) => {
                self.log_file
                    .write_all(b"SL") 
                    .expect("Failed to write log entry opcode");
                self.log_file
                    .write_all(&size.to_le_bytes())
                    .expect("Failed to write size to log file");
            }
        }
    }

    pub(in crate::write_ahead_log) fn read_log_entry(&mut self) -> LogEntry {
        
        let mut opcode = [0u8; 2];
        self.log_file
            .read_exact(&mut opcode)
            .expect("Failed to read log entry opcode");

        match &opcode {
            b"WR" => { // Write operation
                let mut stream_pos_bytes = [0u8; 8];
                self.log_file
                    .read_exact(&mut stream_pos_bytes)
                    .expect("Failed to read stream position from log file");
                let stream_pos = u64::from_le_bytes(stream_pos_bytes);

                let mut data_length_bytes = [0u8; 8];
                self.log_file
                    .read_exact(&mut data_length_bytes)
                    .expect("Failed to read data length from log file");
                let data_length = u64::from_le_bytes(data_length_bytes) as usize;

                let mut data = vec![0u8; data_length];
                self.log_file
                    .read_exact(&mut data)
                    .expect("Failed to read data from log file");

                LogEntry::Write(stream_pos, data)
            }
            b"SL" => { // SetLen operation
                let mut size_bytes = [0u8; 8];
                self.log_file
                    .read_exact(&mut size_bytes)
                    .expect("Failed to read size from log file");
                let size = u64::from_le_bytes(size_bytes);
                LogEntry::SetLen(size)
            }
            _ => panic!("Unknown log entry opcode encountered"),
        }
    }
    
}