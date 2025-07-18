use std::{fs::File, io::{self, Read, Seek, SeekFrom, Write}, path::PathBuf};

use crate::write_ahead_log::write_ahead_log_default::WriteAheadLogDefault;

impl WriteAheadLogDefault {

    
    /// Opens an existing WAL at the specified directory.
    /// 
    /// Recovery will be attempted which means that this function call can take a long time if the WAL is in a faulty state.
    /// If the WAL is in a valid state, it will return immediately and if the WAL can not be recovered, it will panic.
    pub fn open_wal_at_directory(dir_path: PathBuf) -> Self {
        assert!(dir_path.exists(), "Directory does not exist: {:?}", dir_path);
        assert!(dir_path.is_dir(), "Path is not a directory: {:?}", dir_path);

        assert!(dir_path.join("wal.tick").exists(), "wal.tick does not exist in directory: {:?}", dir_path);
        assert!(dir_path.join("wal.tock").exists(), "wal.tock does not exist in directory: {:?}", dir_path);

        assert!(dir_path.join("wal.log").exists(), "wal.log does not exist in directory: {:?}", dir_path);
        assert!(dir_path.join("wal.meta").exists(), "wal.meta does not exist in directory: {:?}", dir_path);

        let tick_file_path = dir_path.join("wal.tick");
        let mut tick_file = open_file_with_permissions(&tick_file_path);

        let tock_file_path = dir_path.join("wal.tock");
        let mut tock_file = open_file_with_permissions(&tock_file_path);

        let log_file_path = dir_path.join("wal.log");
        let log_file = open_file_with_permissions(&log_file_path);

        let meta_file_path = dir_path.join("wal.meta");
        let mut meta_file = open_file_with_permissions(&meta_file_path);

        let mut operational_file_indicator = [0u8; 32];
        meta_file
            .seek(std::io::SeekFrom::Start(0))
            .expect("Failed to seek in meta file");
        meta_file
            .read_exact(&mut operational_file_indicator)
            .expect("Failed to read operational file indicator");
        let valid_indicator = operational_file_indicator.iter().all(|&x| x == 0) || operational_file_indicator.iter().all(|&x| x == 1);
        let log_file_len = log_file.metadata().expect("Failed to get log file metadata").len();

        
        if valid_indicator && log_file_len > 0 {
            // If the operational file indicator is valid but the log file is not empty, do a Recovery Type A
             // 1. Copy the the fallback to the operational file
            let operational_file: &mut File;
            let fallback_file: &mut File;
            if operational_file_indicator.iter().all(|&x| x == 0) {
                operational_file = &mut tick_file;
                fallback_file = &mut tock_file;
            } else {
                operational_file = &mut tock_file;
                fallback_file = &mut tick_file;
            }
            operational_file
                .seek(SeekFrom::Start(0))
                .expect("Failed to seek in operational file");
            fallback_file
                .seek(SeekFrom::Start(0))
                .expect("Failed to seek in fallback file");
            operational_file.set_len(0).expect("Failed to erase operational file");
            io::copy(fallback_file, operational_file).expect("Failed to copy fallback file to operational file");
            operational_file.sync_all().expect("Failed to sync operational file");
            // 2. Erase the log file
            log_file.set_len(0).expect("Failed to erase log file");
            log_file.sync_all().expect("Failed to sync log file");
        } else if !valid_indicator && dir_path.join("wal.tick").exists() {
            // If the operational file indicator is not valid, do a Recovery Type B
            // 1. Copy the tock file to the tick file
            tock_file
                .seek(SeekFrom::Start(0))
                .expect("Failed to seek in tock file");
            tick_file
                .seek(SeekFrom::Start(0))
                .expect("Failed to seek in tick file");
            tick_file.set_len(0).expect("Failed to erase tick file");
            io::copy(&mut tock_file, &mut tick_file).expect("Failed to copy tock file to tick file");
            tick_file.sync_all().expect("Failed to sync tick file");
            // 2. Update the operational file indicator in the meta file (write all ones)
            meta_file
                .seek(SeekFrom::Start(0))
                .expect("Failed to seek in meta file");
            meta_file
                .write_all(&[1u8; 32])
                .expect("Failed to write operational file indicator");
            meta_file.sync_all().expect("Failed to sync meta file");
            // 3. Erase the log file
            log_file.set_len(0).expect("Failed to erase log file");
            log_file.sync_all().expect("Failed to sync log file");
        } 

        return Self {
            tick_file,
            tock_file,
            log_file,
            meta_file,
        };
    }

   
}

fn open_file_with_permissions(path: &PathBuf) -> std::fs::File {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("Failed to open one of the WAL files")
}