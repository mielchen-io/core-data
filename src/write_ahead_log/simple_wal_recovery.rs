use std::{fs::File, io::{Read, Seek}, path::PathBuf};

use crate::write_ahead_log::simple_wal::SimpleWal;

impl SimpleWal {

    
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
        let tick_file = open_file_with_permissions(&tick_file_path);

        let tock_file_path = dir_path.join("wal.tock");
        let tock_file = open_file_with_permissions(&tock_file_path);

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
            let (operational_file, fallback_file) = self.get_operational_and_fallback_file();
            
            io::copy(fallback_file, operational_file)
                .expect("Failed to copy fallback file to operational file");
            operational_file.sync_all()
                .expect("Failed to sync operational file after copying fallback");
        } else if !valid_indicator && dir_path.join("wal.tick").exists() {
            // If the operational file indicator is not valid and wal.tick exists, do a Recovery Type B
            todo!("Recovery Type B: Implement the recovery logic for type B");
        } 

        return Self {
            tick_file,
            tock_file,
            log_file,
            meta_file,
        };
    }

    fn get_operational_and_fallback_file(&mut self) -> (&File, &File) {
        self.meta_file
            .seek(std::io::SeekFrom::Start(0))
            .expect("Failed to seek in meta file");
        let mut operational_file_indicator: [u8; 32] = [0; 32];
        self.meta_file
            .read_exact(&mut operational_file_indicator)
            .expect("Failed to read operational file indicator");
        if operational_file_indicator.iter().all(|&x| x == 0) {
            return (&self.tick_file, &self.tock_file);
        } else if operational_file_indicator.iter().all(|&x| x == 1) {
            return (&self.tock_file, &self.tick_file);
        } else {
            panic!("Invalid operational file indicator");
        }
    }
}



fn open_file_with_permissions(path: &PathBuf) -> std::fs::File {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("Failed to open one of the WAL files")
}