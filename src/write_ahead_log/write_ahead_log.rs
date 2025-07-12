use std::io::{Error, SeekFrom};

pub trait WriteAheadLog {

    fn read(&mut self, size: u64) -> Result<Vec<u8>, Error>;

    fn write(&mut self, buf: Vec<u8>) -> Result<(), Error>;

    fn seek(&mut self, pos: SeekFrom) -> Result<(), Error>;

    fn stream_len(&mut self) -> Result<u64, Error>;

    fn stream_position(&mut self) -> Result<u64, Error>;

    fn atomic_checkpoint(&mut self) -> Result<(), Error>;

    fn set_len(&mut self, size: u64) -> Result<(), Error>;
}