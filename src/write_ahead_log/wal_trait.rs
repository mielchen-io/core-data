use std::io::{SeekFrom};

pub trait WriteAheadLog {

    fn read(&mut self, size: u64) -> Vec<u8>;

    fn write(&mut self, buf: Vec<u8>);

    fn seek(&mut self, pos: SeekFrom);

    fn stream_len(&mut self) -> u64;

    fn stream_position(&mut self) -> u64;

    fn atomic_checkpoint(&mut self);

    fn set_len(&mut self, size: u64);
}