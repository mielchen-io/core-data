use std::io::{Error, SeekFrom};

pub trait ObjectStream {

    /// Return current object position
    fn pos(&self) -> Result<u64, Error>;

    /// Seek to the given object in the stream.
    fn seek(&mut self, pos: SeekFrom) -> Result<(), Error>;

    /// Read the next object from the stream.
    fn read(&mut self) -> Result<Vec<u8>, Error>;

    /// Return the data len of the current object.
    fn len(&self) -> Result<u64, Error>;

    /// Overwrite the current object with the given object-data.
    fn overwrite(&mut self, data: Vec<u8>) -> Result<(), Error>;

    /// Split the object the object in two
    fn split(&mut self, first_object_size: u64) -> Result<(), Error>;

    /// Combine next two objects (You will get a ugly object)
    fn merge(&mut self) -> Result<(), Error>;

    /// Return size of op_code used for an object in bytes
    fn opcode() -> u8;

    /// Append an object to the end of the stream.
    fn append(&mut self, data: Vec<u8>) -> Result<(), Error>;

    /// Delete the current and all following objects.
    fn cut(&mut self) -> Result<(), Error>;
}