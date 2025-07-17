use std::io::{Error, SeekFrom};

use crate::{object_stream::object_stream::ObjectStream, write_ahead_log::write_ahead_log::WriteAheadLog};

#[warn(dead_code)]
struct FileObjectStreamWALImpl {
    wal: dyn WriteAheadLog
}

impl ObjectStream for FileObjectStreamWALImpl {
    
    fn pos(&self) -> Result<u64, Error> {
        //position vom vec cursor holen
        todo!()
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<(), Error> {
        //position im vec cursor setzen 
        todo!()
    }

    fn read(&mut self) -> Result<Vec<u8>, Error> {
        //element von vec lesen für pos
        //als erstes opcode lesen = länge 
        //dann daten lesen 
        //dann pos vorrücken
        //dann daten zurückgeben
        todo!()
    }

    fn len(&self) -> Result<u64, Error> {
        //opcode lesen
        //dann datenlänge zurückgeben
        todo!()
    }

    fn overwrite(&mut self, data: Vec<u8>) -> Result<(), Error> {
        //opcode lesen
        //datenlänge vergleichen
        //dann daten überschreiben
        //dann pos vorrücken
        //dann Ok zurückgeben
        todo!()
    }

    fn split(&mut self, first_object_size: u64) -> Result<(), Error> {
        //pos merken

        //opcodeAlt lesen
        //dann opcodeAlt > first_object_size + opcode_länge
        //DIRTY: opcodeEins überschreiben = first_object_size
        //dann seek zu opcodeZwei (filePos Merken)
        //dann opcodeZwei == opcodeAlt - (first_object_size + opcode_len)

        //gemerkte filepos hinzufügen bei vec und anschließend sortieren aufsteigend

        //pos = pos merken
        todo!()
    }

    fn merge(&mut self) -> Result<(), Error> {
        //pos merken

        //opcodeAlt lesen
        //opcodeAltNext lesen 

        //opcodeNeu = opcodeAlt + opcodeAltNext - opcode_länge

        //remove posNext in vec

        todo!()
    }

    fn opcode() -> u8 {
        //return opcode len
        todo!()
    }

    fn append(&mut self, data: Vec<u8>) -> Result<(), Error> {
        //opcode erstellen
        //dann merge opcode + data
        //dann fileEnd 
        //dann append merged
        //add pos to vec
        //dann Ok zurückgeben
        todo!()
    }

    fn cut(&mut self) -> Result<(), Error> {
        //get pos 
        //get filePos from vec
        //remove alles nach filePos
        //dann Ok zurückgeben
        todo!()
    }

    fn checkpoint(&mut self) -> Result<(), Error> {
        //checkpoint machen
        todo!()
    }
}