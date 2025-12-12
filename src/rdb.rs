use std::{
    fs::File,
    io::{BufReader, Read},
};

struct RecordingReader {
    reader: BufReader<File>,
    memory: Vec<u8>,
    peeked: Vec<u8>,
}

impl RecordingReader {
    fn new(filepath: &String) -> Result<Self, Error> {
        let file = File::open(filepath)?;
        let reader = BufReader::new(file);
        Ok(Self {
            reader,
            memory: vec![],
            peeked: vec![],
        })
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), std::io::Error> {
        self.read_exact_no_memory(buf)?;
        self.memory.append(&mut buf.to_vec());
        Ok(())
    }

    fn read_exact_no_memory(&mut self, buf: &mut [u8]) -> Result<(), std::io::Error> {
        let req_len = buf.len();
        let peeked_len = self.peeked.len();

        for i in 0..req_len.min(peeked_len) {
            let byte = self.peeked.remove(0);
            self.memory.push(byte);
            buf[i] = byte;
        }

        if peeked_len < req_len {
            let extra_len = req_len - peeked_len;
            let mut inner_buf = Vec::with_capacity(extra_len);
            inner_buf.resize(extra_len, 0);
            self.reader.read_exact(&mut inner_buf[..])?;

            for i in 0..extra_len {
                buf[peeked_len + i] = inner_buf[i];
            }
        }

        Ok(())
    }

    fn peek(&mut self, n: usize) -> Result<Vec<u8>, Error> {
        if self.peeked.len() < n {
            let missing_len = n - self.peeked.len();
            let mut buf = Vec::with_capacity(missing_len);
            buf.resize(missing_len, 0u8);
            self.reader.read_exact(&mut buf[..])?;
            self.peeked.append(&mut buf);
        }

        Ok(self.peeked[0..n].to_vec())
    }
}

use crate::common::Error;

#[derive(Debug)]
pub(crate) enum VariableLenString {
    Str(String),
    I8(i8),
    I16(i16),
    I32(i32),
    NotALength(u8),
}

pub(crate) type AuxKeyValuePair = (String, VariableLenString);

#[derive(Debug, Default)]
pub(crate) struct RdbContent {
    version: Option<u16>,
    aux_fields: Vec<AuxKeyValuePair>,
    db_selector: Option<u8>,
    hash_table_size: Option<usize>,
    expiry_hash_table_size: Option<usize>,
}

pub(crate) struct RdbFile {
    filepath: String,
}

impl RdbFile {
    pub(crate) fn new(filepath: String) -> Self {
        Self { filepath }
    }

    pub(crate) fn read(&self) -> Result<RdbContent, Error> {
        let mut reader = RecordingReader::new(&self.filepath)?;
        let mut content = RdbContent::default();

        let mut general_buffer: [u8; 64] = [0; 64];
        reader.read_exact(&mut general_buffer[0..5])?;

        if &general_buffer[0..5] != b"REDIS" {
            return Err("Missing magic string at beginning (REDIS)".into());
        }

        reader.read_exact(&mut general_buffer[0..4])?;
        let version_number_str = String::from_utf8(general_buffer[0..4].to_vec())?;
        debug!("Version number: {}", version_number_str);
        content.version = Some(u16::from_str_radix(&version_number_str, 10)?);

        reader.read_exact(&mut general_buffer[0..1])?;
        let header = general_buffer[0];

        Self::read_section(header, &mut reader, &mut content)?;

        Ok(content)
    }

    fn read_section(
        header: u8,
        reader: &mut RecordingReader,
        content: &mut RdbContent,
    ) -> Result<(), Error> {
        match header {
            0xFF => Self::read_eof(reader)?,
            0xFE => Self::read_db_section(reader, content)?,
            0xFD => unimplemented!("EXPIRETIME"),
            0xFC => unimplemented!("EXPIRETIMEMS"),
            0xFB => Self::read_resize_db(reader, content)?,
            0xFA => Self::read_aux_section(reader, content)?,
            _ => return Err("Unknown section header".into()),
        }

        Ok(())
    }

    fn read_db_section(
        reader: &mut RecordingReader,
        content: &mut RdbContent,
    ) -> Result<(), Error> {
        match Self::read_variable_len_str(reader)? {
            VariableLenString::I8(v) => content.db_selector = Some(v as u8),
            _ => unimplemented!("Unsupported db selector"),
        }
        Ok(())
    }

    fn read_resize_db(reader: &mut RecordingReader, content: &mut RdbContent) -> Result<(), Error> {
        match Self::read_variable_len_str(reader)? {
            VariableLenString::I8(v) => content.hash_table_size = Some(v as usize),
            VariableLenString::I16(v) => content.hash_table_size = Some(v as usize),
            VariableLenString::I32(v) => content.hash_table_size = Some(v as usize),
            _ => panic!("Unexpected type for hash table size"),
        }

        match Self::read_variable_len_str(reader)? {
            VariableLenString::I8(v) => content.expiry_hash_table_size = Some(v as usize),
            VariableLenString::I16(v) => content.expiry_hash_table_size = Some(v as usize),
            VariableLenString::I32(v) => content.expiry_hash_table_size = Some(v as usize),
            _ => panic!("Unexpected type for expiry hash table size"),
        }

        Ok(())
    }

    fn read_aux_section(
        reader: &mut RecordingReader,
        content: &mut RdbContent,
    ) -> Result<(), Error> {
        loop {
            let key = Self::read_variable_len_str(reader)?;
            if let VariableLenString::NotALength(next_header) = key {
                return Self::read_section(next_header, reader, content);
            }

            let VariableLenString::Str(key) = key else {
                panic!("Expected string for aux key");
            };

            let value = Self::read_variable_len_str(reader)?;
            if let VariableLenString::NotALength(byte) = value {
                panic!("Expected Aux value for key. Found: {:b}", byte);
            }

            content.aux_fields.push((key, value));
        }
    }

    fn read_variable_len_str(reader: &mut RecordingReader) -> Result<VariableLenString, Error> {
        let mut buf: [u8; 8] = [0; 8];
        reader.read_exact(&mut buf[0..1])?;

        let lead_bits = buf[0] >> 6;
        match lead_bits {
            0b00 => {
                let len = (buf[0] & 0b0011_1111) as usize;
                Ok(VariableLenString::Str(Self::read_string_of_len(
                    reader, len,
                )?))
            }
            0b01 => {
                let lhs = ((buf[0] & 0b0011_1111) as usize) << 8;
                reader.read_exact(&mut buf[0..1])?;
                let rhs = buf[0] as usize;
                let len = lhs + rhs;
                Ok(VariableLenString::Str(Self::read_string_of_len(
                    reader, len,
                )?))
            }
            0b10 => {
                reader.read_exact(&mut buf[0..4])?;
                let len = (u32::from_le_bytes(buf[0..3].try_into()?)) as usize;
                Ok(VariableLenString::Str(Self::read_string_of_len(
                    reader, len,
                )?))
            }
            0b11 => match buf[0] & 0b0011_1111 {
                0 => {
                    let mut buf = [0u8; 1];
                    reader.read_exact(&mut buf)?;
                    Ok(VariableLenString::I8(i8::from_le_bytes(buf.try_into()?)))
                }
                1 => {
                    let mut buf = [0u8; 2];
                    reader.read_exact(&mut buf)?;
                    Ok(VariableLenString::I16(i16::from_le_bytes(buf.try_into()?)))
                }
                2 => {
                    let mut buf = [0u8; 4];
                    reader.read_exact(&mut buf)?;
                    Ok(VariableLenString::I32(i32::from_le_bytes(buf.try_into()?)))
                }
                3 => unimplemented!("LZF encoded strings are not yet implemented"),
                _ => Ok(VariableLenString::NotALength(buf[0])),
            },
            _ => panic!("Unexpected"),
        }
    }

    fn read_string_of_len(reader: &mut RecordingReader, len: usize) -> Result<String, Error> {
        let mut buf = Vec::with_capacity(len);
        buf.resize(len, 0u8);
        reader.read_exact(&mut buf[0..len])?;
        Ok(String::from_utf8(buf)?)
    }

    fn read_eof(reader: &mut RecordingReader) -> Result<(), Error> {
        let mut buf = [0u8; 8];
        reader.read_exact_no_memory(&mut buf)?;
        let expected_checksum = u64::from_le_bytes(buf);

        let crc = crc::Crc::<u64>::new(&crc::CRC_64_REDIS);
        let mut crc_digest = crc.digest();
        crc_digest.update(&reader.memory);
        let actual_checksum = crc_digest.finalize();

        if expected_checksum == actual_checksum {
            Ok(())
        } else {
            Err("Checksum error".into())
        }
    }
}

#[cfg(test)]
mod test {
    use std::io::Write;

    use crate::rdb::RdbFile;

    #[test]
    fn test_reading_empty() {
        create_empty_rdb_file();
        let rdb = RdbFile::new("/tmp/rdb".into());

        let content = rdb.read().unwrap();
        dbg!(content);
    }

    // ---

    fn create_empty_rdb_file() {
        let fake_rdb_file_bytes_str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
        let fake_rdb_file_bytes = (0..fake_rdb_file_bytes_str.len() / 2)
            .into_iter()
            .map(|i| {
                u8::from_str_radix(&fake_rdb_file_bytes_str[(i * 2)..=(i * 2) + 1], 16).unwrap()
            })
            .collect::<Vec<_>>();

        let mut file = std::fs::File::create("/tmp/rdb").unwrap();
        file.write_all(&fake_rdb_file_bytes).unwrap();
    }
}
