use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io;

#[derive(Debug, Default)]
pub struct YcsbPayload {
    pub _timestamp: u64,
    pub indices: [u32; 10],
    pub spin_usec: u16,
}

pub const YCSB_PAYLOAD_SIZE: usize = 56;

impl YcsbPayload {
    pub fn serialize_into<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_u64::<LittleEndian>(self._timestamp)?;
        Ok(())
    }

    pub fn deserialize<R: io::Read>(reader: &mut R) -> io::Result<YcsbPayload> {
        let p = YcsbPayload {
            _timestamp: reader.read_u64::<LittleEndian>()?,
            indices: {
                let mut indices = [0u32; 10];
                for i in 0..10 {
                    indices[i] = reader.read_u32::<LittleEndian>()?;
                    // println!("{}", indices[i]);
                }
            indices
            },
            spin_usec: reader.read_u16::<LittleEndian>()?,
        };
        return Ok(p);
    }
}
