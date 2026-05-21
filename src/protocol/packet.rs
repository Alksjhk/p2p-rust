/// PTP binary packet protocol.
///
/// Wire format:
///   [4B magic] [1B flags] [2B stream_id] [2B seq_num] [2B ack_num] [payload]
///     0x505450 ("PTP")

pub const MAGIC: [u8; 4] = [0x50, 0x54, 0x50, 0x00];
pub const HEADER_LEN: usize = 11;

// Flag bits
pub const FLAG_SYN: u8 = 0x01;
pub const FLAG_ACK: u8 = 0x02;
pub const FLAG_DATA: u8 = 0x04;
pub const FLAG_FIN: u8 = 0x08;
pub const FLAG_RST: u8 = 0x10;
pub const FLAG_PING: u8 = 0x20;
pub const FLAG_PONG: u8 = 0x40;

pub const CONTROL_STREAM: u16 = 0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Packet {
    pub flags: u8,
    pub stream_id: u16,
    pub seq_num: u16,
    pub ack_num: u16,
    pub payload: Vec<u8>,
}

impl Packet {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(HEADER_LEN + self.payload.len());
        buf.extend_from_slice(&MAGIC);
        buf.push(self.flags);
        buf.extend_from_slice(&self.stream_id.to_be_bytes());
        buf.extend_from_slice(&self.seq_num.to_be_bytes());
        buf.extend_from_slice(&self.ack_num.to_be_bytes());
        buf.extend_from_slice(&self.payload);
        buf
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < HEADER_LEN || data[..4] != MAGIC {
            return None;
        }
        Some(Self {
            flags: data[4],
            stream_id: u16::from_be_bytes([data[5], data[6]]),
            seq_num: u16::from_be_bytes([data[7], data[8]]),
            ack_num: u16::from_be_bytes([data[9], data[10]]),
            payload: data[HEADER_LEN..].to_vec(),
        })
    }

    pub fn has_flag(&self, flag: u8) -> bool {
        self.flags & flag != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {
        let p = Packet {
            flags: FLAG_DATA | FLAG_ACK,
            stream_id: 42,
            seq_num: 1,
            ack_num: 0,
            payload: vec![1, 2, 3],
        };
        assert_eq!(Packet::decode(&p.encode()).unwrap(), p);
    }

    #[test]
    fn test_decode_too_short() {
        assert!(Packet::decode(&[0; 10]).is_none());
    }

    #[test]
    fn test_decode_bad_magic() {
        assert!(Packet::decode(&[0xff; 11]).is_none());
    }

    #[test]
    fn test_empty_payload() {
        assert_eq!(
            Packet {
                flags: FLAG_SYN,
                stream_id: 1,
                seq_num: 0,
                ack_num: 0,
                payload: vec![]
            }
            .encode()
            .len(),
            HEADER_LEN
        );
    }

    #[test]
    fn test_has_flag() {
        let p = Packet {
            flags: FLAG_DATA | FLAG_ACK,
            stream_id: 0,
            seq_num: 0,
            ack_num: 0,
            payload: vec![],
        };
        assert!(p.has_flag(FLAG_DATA));
        assert!(p.has_flag(FLAG_ACK));
        assert!(!p.has_flag(FLAG_SYN));
    }
}
