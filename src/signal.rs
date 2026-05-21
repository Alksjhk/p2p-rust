use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SignalMsg {
    #[serde(rename = "create_room")]
    CreateRoom {
        #[serde(skip_serializing_if = "Option::is_none")]
        secret: Option<String>,
    },
    #[serde(rename = "room_created")]
    RoomCreated {
        room_id: String,
        my_addr: String,
    },
    #[serde(rename = "join_room")]
    JoinRoom {
        room_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        secret: Option<String>,
    },
    #[serde(rename = "room_info")]
    RoomInfo {
        host_addr: String,
        my_addr: String,
        room_id: String,
    },
    #[serde(rename = "peer_joined")]
    PeerJoined {
        peer_addr: String,
        peer_id: String,
        room_id: String,
    },
    #[serde(rename = "peer_left")]
    PeerLeft {
        peer_id: String,
        room_id: String,
    },
    #[serde(rename = "p2p_ready")]
    P2PReady {
        room_id: String,
        peer_id: String,
    },
    #[serde(rename = "error")]
    Error {
        reason: String,
    },
    #[serde(rename = "room_closed")]
    RoomClosed {
        reason: String,
    },
}

pub struct SignalReader {
    reader: BufReader<OwnedReadHalf>,
}

pub struct SignalWriter {
    writer: OwnedWriteHalf,
}

impl SignalReader {
    pub async fn recv(&mut self) -> Result<SignalMsg> {
        let mut line = String::new();
        let n = self.reader.read_line(&mut line).await?;
        if n == 0 {
            bail!("connection closed");
        }
        Ok(serde_json::from_str(line.trim())?)
    }
}

impl SignalWriter {
    pub async fn send(&mut self, msg: &SignalMsg) -> Result<()> {
        let json = serde_json::to_string(msg)?;
        self.writer.write_all(json.as_bytes()).await?;
        self.writer.write_all(b"\n").await?;
        self.writer.flush().await?;
        Ok(())
    }
}

pub async fn connect(addr: &str) -> Result<(SignalReader, SignalWriter)> {
    let stream = TcpStream::connect(addr).await?;
    let (r, w) = stream.into_split();
    Ok((SignalReader { reader: BufReader::new(r) }, SignalWriter { writer: w }))
}

pub async fn accept(
    listener: &tokio::net::TcpListener,
) -> Result<(SignalReader, SignalWriter, std::net::SocketAddr)> {
    let (stream, addr) = listener.accept().await?;
    let (r, w) = stream.into_split();
    Ok((SignalReader { reader: BufReader::new(r) }, SignalWriter { writer: w }, addr))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_room_serialize() {
        let m = SignalMsg::CreateRoom { secret: None };
        let j = serde_json::to_string(&m).unwrap();
        assert!(j.contains("\"create_room\"") && !j.contains("secret"));
    }

    #[test]
    fn test_create_room_with_secret() {
        let m = SignalMsg::CreateRoom { secret: Some("x".into()) };
        let j = serde_json::to_string(&m).unwrap();
        assert!(j.contains("x"));
    }

    #[test]
    fn test_deserialize_room_created() {
        let m: SignalMsg =
            serde_json::from_str(r#"{"type":"room_created","room_id":"abc","my_addr":"1:2"}"#)
                .unwrap();
        assert!(matches!(m, SignalMsg::RoomCreated { .. }));
    }

    #[test]
    fn test_deserialize_error() {
        let m: SignalMsg =
            serde_json::from_str(r#"{"type":"error","reason":"not_found"}"#).unwrap();
        assert!(matches!(m, SignalMsg::Error { .. }));
    }

    #[test]
    fn test_all_variants_roundtrip() {
        let cases = vec![
            SignalMsg::CreateRoom { secret: None },
            SignalMsg::CreateRoom { secret: Some("s".into()) },
            SignalMsg::RoomCreated { room_id: "r".into(), my_addr: "a".into() },
            SignalMsg::JoinRoom { room_id: "r".into(), secret: None },
            SignalMsg::JoinRoom { room_id: "r".into(), secret: Some("s".into()) },
            SignalMsg::RoomInfo {
                host_addr: "h".into(),
                my_addr: "m".into(),
                room_id: "r".into(),
            },
            SignalMsg::PeerJoined {
                peer_addr: "p".into(),
                peer_id: "i".into(),
                room_id: "r".into(),
            },
            SignalMsg::PeerLeft {
                peer_id: "i".into(),
                room_id: "r".into(),
            },
            SignalMsg::P2PReady { room_id: "r".into(), peer_id: "i".into() },
            SignalMsg::Error { reason: "e".into() },
            SignalMsg::RoomClosed { reason: "e".into() },
        ];
        for v in cases {
            let j = serde_json::to_string(&v).unwrap();
            let _: SignalMsg = serde_json::from_str(&j).unwrap();
        }
    }
}
