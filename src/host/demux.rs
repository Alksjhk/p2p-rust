use crate::protocol::packet::Packet;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, Mutex};

/// Dispatches incoming UDP packets to registered handlers by source address.
///
/// A single UDP socket is shared across all peer handlers. Each handler
/// registers its peer's UDP address and receives packets via an mpsc channel.
/// Unregistered addresses are silently dropped.
#[derive(Clone)]
pub struct PacketDemux {
    socket: Arc<UdpSocket>,
    peers: Arc<Mutex<HashMap<SocketAddr, mpsc::UnboundedSender<Packet>>>>,
}

impl PacketDemux {
    pub fn new(socket: Arc<UdpSocket>) -> Self {
        Self {
            socket,
            peers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Register a handler for packets from `addr`.
    /// Returns a receiver that will receive decoded packets from that address.
    pub async fn register(&self, addr: SocketAddr) -> mpsc::UnboundedReceiver<Packet> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.peers.lock().await.insert(addr, tx);
        rx
    }

    /// Unregister a handler for `addr`. Further packets from this address
    /// will be silently dropped.
    pub async fn unregister(&self, addr: SocketAddr) {
        self.peers.lock().await.remove(&addr);
    }

    /// Run the receive loop. Reads packets from the shared UDP socket
    /// and dispatches them by source address.
    ///
    /// Runs until the socket produces an error (typically dropped/closing).
    pub async fn run(&self) {
        let mut buf = [0u8; 2000];
        loop {
            match self.socket.recv_from(&mut buf).await {
                Ok((n, from)) => {
                    if let Some(pkt) = Packet::decode(&buf[..n]) {
                        let peers = self.peers.lock().await;
                        if let Some(tx) = peers.get(&from) {
                            let _ = tx.send(pkt);
                        }
                    }
                }
                Err(_) => break,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::packet::{Packet, FLAG_DATA};

    #[tokio::test]
    async fn test_demux_register_unregister() {
        let demux_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let client_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        let demux_addr = demux_sock.local_addr().unwrap();
        let client_addr = client_sock.local_addr().unwrap();

        let demux = PacketDemux::new(demux_sock.clone());

        // Register a handler
        let mut rx = demux.register(client_addr).await;

        // Start demux in background
        let d = demux.clone();
        tokio::spawn(async move { d.run().await; });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Send a packet from client address
        let pkt = Packet {
            flags: FLAG_DATA,
            connection_id: 0,
            stream_id: 1,
            seq_num: 0,
            ack_num: 0,
            payload: b"hello".to_vec(),
        };
        client_sock.send_to(&pkt.encode(), demux_addr).await.unwrap();

        // Handler receives it
        let received = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            rx.recv(),
        )
        .await
        .expect("timeout")
        .expect("channel closed");
        assert_eq!(received, pkt);

        // Unregister — further packets dropped silently
        demux.unregister(client_addr).await;
        client_sock.send_to(&pkt.encode(), demux_addr).await.unwrap();
        // No assert; just verifying no panic/crash
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}
