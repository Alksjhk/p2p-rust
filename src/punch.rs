// UDP hole punching — full implementation in Task 7

use crate::protocol::packet::{self, Packet, FLAG_PING};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::Notify;
use tokio::time::timeout;

const PROBE_INTERVAL: Duration = Duration::from_millis(500);
const PUNCH_TIMEOUT: Duration = Duration::from_secs(10);

/// Result of hole punching.
#[derive(Debug)]
pub enum PunchResult {
    Success,
    Timeout,
}

/// Attempt to punch a UDP hole to the peer.
/// Both sides call this simultaneously, sending probes to each other.
/// Returns when a packet is received from the peer or timeout.
pub async fn punch_hole(
    socket: Arc<UdpSocket>,
    peer_addr: SocketAddr,
    ready: Arc<Notify>,
) -> PunchResult {
    let start = Instant::now();

    // Build probe packet
    let probe = Packet {
        flags: FLAG_PING,
        connection_id: 0,
        stream_id: packet::CONTROL_STREAM,
        seq_num: 0,
        ack_num: 0,
        payload: b"PTP_PUNCH".to_vec(),
    };
    let probe_bytes = probe.encode();

    // Send initial probe immediately
    let _ = socket.send_to(&probe_bytes, peer_addr).await;

    loop {
        if start.elapsed() >= PUNCH_TIMEOUT {
            return PunchResult::Timeout;
        }

        let mut buf = [0u8; 1500];
        match timeout(PROBE_INTERVAL, socket.recv_from(&mut buf)).await {
            Ok(Ok((_n, from))) if from == peer_addr => {
                tracing::info!(peer = %from, "received UDP packet during punch");
                ready.notify_one();
                return PunchResult::Success;
            }
            Ok(Ok((_n, from))) => {
                // Packet from unexpected address — ignore it
                tracing::debug!(peer = %from, expected = %peer_addr, "ignoring packet from unexpected address during punch");
            }
            _ => {
                // Send another probe
                let _ = socket.send_to(&probe_bytes, peer_addr).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::packet::Packet;

    #[test]
    fn test_probe_packet_format() {
        let p = Packet {
            flags: FLAG_PING,
            connection_id: 0,
            stream_id: 0,
            seq_num: 0,
            ack_num: 0,
            payload: b"PTP_PUNCH".to_vec(),
        };
        let encoded = p.encode();
        let decoded = Packet::decode(&encoded).unwrap();
        assert_eq!(p, decoded);
    }
}
