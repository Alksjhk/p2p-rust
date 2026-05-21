use crate::protocol::packet::{Packet, FLAG_DATA, FLAG_ACK, FLAG_RST};
use std::net::SocketAddr;
use std::sync::Arc;
use std::collections::HashMap;
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::{mpsc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Manages per-stream TCP connections on the host side.
pub struct TcpConnectionManager {
    /// Map from stream_id to TCP writer channel.
    tcp_writers: Arc<Mutex<HashMap<u16, mpsc::UnboundedSender<Vec<u8>>>>>,
    /// Buffered data that arrived before the TCP connection was ready.
    pending_data: Arc<Mutex<HashMap<u16, Vec<Vec<u8>>>>>,
}

impl TcpConnectionManager {
    pub fn new() -> Self {
        Self {
            tcp_writers: Arc::new(Mutex::new(HashMap::new())),
            pending_data: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn writers(&self) -> Arc<Mutex<HashMap<u16, mpsc::UnboundedSender<Vec<u8>>>>> {
        self.tcp_writers.clone()
    }

    /// Buffer data for a stream whose TCP connection isn't ready yet.
    pub async fn buffer_data(&self, stream_id: u16, data: Vec<u8>) {
        let mut pending = self.pending_data.lock().await;
        pending.entry(stream_id).or_default().push(data);
    }

    /// Take all pending data for a stream, removing it from the buffer.
    pub async fn take_pending(&self, stream_id: u16) -> Vec<Vec<u8>> {
        let mut pending = self.pending_data.lock().await;
        pending.remove(&stream_id).unwrap_or_default()
    }

    /// Handle an incoming SYN on a new stream.
    /// Connects to the target TCP port and spawns bidirectional forwarding.
    pub async fn handle_syn(
        &self,
        stream_id: u16,
        target_addr: String,
        udp_socket: Arc<UdpSocket>,
        peer_addr: SocketAddr,
    ) {
        let writers = self.tcp_writers.clone();
        let udp_socket2 = udp_socket.clone();

        match TcpStream::connect(&target_addr).await {
            Ok(tcp) => {
                tracing::info!(stream = stream_id, target = %target_addr, "TCP connection established");
                let (mut tcp_rx, mut tcp_tx) = tcp.into_split();

                // Channel for receiving data to write to TCP
                let (data_tx, mut data_rx) = mpsc::unbounded_channel::<Vec<u8>>();

                // Drain any data that arrived before the TCP connection was ready
                let pending = self.take_pending(stream_id).await;
                for data in pending {
                    let _ = data_tx.send(data);
                }

                {
                    let mut w = writers.lock().await;
                    w.insert(stream_id, data_tx);
                }

                // UDP → TCP: receive data from channel, write to TCP socket
                let writers_clone = writers.clone();
                let tcp_writer = tokio::spawn(async move {
                    while let Some(data) = data_rx.recv().await {
                        if tcp_tx.write_all(&data).await.is_err() { break; }
                    }
                    // Cleanup on disconnect
                    writers_clone.lock().await.remove(&stream_id);
                });

                // TCP → UDP: read from TCP socket, send as DATA packets over UDP
                let tcp_reader = tokio::spawn(async move {
                    let mut seq: u16 = 0;
                    loop {
                        let mut buf = vec![0u8; 1400];
                        match tokio::time::timeout(
                            std::time::Duration::from_millis(50),
                            tcp_rx.read(&mut buf),
                        ).await {
                            Ok(Ok(0)) | Ok(Err(_)) => break,
                            Ok(Ok(n)) => {
                                buf.truncate(n);
                                let pkt = Packet {
                                    flags: FLAG_DATA | FLAG_ACK,
                                    stream_id,
                                    seq_num: seq,
                                    ack_num: 0,
                                    payload: buf,
                                };
                                seq = seq.wrapping_add(1);
                                if udp_socket2.send_to(&pkt.encode(), peer_addr).await.is_err() { break; }
                            }
                            _ => {} // timeout, loop again
                        }
                    }
                });

                let _ = tokio::join!(tcp_reader, tcp_writer);
            }
            Err(e) => {
                tracing::warn!(stream = stream_id, target = %target_addr, error = %e, "TCP connect failed, sending RST");
                // Clean up any buffered data for this stream
                self.take_pending(stream_id).await;
                let rst = Packet {
                    flags: FLAG_RST,
                    stream_id,
                    seq_num: 0,
                    ack_num: 0,
                    payload: vec![],
                };
                let _ = udp_socket.send_to(&rst.encode(), peer_addr).await;
            }
        }
    }
}
