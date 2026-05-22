use crate::protocol::packet::{Packet, FLAG_SYN, FLAG_DATA, FLAG_ACK, FLAG_FIN};
use crate::protocol::stream::StreamManager;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, Mutex};

/// Listen on local_port and forward each TCP connection as a new UDP stream.
pub async fn listen_and_forward(
    local_port: u16,
    udp_socket: Arc<UdpSocket>,
    peer_addr: SocketAddr,
    tcp_writers: Arc<Mutex<HashMap<u16, mpsc::UnboundedSender<Vec<u8>>>>>,
    _current_acks: Arc<Mutex<HashMap<u16, u64>>>,
    stream_mgr: Arc<Mutex<StreamManager>>,
    connection_id: u64,
) {
    let listener = match tokio::net::TcpListener::bind(format!("127.0.0.1:{}", local_port)).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(port = local_port, error = %e, "failed to bind local listener");
            return;
        }
    };
    tracing::info!(port = local_port, "listening for local connections");

    loop {
        let (tcp, addr) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                tracing::warn!(error = %e, "accept error");
                continue;
            }
        };

        let writers = tcp_writers.clone();
        let socket = udp_socket.clone();
        let stream_mgr_clone = stream_mgr.clone();
        let tcp_clone = tcp;

        tokio::spawn(async move {
            // Allocate stream in stream_mgr before sending SYN
            let (sid, seq) = {
                let mut mgr = stream_mgr_clone.lock().await;
                let sid = mgr.allocate(Instant::now());
                let seq = mgr.next_send_seq(sid).unwrap_or(0);
                mgr.record_send(sid, seq, vec![], Instant::now());
                (sid, seq)
            };

            tracing::info!(peer = %addr, stream = sid, "new local connection");

            // Send SYN directly over UDP
            let syn = Packet { flags: FLAG_SYN, connection_id, stream_id: sid, seq_num: seq, ack_num: 0, payload: vec![] };
            if socket.send_to(&syn.encode(), peer_addr).await.is_err() {
                return;
            }

            // Channel for receiving data to write to this TCP connection
            let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
            writers.lock().await.insert(sid, tx);

            let (mut tcp_rx, mut tcp_tx) = tcp_clone.into_split();

            // TCP -> UDP: reader task.  Send data with ACK numbers.
            let udp_socket2 = socket.clone();
            let tcp_reader = tokio::spawn(async move {
                loop {
                    let mut buf = vec![0u8; 1400];
                    match tokio::time::timeout(
                        std::time::Duration::from_millis(50),
                        tcp_rx.read(&mut buf),
                    )
                    .await
                    {
                        Ok(Ok(0)) | Ok(Err(_)) => break,
                        Ok(Ok(n)) => {
                            buf.truncate(n);
                            // Get current ACK number for this stream
                            let (seq, ack_num) = {
                                let mut mgr = stream_mgr_clone.lock().await;
                                let seq = mgr.next_send_seq(sid).unwrap_or(0);
                                let ack_num = mgr.current_ack(sid);
                                mgr.record_send(sid, seq, buf.clone(), Instant::now());
                                (seq, ack_num)
                            };
                            let pkt = Packet {
                                flags: FLAG_DATA | FLAG_ACK,
                                connection_id,
                                stream_id: sid,
                                seq_num: seq,
                                ack_num,
                                payload: buf,
                            };
                            if udp_socket2.send_to(&pkt.encode(), peer_addr).await.is_err() {
                                break;
                            }
                        }
                        _ => {} // timeout, loop again
                    }
                }
            });

            // UDP -> TCP: writer task
            let writers_clone = writers.clone();
            let tcp_writer = tokio::spawn(async move {
                while let Some(data) = rx.recv().await {
                    if tcp_tx.write_all(&data).await.is_err() {
                        break;
                    }
                }
                writers_clone.lock().await.remove(&sid);
            });

            let _ = tokio::join!(tcp_reader, tcp_writer);

            // Send FIN directly over UDP
            let fin = Packet { flags: FLAG_FIN, connection_id, stream_id: sid, seq_num: 0, ack_num: 0, payload: vec![] };
            let _ = socket.send_to(&fin.encode(), peer_addr).await;
        });
    }
}
