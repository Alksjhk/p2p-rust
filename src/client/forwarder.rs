use crate::protocol::packet::{Packet, FLAG_SYN, FLAG_DATA, FLAG_ACK, FLAG_FIN};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, Mutex};

/// Listen on local_port and forward each TCP connection as a new UDP stream.
pub async fn listen_and_forward(
    local_port: u16,
    udp_socket: Arc<UdpSocket>,
    peer_addr: SocketAddr,
    tcp_writers: Arc<Mutex<HashMap<u16, mpsc::UnboundedSender<Vec<u8>>>>>,
) {
    let listener = match tokio::net::TcpListener::bind(format!("127.0.0.1:{}", local_port)).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(port = local_port, error = %e, "failed to bind local listener");
            return;
        }
    };
    tracing::info!(port = local_port, "listening for local connections");

    let mut next_stream_id: u16 = 1;

    loop {
        let (tcp, addr) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                tracing::warn!(error = %e, "accept error");
                continue;
            }
        };

        tracing::info!(peer = %addr, stream = next_stream_id, "new local connection");
        let sid = next_stream_id;
        next_stream_id = next_stream_id.wrapping_add(1);
        let writers = tcp_writers.clone();
        let socket = udp_socket.clone();

        tokio::spawn(async move {
            // Send SYN directly over UDP
            let syn = Packet { flags: FLAG_SYN, stream_id: sid, seq_num: 0, ack_num: 0, payload: vec![] };
            if socket.send_to(&syn.encode(), peer_addr).await.is_err() {
                return;
            }

            // Channel for receiving data to write to this TCP connection
            let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
            writers.lock().await.insert(sid, tx);

            let (mut tcp_rx, mut tcp_tx) = tcp.into_split();

            // TCP -> UDP: reader task.  Send data directly over UDP.
            let udp_socket2 = socket.clone();
            let tcp_reader = tokio::spawn(async move {
                let mut seq: u16 = 1;
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
                            let pkt = Packet {
                                flags: FLAG_DATA | FLAG_ACK,
                                stream_id: sid,
                                seq_num: seq,
                                ack_num: 0,
                                payload: buf,
                            };
                            seq = seq.wrapping_add(1);
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
            let fin = Packet { flags: FLAG_FIN, stream_id: sid, seq_num: 0, ack_num: 0, payload: vec![] };
            let _ = socket.send_to(&fin.encode(), peer_addr).await;
        });
    }
}
