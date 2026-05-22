mod forwarder;

use crate::protocol::packet::{self, Packet, FLAG_SYN, FLAG_ACK, FLAG_DATA, FLAG_PING, FLAG_PONG, FLAG_FIN, FLAG_RST};
use crate::protocol::stream::StreamManager;
use crate::punch::{self, PunchResult};
use crate::signal::{self, SignalMsg};
use anyhow::{bail, Result};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, Mutex, Notify};

pub async fn run(server_addr: String, room_id: String, local_port: u16, _secret: Option<String>) -> Result<()> {
    // 1. Connect to signaling server
    let (mut reader, mut writer) = signal::connect(&server_addr).await?;
    tracing::info!(server = %server_addr, "connected to signaling server");

    // 2. Join room
    writer
        .send(&SignalMsg::JoinRoom {
            room_id: room_id.clone(),
            secret: None,
        })
        .await?;
    let (host_addr, my_addr) = match reader.recv().await? {
        SignalMsg::RoomInfo {
            host_addr,
            my_addr,
            room_id: rid,
        } => {
            tracing::info!(host = %host_addr, room = %rid, "received room info");
            (host_addr, my_addr)
        }
        SignalMsg::Error { reason } => bail!("server error: {}", reason),
        other => bail!("unexpected: {:?}", other),
    };

    let my_socket_addr: SocketAddr = my_addr.parse()?;
    let udp_bind_addr = format!("0.0.0.0:{}", my_socket_addr.port());
    let host_udp: SocketAddr = host_addr.parse()?;

    // 3. Bind UDP socket
    let socket = Arc::new(UdpSocket::bind(&udp_bind_addr).await?);
    tracing::info!(udp = %socket.local_addr().unwrap(), "UDP socket bound");

    // 4. Punch UDP hole
    tracing::info!(peer = %host_udp, "starting UDP hole punch");
    let punch_ready = Arc::new(Notify::new());
    let puncher = punch_ready.clone();
    let punch_socket = socket.clone();

    let punch_handle = tokio::spawn(async move {
        match punch::punch_hole(punch_socket, host_udp, puncher).await {
            PunchResult::Success => tracing::info!("hole punch succeeded"),
            PunchResult::Timeout => tracing::error!("hole punch timed out"),
        }
    });

    tokio::time::timeout(Duration::from_secs(12), punch_ready.notified())
        .await
        .ok();
    let _ = punch_handle.await;

    // Generate connection_id
    let connection_id = {
        use std::time::{SystemTime, UNIX_EPOCH};
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap();
        duration.as_secs() * 1_000_000_000 + duration.subsec_nanos() as u64
    };

    // Notify server we're ready
    writer
        .send(&SignalMsg::P2PReady {
            room_id: room_id.clone(),
            peer_id: format!("client_{}", &room_id[..3]),
        })
        .await?;

    // 5. Set up stream manager and packet channels
    let stream_mgr = StreamManager::new(Instant::now());
    let stream_mgr_shared = Arc::new(Mutex::new(stream_mgr));
    let (packet_tx, mut packet_rx) = mpsc::unbounded_channel::<Packet>();
    let tcp_writers: Arc<Mutex<HashMap<u16, mpsc::UnboundedSender<Vec<u8>>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    // Shared state for current ACK numbers (stream_id -> ack_num)
    let current_acks: Arc<Mutex<HashMap<u16, u64>>> = Arc::new(Mutex::new(HashMap::new()));

    // Start local port forwarder
    let fwd_socket = socket.clone();
    let fwd_peer = host_udp;
    let fwd_writers = tcp_writers.clone();
    let fwd_acks = current_acks.clone();
    let fwd_stream_mgr = stream_mgr_shared.clone();
    tokio::spawn(async move {
        forwarder::listen_and_forward(local_port, fwd_socket, fwd_peer, fwd_writers, fwd_acks, fwd_stream_mgr, connection_id).await;
    });

    // UDP receive task
    let udp_socket = socket.clone();
    let rx_tx = packet_tx.clone();
    tokio::spawn(async move {
        let mut buf = [0u8; 2000];
        loop {
            match udp_socket.recv_from(&mut buf).await {
                Ok((n, _)) => {
                    if let Some(pkt) = Packet::decode(&buf[..n]) {
                        if rx_tx.send(pkt).is_err() {
                            break;
                        }
                    }
                }
                Err(_) => break,
            }
        }
    });

    // 6. Main event loop
    let mut next_ping = Instant::now();
    let mut next_rtx = Instant::now();

    loop {
        let now = Instant::now();

        // Keepalive (only check at intervals)
        if now >= next_ping {
            let should_ping = {
                let mut mgr = stream_mgr_shared.lock().await;
                if mgr.should_send_ping(now) {
                    let pkt = Packet {
                        flags: FLAG_PING,
                        connection_id,
                        stream_id: packet::CONTROL_STREAM,
                        seq_num: 0,
                        ack_num: 0,
                        payload: vec![],
                    };
                    let _ = socket.send_to(&pkt.encode(), host_udp).await;
                    mgr.mark_ping_sent(now);
                    true
                } else {
                    false
                }
            };
            if should_ping && stream_mgr_shared.lock().await.keepalive_failed() {
                tracing::error!("keepalive failed");
                break;
            }
            next_ping = now + Duration::from_secs(1);
        }

        // Retransmit (only check at intervals)
        if now >= next_rtx {
            let retransmits = {
                let mut mgr = stream_mgr_shared.lock().await;
                mgr.retransmit_due(now)
            };
            for (_, pkt) in retransmits {
                let _ = socket.send_to(&pkt.encode(), host_udp).await;
            }
            next_rtx = now + Duration::from_millis(100);
        }

        // Process incoming packets
        if let Ok(Some(pkt)) =
            tokio::time::timeout(Duration::from_millis(50), packet_rx.recv()).await
        {
            let now = Instant::now();
            let payloads = {
                let mut mgr = stream_mgr_shared.lock().await;
                mgr.last_activity = now;
                if pkt.has_flag(FLAG_DATA) {
                    let payloads = mgr.on_data(pkt.stream_id, pkt.seq_num, pkt.payload);
                    mgr.on_ack(pkt.stream_id, pkt.ack_num);

                    // Update shared ACK state for the forwarder
                    let ack_num = mgr.current_ack(pkt.stream_id);
                    current_acks.lock().await.insert(pkt.stream_id, ack_num);
                    payloads
                } else if pkt.has_flag(FLAG_SYN) {
                    // Host-initiated SYN (future use): accept and send ACK
                    let sid = mgr.accept_syn(pkt.stream_id, pkt.seq_num, now);
                    let ack = Packet {
                        flags: FLAG_ACK,
                        connection_id,
                        stream_id: sid,
                        seq_num: 0,
                        ack_num: pkt.seq_num.wrapping_add(1),
                        payload: vec![],
                    };
                    let _ = socket.send_to(&ack.encode(), host_udp).await;
                    vec![]
                } else if pkt.has_flag(FLAG_ACK) {
                    mgr.on_ack(pkt.stream_id, pkt.ack_num);
                    vec![]
                } else if pkt.has_flag(FLAG_PING) {
                    let pong = Packet {
                        flags: FLAG_PONG,
                        connection_id,
                        stream_id: packet::CONTROL_STREAM,
                        seq_num: 0,
                        ack_num: 0,
                        payload: vec![],
                    };
                    let _ = socket.send_to(&pong.encode(), host_udp).await;
                    mgr.on_pong();
                    vec![]
                } else if pkt.has_flag(FLAG_PONG) {
                    mgr.on_pong();
                    vec![]
                } else if pkt.has_flag(FLAG_FIN) || pkt.has_flag(FLAG_RST) {
                    mgr.remove(pkt.stream_id);
                    vec![]
                } else {
                    vec![]
                }
            };

            // Deliver payloads to TCP writers
            if !payloads.is_empty() {
                let writers = tcp_writers.lock().await;
                if let Some(tx) = writers.get(&pkt.stream_id) {
                    for payload in payloads {
                        let _ = tx.send(payload);
                    }
                }
            }
        }

        // Check for ctrl-c
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("shutting down");
                break;
            }
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }

    Ok(())
}
