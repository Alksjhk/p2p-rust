pub mod demux;
pub mod mapper;

use crate::protocol::packet::{self, Packet, FLAG_SYN, FLAG_DATA, FLAG_ACK, FLAG_FIN, FLAG_RST, FLAG_PING, FLAG_PONG};
use crate::protocol::stream::StreamManager;
use crate::punch::{self, PunchResult};
use crate::signal::{self, SignalMsg};
use crate::host::demux::PacketDemux;
use anyhow::{bail, Result};
use mapper::TcpConnectionManager;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, Notify};
use tokio_util::sync::CancellationToken;

/// Run the host mode: connect to signaling server, create a room,
/// and serve multiple clients through independent UDP tunnels.
pub async fn run(server_addr: String, target_addr: String, _secret: Option<String>) -> Result<()> {
    // Normalize target address: treat bare port numbers as localhost
    let target_addr = if target_addr.contains(':') {
        target_addr
    } else {
        format!("127.0.0.1:{}", target_addr)
    };

    // 1. Connect to signaling server
    let (mut reader, mut writer) = signal::connect(&server_addr).await?;
    tracing::info!(server = %server_addr, "connected to signaling server");

    // 2. Create room
    writer.send(&SignalMsg::CreateRoom { secret: None }).await?;
    let (room_id, my_addr) = match reader.recv().await? {
        SignalMsg::RoomCreated { room_id, my_addr } => {
            tracing::info!(room_id = %room_id, my_addr = %my_addr, "room created");
            (room_id, my_addr)
        }
        other => bail!("unexpected response: {:?}", other),
    };

    // Extract UDP port from the TCP addr reported by server
    let my_socket_addr: SocketAddr = my_addr.parse()?;
    let udp_bind_addr = format!("0.0.0.0:{}", my_socket_addr.port());

    // 3. Bind UDP socket
    let socket = Arc::new(UdpSocket::bind(&udp_bind_addr).await?);
    tracing::info!(udp = %socket.local_addr().unwrap(), "UDP socket bound");

    // 4. Start PacketDemux for dispatching inbound UDP by source address
    let demux = Arc::new(PacketDemux::new(socket.clone()));
    let demux_handle = {
        let d = demux.clone();
        tokio::spawn(async move { d.run().await; })
    };

    // 5. Signal channel — peer handlers send messages here,
    //    the main loop forwards them to the signaling writer
    let (signal_tx, mut signal_rx) = mpsc::unbounded_channel::<SignalMsg>();

    // 6. Cancellation: root cancels all handlers on shutdown,
    //    child tokens cancel individual peers on PeerLeft
    let cancel_root = CancellationToken::new();
    let mut peer_tokens: HashMap<String, CancellationToken> = HashMap::new();

    // 7. Main signaling loop
    'main: loop {
        tokio::select! {
            msg = reader.recv() => {
                match msg {
                    Ok(SignalMsg::PeerJoined { peer_addr, peer_id, room_id: _rid }) => {
                        let peer_udp: SocketAddr = match peer_addr.parse() {
                            Ok(a) => a,
                            Err(e) => {
                                tracing::warn!(peer_addr = %peer_addr, error = %e, "invalid peer address");
                                continue;
                            }
                        };
                        tracing::info!(peer = %peer_udp, peer_id = %peer_id, "client joined, spawning handler");

                        let token = cancel_root.child_token();
                        peer_tokens.insert(peer_id.clone(), token.clone());

                        tokio::spawn(peer_handler(
                            demux.clone(),
                            socket.clone(),
                            peer_id,
                            peer_udp,
                            target_addr.clone(),
                            signal_tx.clone(),
                            token,
                            room_id.clone(),
                        ));
                    }
                    Ok(SignalMsg::PeerLeft { peer_id, .. }) => {
                        tracing::info!(peer_id = %peer_id, "client left, cancelling handler");
                        if let Some(token) = peer_tokens.remove(&peer_id) {
                            token.cancel();
                        }
                    }
                    Ok(SignalMsg::RoomClosed { reason }) => {
                        tracing::info!(reason = %reason, "room closed");
                        break 'main;
                    }
                    Ok(_) => {} // ignore other messages
                    Err(e) => {
                        tracing::warn!(error = %e, "signaling connection lost");
                        break 'main;
                    }
                }
            }
            msg = signal_rx.recv() => {
                match msg {
                    Some(msg) => {
                        if let Err(e) = writer.send(&msg).await {
                            tracing::warn!(error = %e, "failed to send signal message");
                            break 'main;
                        }
                    }
                    None => {
                        // All signal_tx senders dropped — no handlers active
                        break 'main;
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("shutting down");
                break 'main;
            }
        }
    }

    // 8. Shutdown: cancel all handlers
    tracing::info!("cancelling all peer handlers");
    cancel_root.cancel();
    peer_tokens.clear();

    // Stop the demux loop by aborting its task
    demux_handle.abort();

    tracing::info!("host shut down");
    Ok(())
}

/// Per-client handler: hole punch, register with demux, run stream processing.
async fn peer_handler(
    demux: Arc<PacketDemux>,
    socket: Arc<UdpSocket>,
    peer_id: String,
    peer_udp: SocketAddr,
    target_addr: String,
    signal_tx: mpsc::UnboundedSender<SignalMsg>,
    cancel: CancellationToken,
    room_id: String,
) {
    tracing::info!(peer = %peer_udp, peer_id = %peer_id, "starting peer handler");

    // 1. Hole punch with source address filtering
    let punch_ready = Arc::new(Notify::new());
    let puncher = punch_ready.clone();
    let punch_socket = socket.clone();
    let p_addr = peer_udp;

    let punch_handle = tokio::spawn(async move {
        match punch::punch_hole(punch_socket, p_addr, puncher).await {
            PunchResult::Success => tracing::info!(peer = %p_addr, "hole punch succeeded"),
            PunchResult::Timeout => tracing::error!(peer = %p_addr, "hole punch timed out"),
        }
    });

    let punch_ok = tokio::time::timeout(Duration::from_secs(12), punch_ready.notified()).await.is_ok();
    let _ = punch_handle.await;
    if !punch_ok {
        tracing::warn!(peer = %peer_udp, "hole punch timed out, stopping handler");
        return;
    }

    // 2. Register with demux to receive packets from this peer
    let mut packet_rx = demux.register(peer_udp).await;

    // 3. Signal P2PReady via the signaling channel (main loop forwards to server)
    if signal_tx.send(SignalMsg::P2PReady {
        room_id: room_id.clone(),
        peer_id: peer_id.clone(),
    }).is_err() {
        tracing::warn!(peer = %peer_udp, "failed to send P2PReady, main loop may have exited");
        return;
    }

    // 4. Create per-peer stream manager and TCP connection manager
    let mut stream_mgr = StreamManager::new(Instant::now());
    let tcp_mgr = Arc::new(TcpConnectionManager::new());

    // 5. Main packet processing loop
    let mut next_ping = Instant::now();
    let mut next_rtx = Instant::now();

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::info!(peer = %peer_udp, "peer handler cancelled");
                break;
            }
            maybe_pkt = tokio::time::timeout(
                Duration::from_millis(50),
                packet_rx.recv(),
            ) => {
                match maybe_pkt {
                    Ok(Some(pkt)) => {
                        let now = Instant::now();
                        stream_mgr.last_activity = now;

                        if pkt.has_flag(FLAG_SYN) {
                            let sid = stream_mgr.accept_syn(pkt.stream_id, pkt.seq_num, now);
                            tracing::info!(stream = sid, "incoming SYN, connecting to target");

                            let ack = Packet {
                                flags: FLAG_ACK,
                                stream_id: sid,
                                seq_num: 0,
                                ack_num: pkt.seq_num.wrapping_add(1),
                                payload: vec![],
                            };
                            let _ = socket.send_to(&ack.encode(), peer_udp).await;

                            let target = target_addr.clone();
                            let udp_sock = socket.clone();
                            let tcp_mgr_clone = tcp_mgr.clone();
                            tokio::spawn(async move {
                                tcp_mgr_clone.handle_syn(sid, target, udp_sock, peer_udp).await;
                            });
                        } else if pkt.has_flag(FLAG_DATA) {
                            stream_mgr.on_data(pkt.stream_id, pkt.seq_num);
                            stream_mgr.on_ack(pkt.stream_id, pkt.ack_num);

                            let writers = tcp_mgr.writers();
                            let w = writers.lock().await;
                            if let Some(tx) = w.get(&pkt.stream_id) {
                                let _ = tx.send(pkt.payload.clone());
                            } else {
                                tcp_mgr.buffer_data(pkt.stream_id, pkt.payload.clone()).await;
                            }
                        } else if pkt.has_flag(FLAG_PING) {
                            let pong = Packet {
                                flags: FLAG_PONG,
                                stream_id: packet::CONTROL_STREAM,
                                seq_num: 0,
                                ack_num: 0,
                                payload: vec![],
                            };
                            let _ = socket.send_to(&pong.encode(), peer_udp).await;
                        } else if pkt.has_flag(FLAG_PONG) {
                            stream_mgr.on_pong();
                        } else if pkt.has_flag(FLAG_FIN) {
                            tracing::info!(stream = pkt.stream_id, "FIN received");
                            stream_mgr.remove(pkt.stream_id);
                        } else if pkt.has_flag(FLAG_RST) {
                            tracing::info!(stream = pkt.stream_id, "RST received");
                            stream_mgr.remove(pkt.stream_id);
                        }
                    }
                    Ok(None) => {
                        // Channel closed — demux unregistered us
                        break;
                    }
                    Err(_) => {
                        // Timeout — no packet this cycle, continue to keepalive/retransmit
                    }
                }
            }
        }

        // Keepalive (non-blocking, checked every ~50ms)
        let now = Instant::now();
        if now >= next_ping {
            if stream_mgr.should_send_ping(now) {
                let pkt = Packet {
                    flags: FLAG_PING,
                    stream_id: packet::CONTROL_STREAM,
                    seq_num: 0,
                    ack_num: 0,
                    payload: vec![],
                };
                let _ = socket.send_to(&pkt.encode(), peer_udp).await;
                stream_mgr.mark_ping_sent(now);
            }
            if stream_mgr.keepalive_failed() {
                tracing::error!(peer = %peer_udp, "keepalive failed, disconnecting");
                break;
            }
            next_ping = now + Duration::from_secs(1);
        }

        // Retransmit
        if now >= next_rtx {
            for (_, pkt) in stream_mgr.retransmit_due(now) {
                let _ = socket.send_to(&pkt.encode(), peer_udp).await;
            }
            next_rtx = now + Duration::from_millis(100);
        }
    }

    // Cleanup
    demux.unregister(peer_udp).await;
    tracing::info!(peer = %peer_udp, peer_id = %peer_id, "peer handler stopped");
}

#[cfg(test)]
mod multi_client_tests {
    use super::*;
    use crate::server;
    use crate::protocol::packet::FLAG_PING;
    use tokio::net::TcpListener;

    /// Test that PacketDemux correctly dispatches to independent handlers.
    #[tokio::test]
    async fn test_demux_multi_peer_dispatch() {
        let demux_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let client1 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let client2 = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        let demux_addr = demux_sock.local_addr().unwrap();

        let demux = demux::PacketDemux::new(demux_sock.clone());

        // Register both clients
        let mut rx1 = demux.register(client1.local_addr().unwrap()).await;
        let mut rx2 = demux.register(client2.local_addr().unwrap()).await;

        let d = demux.clone();
        tokio::spawn(async move { d.run().await; });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let ping = Packet {
            flags: FLAG_PING,
            stream_id: 0,
            seq_num: 0,
            ack_num: 0,
            payload: vec![],
        };

        // Send from both clients
        client1.send_to(&ping.encode(), demux_addr).await.unwrap();
        client2.send_to(&ping.encode(), demux_addr).await.unwrap();

        // Each handler receives its own packet
        let got1 = tokio::time::timeout(std::time::Duration::from_secs(1), rx1.recv())
            .await.expect("timeout client1").expect("channel closed");
        let got2 = tokio::time::timeout(std::time::Duration::from_secs(1), rx2.recv())
            .await.expect("timeout client2").expect("channel closed");

        assert_eq!(got1, ping);
        assert_eq!(got2, ping);
    }

    /// Test signaling flow: server, host, and two clients through
    /// the join/leave lifecycle, verifying PeerJoined, PeerLeft, and P2PReady.
    #[tokio::test]
    async fn test_host_accepts_two_clients_signaling() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let sig_addr = format!("127.0.0.1:{}", port);

        // Start signaling server
        let srv = server::run(sig_addr.clone());
        tokio::spawn(async move { let _ = srv.await; });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Connect as host → create room
        let (mut host_r, mut host_w) = signal::connect(&sig_addr).await.unwrap();
        host_w.send(&SignalMsg::CreateRoom { secret: None }).await.unwrap();
        let room_id = match host_r.recv().await.unwrap() {
            SignalMsg::RoomCreated { room_id, .. } => room_id,
            other => panic!("expected RoomCreated, got {:?}", other),
        };

        // Connect client 1
        let (mut c1_r, mut c1_w) = signal::connect(&sig_addr).await.unwrap();
        c1_w.send(&SignalMsg::JoinRoom { room_id: room_id.clone(), secret: None }).await.unwrap();
        match c1_r.recv().await.unwrap() {
            SignalMsg::RoomInfo { .. } => (),
            other => panic!("expected RoomInfo, got {:?}", other),
        };

        // Connect client 2
        let (mut c2_r, mut c2_w) = signal::connect(&sig_addr).await.unwrap();
        c2_w.send(&SignalMsg::JoinRoom { room_id: room_id.clone(), secret: None }).await.unwrap();
        match c2_r.recv().await.unwrap() {
            SignalMsg::RoomInfo { .. } => (),
            other => panic!("expected RoomInfo, got {:?}", other),
        };

        // Host receives two PeerJoined messages
        let joined1 = host_r.recv().await.unwrap();
        let pid1 = match &joined1 {
            SignalMsg::PeerJoined { peer_id, .. } => peer_id.clone(),
            other => panic!("expected PeerJoined, got {:?}", other),
        };

        let joined2 = host_r.recv().await.unwrap();
        let pid2 = match &joined2 {
            SignalMsg::PeerJoined { peer_id, .. } => peer_id.clone(),
            other => panic!("expected PeerJoined, got {:?}", other),
        };
        assert_ne!(pid1, pid2, "two clients should get unique peer_ids");

        // Send P2PReady for client 1 → server should forward to client 1 only
        host_w.send(&SignalMsg::P2PReady {
            room_id: room_id.clone(),
            peer_id: pid1.clone(),
        }).await.unwrap();

        let ready = tokio::time::timeout(std::time::Duration::from_secs(1), c1_r.recv())
            .await.expect("timeout").expect("channel closed");
        match ready {
            SignalMsg::P2PReady { peer_id, .. } => assert_eq!(peer_id, pid1),
            other => panic!("expected P2PReady, got {:?}", other),
        }

        // Disconnect client 1 → host should receive PeerLeft
        drop(c1_r);
        drop(c1_w);
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let left = host_r.recv().await.unwrap();
        match &left {
            SignalMsg::PeerLeft { peer_id, room_id: rid } => {
                assert_eq!(peer_id, &pid1);
                assert_eq!(rid, &room_id);
            }
            other => panic!("expected PeerLeft, got {:?}", other),
        }

        // Client 2 still works — P2PReady reaches it
        host_w.send(&SignalMsg::P2PReady {
            room_id: room_id.clone(),
            peer_id: pid2.clone(),
        }).await.unwrap();

        let ready2 = tokio::time::timeout(std::time::Duration::from_secs(1), c2_r.recv())
            .await.expect("timeout").expect("channel closed");
        match ready2 {
            SignalMsg::P2PReady { peer_id, .. } => assert_eq!(peer_id, pid2),
            other => panic!("expected P2PReady, got {:?}", other),
        }
    }
}
