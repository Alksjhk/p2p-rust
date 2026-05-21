pub mod room;

use crate::signal::{self, SignalMsg, SignalWriter};
use anyhow::Result;
use room::RoomManager;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex};

struct PeerHandle {
    tx: mpsc::UnboundedSender<SignalMsg>,
    addr: String,
}

struct ServerState {
    rooms: RoomManager,
    peers: HashMap<String, PeerHandle>,
}

pub async fn run(listen: String) -> Result<()> {
    let listener = TcpListener::bind(&listen).await?;
    tracing::info!("Signaling server listening on {}", listen);

    let state = Arc::new(Mutex::new(ServerState {
        rooms: RoomManager::new(),
        peers: HashMap::new(),
    }));

    loop {
        let (reader, writer, addr) = signal::accept(&listener).await?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_peer(reader, writer, state, addr).await {
                tracing::warn!(peer = %addr, error = %e, "disconnected");
            }
        });
    }
}

async fn handle_peer(
    mut reader: crate::signal::SignalReader,
    mut writer: SignalWriter,
    state: Arc<Mutex<ServerState>>,
    addr: std::net::SocketAddr,
) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel::<SignalMsg>();
    let my_peer_id = format!("peer_{:04x}", rand::random::<u16>());
    let my_addr = addr.to_string();

    // Register this peer's writer channel
    {
        let mut s = state.lock().await;
        s.peers.insert(my_peer_id.clone(), PeerHandle { tx: tx.clone(), addr: my_addr.clone() });
    }

    // Writer task: forward channel messages to TCP
    let writer_handle = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if writer.send(&msg).await.is_err() { break; }
        }
    });

    // Reader loop
    let mut peer_id_in_room: Option<String> = None;

    loop {
        let msg = match reader.recv().await {
            Ok(m) => m,
            Err(_) => break,
        };

        match &msg {
            SignalMsg::CreateRoom { .. } => {
                let mut s = state.lock().await;
                let (room_id, pid) = s.rooms.create_room(&my_addr);
                peer_id_in_room = Some(pid.clone());
                // Map this peer's writer to the host peer_id
                s.peers.insert(pid.clone(), PeerHandle { tx: tx.clone(), addr: my_addr.clone() });
                let _ = tx.send(SignalMsg::RoomCreated {
                    room_id,
                    my_addr: my_addr.clone(),
                });
                tracing::info!(peer = %addr, "room created");
            }
            SignalMsg::JoinRoom { room_id, .. } => {
                let mut s = state.lock().await;
                if s.rooms.get_room(room_id).is_none() {
                    let _ = tx.send(SignalMsg::Error { reason: "room_not_found".into() });
                    continue;
                }
                if let Some((pid, host_addr)) = s.rooms.add_client(room_id, &my_addr) {
                    peer_id_in_room = Some(pid.clone());
                    s.peers.insert(pid.clone(), PeerHandle { tx: tx.clone(), addr: my_addr.clone() });
                    let _ = tx.send(SignalMsg::RoomInfo {
                        host_addr,
                        my_addr: my_addr.clone(),
                        room_id: room_id.clone(),
                    });
                    // Notify host
                    if let Some(host_pid) = s.rooms.get_room(room_id).map(|r| r.host_peer_id.clone()) {
                        if let Some(handle) = s.peers.get(&host_pid) {
                            let _ = handle.tx.send(SignalMsg::PeerJoined {
                                peer_addr: my_addr.clone(),
                                peer_id: pid.clone(),
                                room_id: room_id.clone(),
                            });
                        }
                    }
                }
            }
            SignalMsg::P2PReady { room_id, peer_id } => {
                let s = state.lock().await;
                if let Some(room) = s.rooms.get_room(room_id) {
                    // Determine sender: the connection's tracked peer_id
                    let sender_is_host = peer_id_in_room
                        .as_ref()
                        .map(|p| p == &room.host_peer_id)
                        .unwrap_or(false);

                    let targets: Vec<String> = if sender_is_host {
                        // Host is signalling — forward to the specified peer
                        if room.clients.contains_key(peer_id) {
                            vec![peer_id.clone()]
                        } else {
                            // Fallback: host said its own peer_id (single-client compat)
                            room.clients.keys().cloned().collect()
                        }
                    } else {
                        // Client is signalling — forward to host
                        vec![room.host_peer_id.clone()]
                    };
                    for target in targets {
                        if let Some(handle) = s.peers.get(&target) {
                            let _ = handle.tx.send(SignalMsg::P2PReady {
                                room_id: room_id.clone(),
                                peer_id: peer_id.clone(),
                            });
                        }
                    }
                }
            }
            _ => {}
        }
    }

    // Cleanup on disconnect
    {
        let mut s = state.lock().await;
        s.peers.remove(&my_peer_id);
        if let Some(pid) = peer_id_in_room.take() {
            s.peers.remove(&pid);

            // Capture room_id BEFORE remove_peer (which removes peer_to_room entry)
            let peer_room_id = s.rooms.find_room_for_peer(&pid);

            if let Some(rid) = s.rooms.remove_peer(&pid) {
                // Host disconnected -- existing logic: notify clients, remove room
                if let Some(room) = s.rooms.get_room(&rid) {
                    for client in room.clients.keys() {
                        if let Some(handle) = s.peers.get(client) {
                            let _ = handle.tx.send(SignalMsg::RoomClosed {
                                reason: "host_disconnected".into(),
                            });
                        }
                    }
                }
                s.rooms.remove_room(&rid);
                tracing::info!(room_id = %rid, "room closed");
            } else if let Some(rid) = peer_room_id {
                // Client disconnected -- notify host
                if let Some(room) = s.rooms.get_room(&rid) {
                    if let Some(handle) = s.peers.get(&room.host_peer_id) {
                        let _ = handle.tx.send(SignalMsg::PeerLeft {
                            peer_id: pid,
                            room_id: rid,
                        });
                    }
                }
            }
        }
    }

    let _ = writer_handle.await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn test_server_notifies_peer_left() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_port = listener.local_addr().unwrap().port();
        drop(listener);
        let server_addr = format!("127.0.0.1:{}", server_port);

        let srv = run(server_addr.clone());
        tokio::spawn(async move { let _ = srv.await; });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Connect as host -> create room
        let (mut host_r, mut host_w) = signal::connect(&server_addr).await.unwrap();
        host_w.send(&SignalMsg::CreateRoom { secret: None }).await.unwrap();
        let room_id = match host_r.recv().await.unwrap() {
            SignalMsg::RoomCreated { room_id, .. } => room_id,
            other => panic!("expected RoomCreated, got {:?}", other),
        };

        // Connect as client -> join room
        let (mut client_r, mut client_w) = signal::connect(&server_addr).await.unwrap();
        client_w.send(&SignalMsg::JoinRoom { room_id: room_id.clone(), secret: None }).await.unwrap();
        let _room_info = client_r.recv().await.unwrap();

        // Host receives PeerJoined
        let peer_joined = host_r.recv().await.unwrap();
        let client_pid = match &peer_joined {
            SignalMsg::PeerJoined { peer_id, .. } => peer_id.clone(),
            _ => panic!("expected PeerJoined, got {:?}", peer_joined),
        };

        // Drop client -- host should get PeerLeft
        drop(client_r);
        drop(client_w);
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let peer_left = host_r.recv().await.unwrap();
        match &peer_left {
            SignalMsg::PeerLeft { peer_id, room_id: rid } => {
                assert_eq!(peer_id, &client_pid);
                assert_eq!(rid, &room_id);
            }
            _ => panic!("expected PeerLeft, got {:?}", peer_left),
        }
    }
}
