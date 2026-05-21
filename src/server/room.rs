use rand::Rng;
use std::collections::HashMap;

pub struct Room {
    pub room_id: String,
    pub host_peer_id: String,
    pub host_addr: String,
    pub clients: HashMap<String, ClientInfo>,
}

pub struct ClientInfo {
    pub peer_id: String,
    pub addr: String,
}

pub struct RoomManager {
    rooms: HashMap<String, Room>,
    peer_to_room: HashMap<String, String>,
}

impl RoomManager {
    pub fn new() -> Self {
        Self {
            rooms: HashMap::new(),
            peer_to_room: HashMap::new(),
        }
    }

    pub fn create_room(&mut self, host_addr: &str) -> (String, String) {
        let room_id = generate_room_id();
        let host_peer_id = format!("host_{}", &room_id[..4]);
        self.rooms.insert(
            room_id.clone(),
            Room {
                room_id: room_id.clone(),
                host_peer_id: host_peer_id.clone(),
                host_addr: host_addr.to_string(),
                clients: HashMap::new(),
            },
        );
        self.peer_to_room
            .insert(host_peer_id.clone(), room_id.clone());
        (room_id, host_peer_id)
    }

    pub fn get_room(&self, id: &str) -> Option<&Room> {
        self.rooms.get(id)
    }

    pub fn get_room_mut(&mut self, id: &str) -> Option<&mut Room> {
        self.rooms.get_mut(id)
    }

    pub fn add_client(&mut self, room_id: &str, addr: &str) -> Option<(String, String)> {
        let room = self.rooms.get_mut(room_id)?;
        let peer_id = format!(
            "client_{}_{}",
            &room_id[..3],
            room.clients.len() + 1
        );
        room.clients.insert(
            peer_id.clone(),
            ClientInfo {
                peer_id: peer_id.clone(),
                addr: addr.into(),
            },
        );
        self.peer_to_room
            .insert(peer_id.clone(), room_id.into());
        Some((peer_id, room.host_addr.clone()))
    }

    pub fn remove_peer(&mut self, peer_id: &str) -> Option<String> {
        let room_id = self.peer_to_room.remove(peer_id)?;
        if let Some(room) = self.rooms.get_mut(&room_id) {
            if room.host_peer_id == peer_id {
                return Some(room_id);
            }
            room.clients.remove(peer_id);
        }
        None
    }

    pub fn remove_room(&mut self, room_id: &str) {
        if let Some(room) = self.rooms.remove(room_id) {
            self.peer_to_room.remove(&room.host_peer_id);
            for c in room.clients.values() {
                self.peer_to_room.remove(&c.peer_id);
            }
        }
    }

    /// Look up which room a peer belongs to (if any), without modifying state.
    pub fn find_room_for_peer(&self, peer_id: &str) -> Option<String> {
        self.peer_to_room.get(peer_id).cloned()
    }
}

fn generate_room_id() -> String {
    rand::thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(6)
        .map(char::from)
        .collect::<String>()
        .to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_room() {
        let mut m = RoomManager::new();
        let (rid, pid) = m.create_room("1.2.3.4:7789");
        assert_eq!(rid.len(), 6);
        assert_eq!(m.get_room(&rid).unwrap().host_addr, "1.2.3.4:7789");
        assert!(pid.starts_with("host_"));
    }

    #[test]
    fn test_add_client() {
        let mut m = RoomManager::new();
        let (rid, _) = m.create_room("1:2");
        let r = m.add_client(&rid, "3:4");
        assert!(r.is_some());
        assert_eq!(m.get_room(&rid).unwrap().clients.len(), 1);
    }

    #[test]
    fn test_add_client_bad_room() {
        assert!(RoomManager::new().add_client("nope", "1:2").is_none());
    }

    #[test]
    fn test_remove_client() {
        let mut m = RoomManager::new();
        let (rid, _) = m.create_room("1:2");
        let (pid, _) = m.add_client(&rid, "3:4").unwrap();
        assert!(m.remove_peer(&pid).is_none());
        assert!(m.get_room(&rid).unwrap().clients.is_empty());
    }

    #[test]
    fn test_remove_host_removes_room() {
        let mut m = RoomManager::new();
        let (rid, hpid) = m.create_room("1:2");
        assert_eq!(m.remove_peer(&hpid), Some(rid));
    }
}
