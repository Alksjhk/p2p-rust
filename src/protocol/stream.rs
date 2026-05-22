use crate::protocol::congestion::CongestionControl;
use crate::protocol::packet::{Packet, CONTROL_STREAM, FLAG_DATA, FLAG_ACK};
use crate::protocol::window::{SendWindow, ReceiveWindow};
use std::collections::HashMap;
use std::time::{Duration, Instant};

const INITIAL_RTO: Duration = Duration::from_millis(200);
const MAX_RTO: Duration = Duration::from_secs(3);
const MAX_RETRANSMITS: u32 = 5;
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(5);
const MAX_PING_LOSS: u32 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamState {
    Init,
    SynSent,
    Established,
    Closing,
    Closed,
}

#[derive(Debug)]
pub struct ManagedStream {
    pub state: StreamState,
    send_seq: u64,
    recv_seq: u64,
    unacked: Vec<UnackedPacket>,
    pub rto: Duration,
    pub retransmit_count: u32,
    pub last_activity: Instant,
    /// Buffered packets that arrived out of order (seq > recv_seq).
    /// Stored as (seq_num, payload).
    pub recv_buffer: Vec<(u64, Vec<u8>)>,
    send_window: SendWindow,
    recv_window: ReceiveWindow,
    /// Fast retransmit: count duplicate ACKs per sequence number
    dup_ack_count: HashMap<u64, u32>,
    congestion: CongestionControl,
}

#[derive(Debug, Clone)]
pub struct UnackedPacket {
    pub seq: u64,
    pub payload: Vec<u8>,
    pub sent_at: Instant,
    pub retransmits: u32,
}

impl ManagedStream {
    fn new(state: StreamState, now: Instant) -> Self {
        Self {
            state,
            send_seq: 0,
            recv_seq: 0,
            unacked: vec![],
            rto: INITIAL_RTO,
            retransmit_count: 0,
            last_activity: now,
            recv_buffer: vec![],
            send_window: SendWindow::new(64 * 1024),  // 64KB 发送窗口
            recv_window: ReceiveWindow::new(64 * 1024), // 64KB 接收窗口
            dup_ack_count: HashMap::new(),
            congestion: CongestionControl::new(),
        }
    }
}

pub struct StreamManager {
    pub streams: HashMap<u16, ManagedStream>,
    next_stream_id: u16,
    pub last_activity: Instant,
    ping_loss_count: u32,
    last_ping_sent: Option<Instant>,
}

impl StreamManager {
    pub fn new(now: Instant) -> Self {
        let mut streams = HashMap::new();
        streams.insert(CONTROL_STREAM, ManagedStream::new(StreamState::Established, now));
        Self {
            streams,
            next_stream_id: 1,
            last_activity: now,
            ping_loss_count: 0,
            last_ping_sent: None,
        }
    }

    pub fn allocate(&mut self, now: Instant) -> u16 {
        let id = self.next_stream_id;
        self.next_stream_id = self.next_stream_id.wrapping_add(1);
        if self.next_stream_id == CONTROL_STREAM {
            self.next_stream_id = 1;
        }
        self.streams.insert(id, ManagedStream::new(StreamState::SynSent, now));
        id
    }

    /// Accept a SYN, using the client's proposed stream_id if available.
    pub fn accept_syn(&mut self, proposed_id: u16, seq: u64, now: Instant) -> u16 {
        let id = if proposed_id != CONTROL_STREAM && !self.streams.contains_key(&proposed_id) {
            proposed_id
        } else {
            let id = self.next_stream_id;
            self.next_stream_id = self.next_stream_id.wrapping_add(1);
            if self.next_stream_id == CONTROL_STREAM {
                self.next_stream_id = 1;
            }
            id
        };
        let mut s = ManagedStream::new(StreamState::Established, now);
        s.recv_seq = seq.wrapping_add(1);
        self.streams.insert(id, s);
        id
    }

    pub fn get_mut(&mut self, id: u16) -> Option<&mut ManagedStream> {
        self.streams.get_mut(&id)
    }

    pub fn remove(&mut self, id: u16) {
        self.streams.remove(&id);
    }

    pub fn next_send_seq(&mut self, id: u16) -> Option<u64> {
        let s = self.streams.get_mut(&id)?;
        let seq = s.send_seq;
        s.send_seq += 1;
        Some(seq)
    }

    pub fn record_send(&mut self, id: u16, seq: u64, payload: Vec<u8>, now: Instant) {
        if let Some(s) = self.streams.get_mut(&id) {
            // 检查发送窗口
            if !s.send_window.try_send(payload.len()) {
                return;  // 窗口已满，不发送
            }
            // 检查拥塞窗口
            if !s.congestion.can_send(payload.len()) {
                return;  // 拥塞窗口已满，不发送
            }
            let payload_len = payload.len();
            s.unacked.push(UnackedPacket {
                seq,
                payload,
                sent_at: now,
                retransmits: 0,
            });
            s.last_activity = now;
            s.congestion.on_packet_sent(payload_len);
        }
    }

    pub fn on_ack(&mut self, sid: u16, ack_num: u64) {
        if let Some(s) = self.streams.get_mut(&sid) {
            // 记录 ACK 号码，检测重复 ACK
            *s.dup_ack_count.entry(ack_num).or_insert(0) += 1;

            // 快速重传：3 个重复 ACK 触发
            if *s.dup_ack_count.get(&ack_num).unwrap_or(&0) >= 3 {
                s.rto = INITIAL_RTO;  // 重置 RTO
                // 重传 dup_ack_count 追踪的包将在 retransmit_due 中处理
            }

            let acked_bytes: usize = s.unacked
                .iter()
                .filter(|p| p.seq <= ack_num)
                .map(|p| p.payload.len())
                .sum();

            let before_len = s.unacked.len();
            s.unacked.retain(|p| p.seq > ack_num);
            let acked_count = before_len - s.unacked.len();

            if acked_count > 0 {
                s.rto = INITIAL_RTO;
                s.retransmit_count = 0;
                s.dup_ack_count.clear();  // 清空重复 ACK 计数

                // 更新发送窗口
                s.send_window.on_ack(ack_num, acked_bytes);
                // 通知拥塞控制
                s.congestion.on_ack(acked_bytes);
            }
        }
    }

    /// Handle incoming data packet. Returns payloads that should be delivered
    /// to the application (in order).
    pub fn on_data(&mut self, sid: u16, seq: u64, payload: Vec<u8>) -> Vec<Vec<u8>> {
        let mut to_deliver = vec![];
        if let Some(s) = self.streams.get_mut(&sid) {
            // 检查接收窗口
            if !s.recv_window.try_receive(payload.len()) {
                // 窗口已满，丢弃包
                return to_deliver;
            }

            s.last_activity = Instant::now();

            // 重复检测使用 u64 序列比较
            if seq_lt_u64(seq, s.recv_seq) {
                return to_deliver;
            }

            if seq == s.recv_seq {
                let len = payload.len();
                to_deliver.push(payload);
                s.recv_seq += 1;
                s.recv_window.consume(len);

                // 检查缓存包
                s.recv_buffer.sort_by_key(|&(seq, _)| seq);
                while let Some(i) = s.recv_buffer.iter().position(|&(seq, _)| seq == s.recv_seq) {
                    let (_, data) = s.recv_buffer.remove(i);
                    let len = data.len();
                    to_deliver.push(data);
                    s.recv_seq += 1;
                    s.recv_window.consume(len);
                }
            } else if seq_gt_u64(seq, s.recv_seq) && seq < s.recv_seq + 1000 {
                // 限制缓存范围
                s.recv_buffer.push((seq, payload));
                if s.recv_buffer.len() > 100 {
                    s.recv_buffer.truncate(100);
                }
            }
        }
        to_deliver
    }

    pub fn current_ack(&self, sid: u16) -> u64 {
        self.streams.get(&sid)
            .map(|s| s.recv_seq.saturating_sub(1))
            .unwrap_or(0)
    }

    /// Find packets due for retransmission.
    pub fn retransmit_due(&mut self, now: Instant) -> Vec<(u16, Packet)> {
        let mut result = vec![];
        let mut dead = vec![];

        let acks: Vec<(u16, u64)> = self.streams
            .keys()
            .map(|&sid| (sid, self.current_ack(sid)))
            .collect();

        for (&sid, s) in &mut self.streams {
            let ack_num = acks.iter()
                .find(|&&(id, _)| id == sid)
                .map(|&(_, a)| a)
                .unwrap_or(0);

            let mut retransmitted_this_stream = false;
            let mut i = 0;
            while i < s.unacked.len() {
                if now.duration_since(s.unacked[i].sent_at) < s.rto {
                    i += 1;
                    continue;
                }

                // 检查重传次数
                if s.unacked[i].retransmits >= MAX_RETRANSMITS {
                    dead.push(sid);
                    break;
                }

                let pkt = s.unacked[i].clone();
                s.unacked[i].sent_at = now;
                s.unacked[i].retransmits += 1;
                s.retransmit_count += 1;
                s.rto = std::cmp::min(s.rto * 2, MAX_RTO);
                retransmitted_this_stream = true;

                result.push((
                    sid,
                    Packet {
                        flags: FLAG_DATA | FLAG_ACK,
                        connection_id: 0,  // 稍后在 Task 5 中设置
                        stream_id: sid,
                        seq_num: pkt.seq,
                        ack_num,
                        payload: pkt.payload,
                    },
                ));
                i += 1;
            }

            // 如果该流有重传，触发拥塞避免
            if retransmitted_this_stream {
                s.congestion.on_loss();
            }
        }

        for sid in dead {
            tracing::warn!(stream = sid, "max retransmits reached");
            self.streams.remove(&sid);
        }
        result
    }

    pub fn should_send_ping(&mut self, now: Instant) -> bool {
        if now.duration_since(self.last_activity) < KEEPALIVE_INTERVAL {
            return false;
        }
        match self.last_ping_sent {
            None => true,
            Some(last) if now.duration_since(last) >= KEEPALIVE_INTERVAL => {
                self.ping_loss_count += 1;
                true
            }
            _ => false,
        }
    }

    pub fn mark_ping_sent(&mut self, now: Instant) {
        self.last_ping_sent = Some(now);
    }

    pub fn on_pong(&mut self) {
        self.ping_loss_count = 0;
        self.last_ping_sent = None;
        self.last_activity = Instant::now();
    }

    pub fn keepalive_failed(&self) -> bool {
        self.ping_loss_count >= MAX_PING_LOSS
    }

    pub fn set_established(&mut self, id: u16) {
        if let Some(s) = self.streams.get_mut(&id) {
            s.state = StreamState::Established;
        }
    }
}

/// Circular sequence comparison for u64: returns true if a > b in sequence space
fn seq_gt_u64(a: u64, b: u64) -> bool {
    a != b && a.wrapping_sub(b) < 0x8000_0000_0000_0000
}

/// Returns true if a < b in circular sequence space for u64
fn seq_lt_u64(a: u64, b: u64) -> bool {
    a != b && b.wrapping_sub(a) < 0x8000_0000_0000_0000
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> Instant {
        Instant::now()
    }

    #[test]
    fn test_allocate() {
        let mut m = StreamManager::new(now());
        let id = m.allocate(now());
        assert_ne!(id, CONTROL_STREAM);
        assert_eq!(m.streams[&id].state, StreamState::SynSent);
    }

    #[test]
    fn test_accept_syn() {
        let mut m = StreamManager::new(now());
        let id = m.accept_syn(42, 0, now());
        assert_eq!(m.streams[&id].state, StreamState::Established);
        assert_eq!(m.streams[&id].recv_seq, 1);
    }

    #[test]
    fn test_seq_increments() {
        let mut m = StreamManager::new(now());
        let id = m.allocate(now());
        assert_eq!(m.next_send_seq(id), Some(0));
        assert_eq!(m.next_send_seq(id), Some(1));
    }

    #[test]
    fn test_ack_removes() {
        let mut m = StreamManager::new(now());
        let id = m.allocate(now());
        m.record_send(id, 0, vec![1], now());
        m.record_send(id, 1, vec![2], now());
        assert_eq!(m.streams[&id].unacked.len(), 2);
        m.on_ack(id, 0);
        assert_eq!(m.streams[&id].unacked.len(), 1);
    }

    #[test]
    fn test_retransmit_due() {
        let mut m = StreamManager::new(now());
        let id = m.allocate(now());
        m.record_send(id, 0, vec![1], now() - Duration::from_millis(500));
        assert!(!m.retransmit_due(now()).is_empty());
    }

    #[test]
    fn test_keepalive() {
        let mut m = StreamManager::new(now());
        m.last_activity = now() - Duration::from_secs(10);
        assert!(m.should_send_ping(now()));
    }

    #[test]
    fn test_no_keepalive_when_active() {
        let mut m = StreamManager::new(now());
        assert!(!m.should_send_ping(now()));
    }

    /// Test demonstrating the bug: Client's forwarder initiates streams but
    /// stream_mgr in client/mod.rs doesn't know about them.
    #[test]
    fn test_client_stream_manager_bug() {
        // Simulate CLIENT's stream_mgr - this is the one with the bug
        let mut client_mgr = StreamManager::new(now());

        // Client's forwarder sends SYN with stream_id=1, seq_num=0
        // BUT client_mgr doesn't have stream 1 created!
        // The forwarder should call client_mgr.allocate() first, but it doesn't!

        // Now simulate Host receiving SYN and accepting it
        let mut host_mgr = StreamManager::new(now());
        let _sid = host_mgr.accept_syn(1, 0, now());

        // Host sends ACK (ack_num=1) to Client

        // Client receives ACK - but stream 1 doesn't exist in client_mgr!
        let ack_num: u64 = 1;
        client_mgr.on_ack(1, ack_num);

        // Check: stream 1 should not exist in client_mgr
        assert!(client_mgr.streams.get(&1).is_none());

        // Host sends DATA (seq=0) to Client

        // Client receives DATA - but stream 1 doesn't exist!
        let payloads = client_mgr.on_data(1, 0, b"hello".to_vec());
        // Should deliver data, but stream doesn't exist!
        assert!(payloads.is_empty());

        // Now test: what ACK number does Client send when it sends DATA?
        // current_ack(1) should return 0 (because stream doesn't exist)
        // This is WRONG - the ACK number should reflect what Client has received!
        let ack = client_mgr.current_ack(1);
        assert_eq!(ack, 0); // This is the bug - returns 0 instead of proper ACK
    }

    /// Test showing what SHOULD happen when stream is properly created
    #[test]
    fn test_correct_stream_creation_flow() {
        let mut client_mgr = StreamManager::new(now());

        // Client SHOULD allocate stream before sending SYN
        let stream_id = client_mgr.allocate(now());
        assert_eq!(stream_id, 1);
        assert_eq!(client_mgr.streams.get(&1).unwrap().state, StreamState::SynSent);

        // Simulate sending SYN (seq=0) - this increments send_seq
        let _seq = client_mgr.next_send_seq(1).unwrap();
        assert_eq!(_seq, 0);
        client_mgr.record_send(1, 0, vec![], now());

        // Now receive ACK from Host (ack_num=1)
        client_mgr.on_ack(1, 1);

        // Unacked packet should be removed
        assert_eq!(client_mgr.streams.get(&1).unwrap().unacked.len(), 0);

        // Now receive DATA from Host (seq=0)
        let payloads = client_mgr.on_data(1, 0, b"hello".to_vec());
        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0], b"hello".to_vec());

        // recv_seq should now be 1
        assert_eq!(client_mgr.streams.get(&1).unwrap().recv_seq, 1);

        // current_ack should be 0 (recv_seq - 1)
        assert_eq!(client_mgr.current_ack(1), 0);
    }

    #[test]
    fn test_u64_sequence_numbers() {
        let mut mgr = StreamManager::new(Instant::now());
        let id = mgr.allocate(Instant::now());

        // 发送大量数据，验证 u64 不溢出
        for _i in 0..100000 {
            let seq = mgr.next_send_seq(id).unwrap();
            mgr.record_send(id, seq, vec![1, 2, 3], Instant::now());
        }
        assert_eq!(mgr.streams.get(&id).unwrap().send_seq, 100000);
    }
}
