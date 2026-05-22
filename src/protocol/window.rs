use std::num::Wrapping;

/// Circular sequence comparison: returns true if a > b in sequence space
fn seq_gt(a: u64, b: u64) -> bool {
    a != b && a.wrapping_sub(b) < 0x8000_0000_0000_0000
}

#[derive(Debug)]
pub struct SendWindow {
    window_size: usize,
    unacked_bytes: usize,
    last_ack: Option<Wrapping<u64>>,
}

impl SendWindow {
    pub fn new(window_size: usize) -> Self {
        Self {
            window_size,
            unacked_bytes: 0,
            last_ack: None, // None means no ack received yet
        }
    }

    pub fn try_send(&mut self, len: usize) -> bool {
        if self.unacked_bytes + len > self.window_size {
            return false;
        }
        self.unacked_bytes += len;
        true
    }

    pub fn on_ack(&mut self, ack_num: u64, bytes_acknowledged: usize) {
        // Only update if ack_num is newer than last_ack (circular comparison)
        // Defensive: don't subtract more bytes than we have tracked
        if self.last_ack.is_none() || seq_gt(ack_num, self.last_ack.unwrap().0) {
            self.last_ack = Some(Wrapping(ack_num));
            let acked = bytes_acknowledged.min(self.unacked_bytes);
            self.unacked_bytes -= acked;
        }
    }

    pub fn unacked_bytes(&self) -> usize {
        self.unacked_bytes
    }

    pub fn available(&self) -> usize {
        self.window_size.saturating_sub(self.unacked_bytes)
    }
}

#[derive(Debug)]
pub struct ReceiveWindow {
    window_size: usize,
    buffered_bytes: usize,
    read_offset: u64,
}

impl ReceiveWindow {
    pub fn new(window_size: usize) -> Self {
        Self {
            window_size,
            buffered_bytes: 0,
            read_offset: 0,
        }
    }

    pub fn try_receive(&mut self, len: usize) -> bool {
        if self.buffered_bytes + len > self.window_size {
            return false;
        }
        self.buffered_bytes += len;
        true
    }

    pub fn consume(&mut self, bytes: usize) {
        self.buffered_bytes = self.buffered_bytes.saturating_sub(bytes);
        self.read_offset += bytes as u64;
    }

    pub fn buffered_bytes(&self) -> usize {
        self.buffered_bytes
    }

    pub fn window_available(&self) -> usize {
        self.window_size.saturating_sub(self.buffered_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_send_window_allow_send() {
        let mut window = SendWindow::new(1024);
        assert!(window.try_send(500));
        assert_eq!(window.unacked_bytes(), 500);
        assert!(window.try_send(524));
        assert!(!window.try_send(1)); // Window full
    }

    #[test]
    fn test_send_window_on_ack() {
        let mut window = SendWindow::new(1024);
        window.try_send(500);
        window.on_ack(0, 500);
        assert_eq!(window.unacked_bytes(), 0);
        assert!(window.try_send(1024));
    }

    #[test]
    fn test_send_window_circular_wraparound() {
        let mut window = SendWindow::new(1024);
        // Send data at sequence near u64::MAX
        window.try_send(500);
        window.on_ack(u64::MAX, 500);

        // Ack with wrapped value (0) should be considered newer
        window.try_send(600);
        window.on_ack(0, 600);

        assert_eq!(window.unacked_bytes(), 0);
        assert!(window.try_send(1024));
    }

    #[test]
    fn test_send_window_old_ack_ignored() {
        let mut window = SendWindow::new(1024);
        window.try_send(500);
        window.on_ack(10, 500);
        assert_eq!(window.unacked_bytes(), 0);

        // Send more data, ack at sequence 20
        window.try_send(300);
        window.on_ack(20, 300);
        assert_eq!(window.unacked_bytes(), 0);

        // Old ack (sequence 5) should be ignored
        window.on_ack(5, 100);
        assert_eq!(window.unacked_bytes(), 0);
    }

    #[test]
    fn test_send_window_defensive_ack_bytes() {
        let mut window = SendWindow::new(1024);
        window.try_send(100);
        assert_eq!(window.unacked_bytes(), 100);

        // Try to acknowledge more bytes than we sent (defensive check)
        window.on_ack(1, 500);
        assert_eq!(window.unacked_bytes(), 0); // Should only subtract 100
    }

    #[test]
    fn test_receive_window_accept_data() {
        let mut window = ReceiveWindow::new(2048);
        assert!(window.try_receive(1000));
        assert_eq!(window.buffered_bytes(), 1000);
        assert!(window.try_receive(1048));
        assert!(!window.try_receive(1));
    }
}
