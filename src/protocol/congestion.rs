// Simplified TCP-style congestion control

const MSS: usize = 1460;
const INITIAL_CWND: usize = 10 * MSS;
const MIN_CWND: usize = 2 * MSS;

#[derive(Debug)]
pub enum CongestionState {
    SlowStart,
    CongestionAvoidance,
}

#[derive(Debug)]
pub struct CongestionControl {
    cwnd: usize,                    // Congestion window
    ssthresh: usize,                // Slow start threshold
    state: CongestionState,
    bytes_in_flight: usize,
    bytes_acked_this_round: usize,
}

impl CongestionControl {
    pub fn new() -> Self {
        Self {
            cwnd: INITIAL_CWND,
            ssthresh: usize::MAX,
            state: CongestionState::SlowStart,
            bytes_in_flight: 0,
            bytes_acked_this_round: 0,
        }
    }

    pub fn cwnd(&self) -> usize {
        self.cwnd
    }

    pub fn can_send(&self, bytes: usize) -> bool {
        self.bytes_in_flight + bytes <= self.cwnd
    }

    pub fn on_packet_sent(&mut self, bytes: usize) {
        self.bytes_in_flight += bytes;
    }

    pub fn on_ack(&mut self, bytes_acknowledged: usize) {
        self.bytes_in_flight = self.bytes_in_flight.saturating_sub(bytes_acknowledged);

        match self.state {
            CongestionState::SlowStart => {
                // Slow start: increase by bytes acknowledged
                self.cwnd += bytes_acknowledged;
                if self.cwnd >= self.ssthresh {
                    self.state = CongestionState::CongestionAvoidance;
                }
            }
            CongestionState::CongestionAvoidance => {
                // Congestion avoidance: increase by MSS per RTT
                self.bytes_acked_this_round += bytes_acknowledged;
                if self.bytes_acked_this_round >= self.cwnd {
                    self.cwnd += MSS;
                    self.bytes_acked_this_round = 0;
                }
            }
        }
    }

    pub fn on_loss(&mut self) {
        // Congestion occurred: set ssthresh to cwnd/2, reset cwnd
        self.ssthresh = self.cwnd / 2;
        self.cwnd = MIN_CWND;
        self.state = CongestionState::SlowStart;
        self.bytes_acked_this_round = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_congestion_slow_start() {
        let mut cc = CongestionControl::new();
        assert_eq!(cc.cwnd(), 10 * 1460); // Initial 10 MSS
        cc.on_ack(1460);
        assert!(cc.cwnd() > 1460); // Window grows during slow start
    }

    #[test]
    fn test_congestion_avoidance() {
        let mut cc = CongestionControl::new();
        // Directly enter congestion avoidance state
        cc.ssthresh = INITIAL_CWND;
        cc.cwnd = INITIAL_CWND;
        cc.state = CongestionState::CongestionAvoidance;

        let cwnd_before = cc.cwnd();
        // Send and acknowledge one RTT of data
        for _ in 0..10 {
            cc.on_packet_sent(MSS);
            cc.on_ack(MSS);
        }
        // Congestion avoidance increases by only one MSS per RTT
        assert_eq!(cc.cwnd(), cwnd_before + MSS);
    }

    #[test]
    fn test_can_send() {
        let mut cc = CongestionControl::new();

        // Initially can send up to cwnd
        assert!(cc.can_send(INITIAL_CWND));
        assert!(!cc.can_send(INITIAL_CWND + 1));

        // After sending some data
        cc.on_packet_sent(5000);
        assert!(cc.can_send(INITIAL_CWND - 5000));
        assert!(!cc.can_send(INITIAL_CWND - 5000 + 1));
    }

    #[test]
    fn test_on_loss() {
        let mut cc = CongestionControl::new();

        // Build up cwnd
        for _ in 0..20 {
            cc.on_packet_sent(MSS);
            cc.on_ack(MSS);
        }

        let cwnd_before_loss = cc.cwnd();
        cc.on_loss();

        // ssthresh should be half of previous cwnd
        assert_eq!(cc.ssthresh, cwnd_before_loss / 2);
        // cwnd should reset to MIN_CWND
        assert_eq!(cc.cwnd(), MIN_CWND);
        // state should be SlowStart
        assert!(matches!(cc.state, CongestionState::SlowStart));
    }

    #[test]
    fn test_transition_to_congestion_avoidance() {
        let mut cc = CongestionControl::new();

        // Reset cwnd to be below ssthresh
        cc.cwnd = 2 * MSS;
        cc.ssthresh = 3 * MSS;

        // Initially in slow start
        assert!(matches!(cc.state, CongestionState::SlowStart));

        // A single large ACK that pushes cwnd above ssthresh
        cc.on_ack(2 * MSS);
        // Should transition to congestion avoidance since cwnd (now 4*MSS) >= ssthresh (3*MSS)
        assert!(matches!(cc.state, CongestionState::CongestionAvoidance));
    }
}