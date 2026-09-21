//! How much one connection may send, and when it has sent too much.
//!
//! `PROTOCOL.md` §2.1 bounds one frame and leaves the rate to the deployment, which in
//! the reference posture means a front that supplies one. A deployment without a front
//! has nothing between a peer and back-to-back frames, and the cheapest legal frame is
//! not free: every one of them costs this server a room lock and a queue lookup per peer.
//! The budget is the in-band backstop, priced so that a flood of tiny frames is charged
//! for the work it takes and not only for the bytes it moves.

use std::time::Instant;

/// A byte count as the 64 bits the refill arithmetic works in. A platform whose `usize`
/// is narrower widens here and never narrows back.
fn wide(bytes: usize) -> u64 {
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

/// The nanosecond denominator the refill is measured in.
const NANOS_PER_SEC: u128 = 1_000_000_000;

/// What every inbound frame costs before its payload is counted: the fixed work a frame
/// costs whatever it carries. Without it, a one-byte frame would look free.
pub const FRAME_COST_BYTES: usize = 1024;

/// One connection's inbound budget: bytes it may send, refilled at a rate, with a
/// ceiling on how much of its rate may be saved up.
///
/// The bytes are a *cost*, not a promise about the wire: a frame is charged its payload
/// or [`FRAME_COST_BYTES`], whichever is larger. Time that has not yet bought a whole
/// byte is carried, so a rate of a few bytes a second still refills.
#[derive(Debug, Clone, Copy)]
pub struct InboundBudget {
    /// What is left to spend.
    tokens: u64,
    capacity: u64,
    rate_per_sec: u64,
    last: Instant,
    /// Nanoseconds since the last whole byte was earned.
    carry_nanos: u64,
}

impl InboundBudget {
    /// A budget that starts full, which is what a new connection gets: a fresh peer may
    /// spend `capacity` bytes as fast as its link allows before it is held to `rate`.
    #[must_use]
    pub fn new(rate_per_sec: usize, capacity: usize) -> Self {
        let burst = wide(capacity);
        Self {
            tokens: burst,
            capacity: burst,
            rate_per_sec: wide(rate_per_sec),
            last: Instant::now(),
            carry_nanos: 0,
        }
    }

    /// Charges one inbound frame of `payload_bytes` against the budget as it stands
    /// now, reporting whether the connection could afford it.
    pub fn try_take(&mut self, payload_bytes: usize) -> bool {
        self.try_take_at(payload_bytes, Instant::now())
    }

    /// [`InboundBudget::try_take`] at a caller-supplied instant, so the refill can be
    /// exercised without waiting for a clock to move.
    pub fn try_take_at(&mut self, payload_bytes: usize, now: Instant) -> bool {
        self.refill(now);
        let cost = wide(payload_bytes).max(wide(FRAME_COST_BYTES));
        if cost > self.tokens {
            return false;
        }
        self.tokens = self.tokens.saturating_sub(cost);
        true
    }

    /// Bytes left to spend, for a caller that wants to report the state it found.
    #[must_use]
    pub const fn remaining(&self) -> u64 {
        self.tokens
    }

    /// Earns the time since the last refill at the configured rate, up to the capacity.
    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last);
        self.last = now;
        if self.rate_per_sec == 0 {
            return;
        }
        let nanos = u64::try_from(elapsed.as_nanos())
            .unwrap_or(u64::MAX)
            .saturating_add(self.carry_nanos);
        let earned =
            u128::from(nanos).saturating_mul(u128::from(self.rate_per_sec));
        let bought = earned.checked_div(NANOS_PER_SEC).unwrap_or(0);
        let spent = bought
            .saturating_mul(NANOS_PER_SEC)
            .checked_div(u128::from(self.rate_per_sec))
            .unwrap_or(0);
        self.carry_nanos =
            u64::try_from(u128::from(nanos).saturating_sub(spent))
                .unwrap_or(u64::MAX);
        let earned_bytes = u64::try_from(bought).unwrap_or(u64::MAX);
        self.tokens =
            self.tokens.saturating_add(earned_bytes).min(self.capacity);
        if self.tokens >= self.capacity {
            // A full bucket saves no time: otherwise a peer that was quiet would spend
            // its saved credit as a burst on top of the ceiling.
            self.carry_nanos = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    /// The test clock, moved by hand: nothing here waits for a real one.
    fn at(start: Instant, after: Duration) -> Instant {
        start
            .checked_add(after)
            .expect("the test clock moves forward")
    }

    fn at_ms(start: Instant, millis: u64) -> Instant {
        at(start, Duration::from_millis(millis))
    }

    /// A burst is spent to the byte and then refused, and the refusal reports the state
    /// it found rather than taking what was left.
    #[test]
    fn a_burst_is_spent_exactly() {
        let start = Instant::now();
        let mut budget = InboundBudget::new(0, 4096);
        assert!(budget.try_take_at(4096, start));
        assert_eq!(budget.remaining(), 0);
        assert!(!budget.try_take_at(1, start));
    }

    /// A frame is charged its payload or the per-frame floor, whichever is larger: a
    /// flood of tiny frames cannot spend a burst a byte at a time.
    #[test]
    fn a_small_frame_costs_the_frame_floor() {
        let start = Instant::now();
        let mut budget = InboundBudget::new(0, 3 * FRAME_COST_BYTES);
        for _ in 0..3 {
            assert!(budget.try_take_at(1, start));
        }
        assert!(!budget.try_take_at(1, start));
        assert_eq!(budget.remaining(), 0);
    }

    /// Time at the configured rate buys bytes, and a burst larger than the rate needs
    /// the time to buy it: the budget is short until the clock has moved.
    #[test]
    fn the_rate_refills_after_the_time_it_needs() {
        let start = Instant::now();
        let mut budget = InboundBudget::new(1024, 4096);
        assert!(budget.try_take_at(4096, start));
        assert!(
            !budget.try_take_at(1024, at_ms(start, 500)),
            "half a second buys half the frame it needs"
        );
        assert!(
            budget.try_take_at(1024, at_ms(start, 1000)),
            "a second at 1024 bytes a second buys the frame back"
        );
    }

    /// A rate too slow to buy a byte between two frames still refills: the milliseconds
    /// that bought nothing are carried, so the byte the rate promises arrives on time.
    #[test]
    fn a_slow_rate_carries_the_time_that_bought_no_byte() {
        let start = Instant::now();
        let mut budget = InboundBudget::new(1, FRAME_COST_BYTES);
        assert!(budget.try_take_at(FRAME_COST_BYTES, start));
        assert_eq!(budget.remaining(), 0);
        for tick in 1..1000 {
            let _ = budget.try_take_at(FRAME_COST_BYTES, at_ms(start, tick));
            assert_eq!(budget.remaining(), 0, "no byte at tick {tick} of 1000");
        }
        let _ = budget.try_take_at(FRAME_COST_BYTES, at_ms(start, 1000));
        assert_eq!(
            budget.remaining(),
            1,
            "a second at one byte per second buys the byte"
        );
    }

    /// A quiet connection does not bank its rate as a burst on top of the ceiling: the
    /// bucket is refilled to its capacity and no further.
    #[test]
    fn a_quiet_connection_saves_nothing_past_the_ceiling() {
        let start = Instant::now();
        let mut budget = InboundBudget::new(1000, 2000);
        assert!(budget.try_take_at(2000, start));
        let later = at(start, Duration::from_secs(60));
        assert!(budget.try_take_at(2000, later));
        assert!(
            !budget.try_take_at(1, later),
            "a minute of quiet is not a minute of credit"
        );
    }

    /// A rate of zero is a budget that never refills, not a division by an unearned
    /// rate: the burst is all there is.
    #[test]
    fn a_zero_rate_still_spends_its_burst() {
        let start = Instant::now();
        let mut budget = InboundBudget::new(0, 2048);
        assert!(budget.try_take_at(2048, start));
        assert!(!budget.try_take_at(
            FRAME_COST_BYTES,
            at(start, Duration::from_secs(3600))
        ));
    }

    /// A payload past the whole budget is refused, and the charge does not wrap: the
    /// conversion saturates rather than coming back round as an affordable cost.
    #[test]
    fn a_payload_past_the_budget_is_refused() {
        let start = Instant::now();
        let mut budget = InboundBudget::new(0, 4096);
        assert!(!budget.try_take_at(usize::MAX, start));
        assert_eq!(budget.remaining(), 4096);
    }
}
