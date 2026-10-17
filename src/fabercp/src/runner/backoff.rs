//! Restart backoff of one container.

use std::time::Duration;

/// The first delay.
pub const BACKOFF_MIN: Duration = Duration::from_millis(100);
/// The longest delay.
pub const BACKOFF_MAX: Duration = Duration::from_secs(60);
/// A container that stayed up this long starts again from [`BACKOFF_MIN`].
pub const BACKOFF_RESET_AFTER: Duration = Duration::from_secs(10);

/// Delays that start at [`BACKOFF_MIN`], double on each restart up to
/// [`BACKOFF_MAX`], and start over after the container was up for
/// [`BACKOFF_RESET_AFTER`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Backoff {
    next: Duration,
}

impl Default for Backoff {
    fn default() -> Self {
        Self { next: BACKOFF_MIN }
    }
}

impl Backoff {
    /// The delay before the restart that follows a run of `up`.
    pub fn on_exit(&mut self, up: Duration) -> Duration {
        if up >= BACKOFF_RESET_AFTER {
            self.next = BACKOFF_MIN;
        }
        let delay = self.next;
        self.next = delay.saturating_mul(2).min(BACKOFF_MAX);
        delay
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doubles_from_100ms_up_to_a_minute() {
        let mut b = Backoff::default();
        let delays: Vec<_> = (0..12).map(|_| b.on_exit(Duration::ZERO)).collect();
        let ms = Duration::from_millis;
        let want = [100, 200, 400, 800, 1600, 3200, 6400, 12800, 25600, 51200].map(ms);
        assert_eq!(delays[..10], want);
        assert_eq!(delays[10..], [BACKOFF_MAX, BACKOFF_MAX]);
    }

    #[test]
    fn resets_after_ten_seconds_up_and_not_before() {
        let mut b = Backoff::default();
        for _ in 0..5 {
            b.on_exit(Duration::ZERO);
        }
        assert_eq!(
            b.on_exit(BACKOFF_RESET_AFTER - Duration::from_millis(1)),
            Duration::from_millis(3200)
        );
        assert_eq!(b.on_exit(BACKOFF_RESET_AFTER), BACKOFF_MIN);
        assert_eq!(b.on_exit(Duration::ZERO), Duration::from_millis(200));
    }
}
