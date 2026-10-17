//! The supervisor's clock: a monotonic reading as a [`Duration`] since an
//! arbitrary origin. A trait so that tests move time by hand.

use std::sync::Arc;
use std::time::Duration;

pub trait Clock: Send + Sync + 'static {
    /// Time since the clock's origin. Never goes back.
    fn now(&self) -> Duration;
}

impl<T: Clock> Clock for Arc<T> {
    fn now(&self) -> Duration {
        (**self).now()
    }
}

/// The real clock. It reads tokio's time, so a test that pauses tokio's
/// clock moves this one too.
#[derive(Debug, Clone, Copy)]
pub struct MonotonicClock {
    origin: tokio::time::Instant,
}

impl MonotonicClock {
    pub fn new() -> Self {
        Self {
            origin: tokio::time::Instant::now(),
        }
    }
}

impl Default for MonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for MonotonicClock {
    fn now(&self) -> Duration {
        self.origin.elapsed()
    }
}

#[cfg(test)]
pub use fake::FakeClock;

#[cfg(test)]
mod fake {
    use std::sync::Mutex;
    use std::time::Duration;

    use super::Clock;

    /// A clock that moves only when [`advance`](Self::advance) says so.
    #[derive(Debug, Default)]
    pub struct FakeClock(Mutex<Duration>);

    impl FakeClock {
        pub fn advance(&self, by: Duration) {
            *self.0.lock().unwrap() += by;
        }
    }

    impl Clock for FakeClock {
        fn now(&self) -> Duration {
            *self.0.lock().unwrap()
        }
    }
}
