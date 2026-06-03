use std::time::Instant;

#[cfg(test)]
use std::time::Duration;

pub(crate) trait Clock {
    fn now(&self) -> Instant;
}

pub(crate) struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

#[cfg(test)]
pub(crate) struct TestClock {
    now: Instant,
}

#[cfg(test)]
impl TestClock {
    pub fn new() -> Self {
        Self { now: Instant::now() }
    }

    pub fn advance(&mut self, duration: Duration) {
        self.now += duration
    }
}

#[cfg(test)]
impl Clock for TestClock {
    fn now(&self) -> Instant {
        self.now
    }
}