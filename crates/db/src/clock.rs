use std::time::{Duration, Instant, SystemTime};

pub(crate) trait Clock {
    fn now(&self) -> Instant;
    fn now_unix_secs(&self) -> u64;
    fn now_unix_millis(&self) -> u64;
}

pub(crate) struct SystemClock;

impl SystemClock {
    fn now_unix(&self) -> Duration {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
    }
}

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn now_unix_secs(&self) -> u64 {
        self.now_unix().as_secs()
    }

    fn now_unix_millis(&self) -> u64 {
        self.now_unix().as_millis() as u64
    }
}

#[cfg(test)]
pub(crate) struct TestClock {
    now: Instant,
    now_unix: SystemTime,
}

#[cfg(test)]
impl TestClock {
    pub fn new() -> Self {
        Self {
            now: Instant::now(),
            now_unix: SystemTime::now(),
        }
    }

    pub fn advance(&mut self, duration: Duration) {
        self.now += duration;
        self.now_unix += duration;
    }
}

#[cfg(test)]
impl Clock for TestClock {
    fn now(&self) -> Instant {
        self.now
    }

    fn now_unix_secs(&self) -> u64 {
        self.now_unix
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn now_unix_millis(&self) -> u64 {
        self.now_unix
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}
