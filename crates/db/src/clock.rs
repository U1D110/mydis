use std::time::{Duration, SystemTime};

pub(crate) trait Clock {
    fn now(&self) -> Duration;
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
    fn now(&self) -> Duration {
        self.now_unix()
    }
}

#[cfg(test)]
pub(crate) struct TestClock {
    now: SystemTime,
}

#[cfg(test)]
impl TestClock {
    pub fn new() -> Self {
        Self {
            now: SystemTime::now(),
        }
    }

    pub fn advance(&mut self, duration: Duration) {
        self.now += duration;
    }
}

#[cfg(test)]
impl Clock for TestClock {
    fn now(&self) -> Duration {
        self.now
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
    }
}
