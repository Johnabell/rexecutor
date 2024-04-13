use chrono::TimeDelta;
use std::hash::Hash;

use crate::job::JobStatus;

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct UniquenessCriteria<'a> {
    pub key: Option<i64>,
    pub executor: bool,
    pub duration: Option<TimeDelta>,
    pub statuses: &'a [JobStatus],
}

impl UniquenessCriteria<'static> {
    pub const fn default_const() -> Self {
        Self {
            key: None,
            executor: false,
            duration: None,
            statuses: &[],
        }
    }
}

impl<'a> UniquenessCriteria<'a> {
    pub const NONE: Self = Self::new();

    pub const fn new() -> Self {
        Self {
            key: None,
            executor: false,
            duration: None,
            statuses: &[],
        }
    }

    pub const fn by_executor(mut self) -> Self {
        self.executor = true;
        self
    }

    pub const fn by_duration(mut self, duration: TimeDelta) -> Self {
        self.duration = Some(duration);
        self
    }

    pub const fn by_statuses(mut self, statuses: &'a [JobStatus]) -> Self {
        self.statuses = statuses;
        self
    }

    pub fn by_key<T: Hash>(mut self, value: &T) -> Self {
        let key = fxhash::hash64(value);
        self.key = Some(key as i64);
        self
    }
}
