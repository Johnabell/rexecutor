use chrono::TimeDelta;
use std::hash::Hash;

use crate::job::JobStatus;

#[allow(private_bounds)]
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct UniquenessCriteria<'a> {
    pub key: Option<i64>,
    pub executor: bool,
    pub duration: Option<TimeDelta>,
    pub statuses: &'a [JobStatus],
    // TODO: add ability to update job details on conflict
    // pub on_conflict: T,
}

trait ConflictResolution {}

pub struct DoNothing;

impl ConflictResolution for DoNothing {}
impl<'a> ConflictResolution for Replace<'a> {}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct Replace<'a> {
    pub scheduled_at: bool,
    pub data: bool,
    pub metadata: bool,
    pub priority: bool,
    pub max_attempts: bool,
    pub for_statuses: &'a [JobStatus],
}

impl<'a> UniquenessCriteria<'a> {
    pub const NONE: Self = Self::new();

    pub const fn new() -> Self {
        Self {
            key: None,
            executor: false,
            duration: None,
            statuses: &[],
            // on_conflict: DoNothing
        }
    }
}

impl<'a> UniquenessCriteria<'a> {

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

    pub fn by_key<H: Hash>(mut self, value: &H) -> Self {
        let key = fxhash::hash64(value);
        self.key = Some(key as i64);
        self
    }
}
