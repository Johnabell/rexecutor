use std::fmt::Display;

use chrono::{DateTime, Utc};

pub mod builder;
pub(crate) mod runner;

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub struct JobId(i32);

impl From<i32> for JobId {
    fn from(value: i32) -> Self {
        Self(value)
    }
}

impl From<JobId> for i32 {
    fn from(value: JobId) -> Self {
        value.0
    }
}

impl Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JobId({})", self.0)
    }
}

// TODO: add support for queues and concurrent execution limits
// #[non_exhaustive]
pub struct Job<E> {
    pub id: JobId,
    pub status: JobStatus,
    pub executor: String,
    pub data: E,
    pub attempt: u16,
    pub max_attempts: u16,
    pub errors: Vec<JobError>,
    pub inserted_at: DateTime<Utc>,
    pub scheduled_at: DateTime<Utc>,
    pub attempted_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub cancelled_at: Option<DateTime<Utc>>,
    pub discarded_at: Option<DateTime<Utc>>,
}

pub enum JobStatus {
    Complete,
    Executing,
    Scheduled,
    Retryable,
    Cancelled,
    Discarded,
}

pub struct JobError {
    pub attempt: u16,
    pub error_type: String,
    pub details: String,
    pub recorded_at: DateTime<Utc>,
}
