use chrono::{DateTime, Utc};

pub mod builder;

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub struct JobId(i64);

// TODO: add support for queues and concurrent execution limits
#[non_exhaustive]
pub struct Job<E> {
    pub id: JobId,
    pub status: JobStatus,
    pub executor: String,
    pub data: E,
    pub attempt: u32,
    pub max_attempts: u32,
    pub errors: Vec<JobError>,
    pub inserted_at: DateTime<Utc>,
    pub schedule_at: DateTime<Utc>,
    pub attempted_at: Option<DateTime<Utc>>,
    pub competed_at: Option<DateTime<Utc>>,
    pub cancelled_at: Option<DateTime<Utc>>,
    pub discarded_at: Option<DateTime<Utc>>,
}

pub enum JobStatus {
    Complete,
    Scheduled,
    Cancelled,
    Discarded,
}

pub struct JobError {
    pub attempt: i32,
    pub error_type: String,
    pub details: String,
}
