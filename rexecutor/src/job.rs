use std::fmt::Display;

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;

use crate::backend;

pub mod builder;
pub(crate) mod runner;
pub mod uniqueness_criteria;

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

// TODO: add support for priority
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

impl<E> TryFrom<backend::Job> for Job<E>
where
    E: DeserializeOwned,
{
    type Error = serde_json::Error;

    fn try_from(value: backend::Job) -> Result<Self, Self::Error> {
        let data = serde_json::from_value(value.data)?;
        Ok(Self {
            id: value.id.into(),
            status: value.status,
            executor: value.executor,
            data,
            attempt: value.attempt.try_into().unwrap(),
            attempted_at: value.attempted_at,
            max_attempts: value.max_attempts.try_into().unwrap(),
            errors: value.errors,
            inserted_at: value.inserted_at,
            scheduled_at: value.scheduled_at,
            completed_at: value.completed_at,
            cancelled_at: value.cancelled_at,
            discarded_at: value.discarded_at,
        })
    }
}

impl<E> Job<E> {
    pub(crate) fn is_final_attempt(&self) -> bool {
        self.attempt == self.max_attempts
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum JobStatus {
    Complete,
    Executing,
    Scheduled,
    Retryable,
    Cancelled,
    Discarded,
}

#[derive(Debug)]
pub struct JobError {
    pub attempt: u16,
    pub error_type: ErrorType,
    pub details: String,
    pub recorded_at: DateTime<Utc>,
}

#[derive(Debug)]
pub enum ErrorType {
    Panic,
    Timeout,
    Cancelled,
    Other(String),
}
