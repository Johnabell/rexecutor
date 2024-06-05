use std::fmt::Display;

use chrono::{DateTime, Utc};
use rexecutor::backend::BackendError;
use serde::{Deserialize, Serialize};
use sqlx::{
    postgres::{PgHasArrayType, PgTypeInfo},
    prelude::FromRow,
};

#[derive(sqlx::Type, Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[sqlx(type_name = "rexecutor_job_state", rename_all = "lowercase")]
pub(crate) enum JobStatus {
    Complete,
    Executing,
    Scheduled,
    Retryable,
    Cancelled,
    Discarded,
}

impl PgHasArrayType for JobStatus {
    fn array_type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("rexecutor_job_state[]")
    }
}

impl From<JobStatus> for rexecutor::job::JobStatus {
    fn from(value: JobStatus) -> Self {
        match value {
            JobStatus::Complete => Self::Complete,
            JobStatus::Executing => Self::Executing,
            JobStatus::Scheduled => Self::Scheduled,
            JobStatus::Retryable => Self::Retryable,
            JobStatus::Cancelled => Self::Cancelled,
            JobStatus::Discarded => Self::Discarded,
        }
    }
}

impl From<rexecutor::job::JobStatus> for JobStatus {
    fn from(value: rexecutor::job::JobStatus) -> Self {
        match value {
            rexecutor::job::JobStatus::Complete => Self::Complete,
            rexecutor::job::JobStatus::Executing => Self::Executing,
            rexecutor::job::JobStatus::Scheduled => Self::Scheduled,
            rexecutor::job::JobStatus::Retryable => Self::Retryable,
            rexecutor::job::JobStatus::Cancelled => Self::Cancelled,
            rexecutor::job::JobStatus::Discarded => Self::Discarded,
        }
    }
}

#[derive(Debug, FromRow)]
pub(crate) struct Job {
    pub id: i32,
    pub status: JobStatus,
    pub executor: String,
    pub data: serde_json::Value,
    pub metadata: serde_json::Value,
    pub attempt: i32,
    pub max_attempts: i32,
    pub priority: i32,
    pub tags: Vec<String>,
    pub errors: Vec<serde_json::Value>,
    pub inserted_at: DateTime<Utc>,
    pub scheduled_at: DateTime<Utc>,
    pub attempted_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub cancelled_at: Option<DateTime<Utc>>,
    pub discarded_at: Option<DateTime<Utc>>,
}

impl TryFrom<Job> for rexecutor::backend::Job {
    type Error = BackendError;

    fn try_from(value: Job) -> Result<Self, Self::Error> {
        let data = serde_json::from_value(value.data)?;
        let metadata = serde_json::from_value(value.metadata)?;
        Ok(Self {
            id: value.id,
            status: value.status.into(),
            executor: value.executor,
            data,
            metadata,
            attempt: value.attempt,
            attempted_at: value.attempted_at,
            max_attempts: value.max_attempts,
            priority: value.priority,
            tags: value.tags,
            errors: value
                .errors
                .into_iter()
                .map(serde_json::from_value::<JobError>)
                .map(|res| res.map(From::from))
                .collect::<Result<_, _>>()?,
            inserted_at: value.inserted_at,
            scheduled_at: value.scheduled_at,
            completed_at: value.completed_at,
            cancelled_at: value.cancelled_at,
            discarded_at: value.discarded_at,
        })
    }
}

#[derive(Deserialize, Debug)]
pub struct JobError {
    pub attempt: u16,
    pub error_type: ErrorType,
    pub details: String,
    pub recorded_at: DateTime<Utc>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum ErrorType {
    Panic,
    Timeout,
    Cancelled,
    #[serde(untagged)]
    Other(String),
}

impl Display for ErrorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let val = match self {
            ErrorType::Panic => "panic",
            ErrorType::Timeout => "timeout",
            ErrorType::Cancelled => "cancelled",
            ErrorType::Other(val) => val,
        };
        write!(f, "{val}")
    }
}

impl From<JobError> for rexecutor::job::JobError {
    fn from(value: JobError) -> Self {
        Self {
            attempt: value.attempt,
            error_type: value.error_type.into(),
            details: value.details,
            recorded_at: value.recorded_at,
        }
    }
}

impl From<ErrorType> for rexecutor::job::ErrorType {
    fn from(value: ErrorType) -> Self {
        match value {
            ErrorType::Panic => Self::Panic,
            ErrorType::Timeout => Self::Timeout,
            ErrorType::Cancelled => Self::Cancelled,
            ErrorType::Other(other) => Self::Other(other),
        }
    }
}

impl From<rexecutor::job::ErrorType> for ErrorType {
    fn from(value: rexecutor::job::ErrorType) -> Self {
        match value {
            rexecutor::job::ErrorType::Panic => Self::Panic,
            rexecutor::job::ErrorType::Timeout => Self::Timeout,
            rexecutor::job::ErrorType::Cancelled => Self::Cancelled,
            rexecutor::job::ErrorType::Other(other) => Self::Other(other),
        }
    }
}
