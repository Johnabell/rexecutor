use chrono::{DateTime, Utc};
use rexecutor::backend::BackendError;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(sqlx::Type, Debug, Clone, Copy, Serialize)]
#[cfg_attr(test, derive(PartialEq))]
#[sqlx(type_name = "rexecutor_job_status", rename_all = "lowercase")]
pub(crate) enum JobStatus {
    Complete,
    Executing,
    Scheduled,
    Retryable,
    Cancelled,
    Discarded,
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

#[derive(Debug)]
pub(crate) struct Job {
    pub id: i32,
    pub status: JobStatus,
    pub executor: String,
    pub data: serde_json::Value,
    pub attempt: i32,
    pub max_attempts: i32,
    pub errors: Vec<serde_json::Value>,
    pub inserted_at: DateTime<Utc>,
    pub scheduled_at: DateTime<Utc>,
    pub attempted_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub cancelled_at: Option<DateTime<Utc>>,
    pub discarded_at: Option<DateTime<Utc>>,
}

impl<D> TryFrom<Job> for rexecutor::job::Job<D>
where
    D: DeserializeOwned,
{
    type Error = BackendError;

    fn try_from(value: Job) -> Result<Self, Self::Error> {
        let data = serde_json::from_value(value.data)?;
        Ok(Self {
            id: value.id.into(),
            status: value.status.into(),
            executor: value.executor,
            data,
            attempt: value.attempt.try_into().unwrap(),
            attempted_at: value.attempted_at,
            max_attempts: value.max_attempts.try_into().unwrap(),
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

#[derive(Deserialize)]
pub struct JobError {
    pub attempt: u16,
    pub error_type: String,
    pub details: String,
    pub recorded_at: DateTime<Utc>,
}

impl From<JobError> for rexecutor::job::JobError {
    fn from(value: JobError) -> Self {
        Self {
            attempt: value.attempt,
            error_type: value.error_type,
            details: value.details,
            recorded_at: value.recorded_at,
        }
    }
}
