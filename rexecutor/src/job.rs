//! Structs and definitions for jobs.
//!
//! The main type is the [`Job`] representing a single executable job in the system.
use std::fmt::Display;

use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::backend;

pub mod builder;
pub mod query;
pub(crate) mod runner;
pub mod uniqueness_criteria;

/// The id of a job.
#[derive(Debug, Eq, Clone, Copy)]
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

impl PartialEq<i32> for JobId {
    fn eq(&self, other: &i32) -> bool {
        self.0 == *other
    }
}

impl PartialEq<JobId> for i32 {
    fn eq(&self, other: &JobId) -> bool {
        *self == other.0
    }
}

impl PartialEq<JobId> for JobId {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JobId({})", self.0)
    }
}

/// The main data type representing a single executable job.
///
/// The job is passed as an argument to a number of the methods of [`crate::executor::Executor`] in
/// particular the [`crate::executor::Executor::execute`] method responsible for defining how the
/// job should be ran.
#[derive(Debug, Eq, PartialEq, Clone)]
// #[non_exhaustive]
pub struct Job<D, M> {
    /// The id of the job.
    pub id: JobId,
    /// The current status of the job.
    pub status: JobStatus,
    /// The name of the executor which is responsible for running this job.
    ///
    /// Determined via [`crate::executor::Executor::NAME`].
    pub executor: String,
    /// The data for running this job.
    pub data: D,
    /// Any metadata associated with this job.
    pub metadata: Option<M>,
    /// The current attempt of the job.
    pub attempt: u16,
    /// The maximum number of times to attempt this job before discarding it.
    pub max_attempts: u16,
    /// The priority of the job.
    ///
    /// The lower the number the higher the priority, i.e. the highest priority jobs are those with
    /// priority zero.
    ///
    /// The priority is used to determine which jobs should be ran first when many jobs are
    /// scheduled to run at the same time. The priority is applied to all jobs for the same
    /// executor and not used to prioritise between different executors. Instead all executors will
    /// attempt to execute their highest priority jobs simultaneously.
    pub priority: u16,
    /// The tags associated with this job.
    pub tags: Vec<String>,
    /// Any errors that have occurred in previous attempts to run this job.
    pub errors: Vec<JobError>,
    /// The timestamp when this job was inserted.
    pub inserted_at: DateTime<Utc>,
    /// The timestamp when the job should next be executed.
    pub scheduled_at: DateTime<Utc>,
    /// The timestamp of the last attempt to execute the job.
    pub attempted_at: Option<DateTime<Utc>>,
    /// The timestamp when the job completed successfully.
    pub completed_at: Option<DateTime<Utc>>,
    /// The timestamp when the job was cancelled.
    pub cancelled_at: Option<DateTime<Utc>>,
    /// The timestamp when the job was discarded.
    pub discarded_at: Option<DateTime<Utc>>,
}

impl<D, M> TryFrom<backend::Job> for Job<D, M>
where
    D: DeserializeOwned,
    M: DeserializeOwned,
{
    type Error = serde_json::Error;

    fn try_from(value: backend::Job) -> Result<Self, Self::Error> {
        let data = serde_json::from_value(value.data)?;
        let metadata = serde_json::from_value(value.metadata)?;
        Ok(Self {
            id: value.id.into(),
            status: value.status,
            executor: value.executor,
            data,
            metadata,
            attempt: value.attempt.try_into().unwrap(),
            attempted_at: value.attempted_at,
            max_attempts: value.max_attempts.try_into().unwrap(),
            priority: value.priority.try_into().unwrap(),
            tags: value.tags,
            errors: value.errors,
            inserted_at: value.inserted_at,
            scheduled_at: value.scheduled_at,
            completed_at: value.completed_at,
            cancelled_at: value.cancelled_at,
            discarded_at: value.discarded_at,
        })
    }
}

impl<D, M> Job<D, M> {
    pub(crate) fn is_final_attempt(&self) -> bool {
        self.attempt == self.max_attempts
    }
}

/// The status of the job.
///
/// When a job is first inserted it will be in the [`JobStatus::Scheduled`] state.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum JobStatus {
    /// The status of the job after it has ran successfully.
    ///
    /// I.e The state of the job after [`crate::executor::Executor`] returns
    /// [`crate::executor::ExecutionResult::Done`].
    Complete,
    /// The status of the job while an executor is running the job.
    Executing,
    /// Initial state of the job before any attempt has been made to run the job.
    ///
    /// Usually this will imply that the jobs `scheduled_at` field is in the future. However, this
    /// is not always the case. It is possible that a job gets inserted with a `scheduled_at` in
    /// the past, or when the system has been offline for some time.
    Scheduled,
    /// State of the job after the first failed attempt to execute the job if the job's
    /// `max_attempts` field is greater than one.
    Retryable,
    /// The state of the job after [`crate::executor::Executor`] returns
    /// [`crate::executor::ExecutionResult::Cancel`].
    Cancelled,
    /// The state of the job attempting the job `max_attempts` times without success.
    Discarded,
}

impl JobStatus {
    /// A static array containing all the possible job statuses.
    pub const ALL: [JobStatus; 6] = [
        Self::Complete,
        Self::Executing,
        Self::Scheduled,
        Self::Retryable,
        Self::Cancelled,
        Self::Discarded,
    ];
}

/// A record of an error that was returned from a job.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct JobError {
    /// The attempt when this error occurred.
    pub attempt: u16,
    /// The type of error that occurred.
    pub error_type: ErrorType,
    /// The details of the error.
    pub details: String,
    /// The time at which this error was recorded.
    pub recorded_at: DateTime<Utc>,
}

/// Calcification of the types of errors that can cause a job to fail to execute successfully.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorType {
    /// The execution of the job resulted in a `panic`.
    Panic,
    /// The job failed to complete before it timed out.
    ///
    /// The timeout can be set using [`crate::executor::Executor::timeout`].
    Timeout,
    /// The job was cancelled.
    ///
    /// This is achieved by returning [`crate::executor::ExecutionResult::Cancel`] from
    /// [`crate::executor::Executor`].
    Cancelled,
    /// A customer error type.
    ///
    /// The details of which will be taken from the error returned as part of
    /// [`crate::executor::ExecutionResult::Error`] from [`crate::executor::Executor`].
    #[serde(untagged)]
    Other(String),
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn job_id_display() {
        let job_id = JobId(42);
        assert_eq!(&job_id.to_string(), "JobId(42)");
    }

    #[test]
    fn job_id_from_i32() {
        let value = 42;
        let job_id = JobId(42);
        let into_job_id: JobId = value.into();
        let into_i32: i32 = job_id.into();

        assert_eq!(into_job_id, job_id);
        assert_eq!(into_i32, value);
    }

    #[test]
    fn from_backend_job_errors() {
        let backend_job = backend::Job::raw_job();
        let result = Job::<String, String>::try_from(backend_job);
        assert!(result.is_err());

        let backend_job =
            backend::Job::raw_job().with_raw_metadata(serde_json::Value::Number(42.into()));
        let result = Job::<(), String>::try_from(backend_job);
        assert!(result.is_err());
    }

    #[test]
    fn from_backend_job() {
        let backend_job =
            backend::Job::raw_job().with_raw_data(serde_json::Value::String("data".to_owned()));
        let job = Job::<String, String>::try_from(backend_job.clone()).expect("Should not error");

        assert_eq!(job.id, backend_job.id);
        assert_eq!(job.status, backend_job.status);
        assert_eq!(job.executor, backend_job.executor);
        assert_eq!(&job.data, "data");
        assert_eq!(job.metadata, None);
        assert_eq!(job.attempt as i32, backend_job.attempt);
        assert_eq!(job.max_attempts as i32, backend_job.max_attempts);
        assert_eq!(job.priority as i32, backend_job.priority);
        assert_eq!(job.tags, backend_job.tags);
        assert_eq!(job.errors, backend_job.errors);
        assert_eq!(job.inserted_at, backend_job.inserted_at);
        assert_eq!(job.scheduled_at, backend_job.scheduled_at);
        assert_eq!(job.attempted_at, backend_job.attempted_at);
        assert_eq!(job.completed_at, backend_job.completed_at);
        assert_eq!(job.cancelled_at, backend_job.cancelled_at);
        assert_eq!(job.discarded_at, backend_job.discarded_at);
    }
}
