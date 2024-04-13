use std::pin::Pin;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;
use thiserror::Error;

use crate::{
    executor::ExecutorIdentifier,
    job::{uniqueness_criteria::UniquenessCriteria, ErrorType, JobError, JobId, JobStatus},
};
#[async_trait]
pub trait Backend: std::fmt::Debug {
    async fn subscribe_new_events(
        &self,
        executor_identifier: ExecutorIdentifier,
    ) -> Pin<Box<dyn Stream<Item = Result<Job, BackendError>> + Send>>;
    async fn enqueue<'a>(&self, job: EnqueuableJob<'a>) -> Result<JobId, BackendError>;
    async fn mark_job_complete(&self, id: JobId) -> Result<(), BackendError>;
    async fn mark_job_retryable(
        &self,
        id: JobId,
        next_scheduled_at: DateTime<Utc>,
        error: ExecutionError,
    ) -> Result<(), BackendError>;
    async fn mark_job_discarded(
        &self,
        id: JobId,
        error: ExecutionError,
    ) -> Result<(), BackendError>;
    async fn mark_job_cancelled(
        &self,
        id: JobId,
        error: ExecutionError,
    ) -> Result<(), BackendError>;
    async fn mark_job_snoozed(
        &self,
        id: JobId,
        next_scheduled_at: DateTime<Utc>,
    ) -> Result<(), BackendError>;
}

#[derive(Debug)]
pub struct ExecutionError {
    pub error_type: ErrorType,
    pub message: String,
}

// TODO: should this be non_exhaustive?
// #[non_exhaustive]
#[derive(Debug)]
pub struct EnqueuableJob<'a> {
    pub executor: String,
    pub data: serde_json::Value,
    pub max_attempts: u16,
    pub scheduled_at: DateTime<Utc>,
    pub tags: Vec<String>,
    pub uniqueness_criteria: Option<UniquenessCriteria<'a>>,
}

#[derive(Debug)]
pub struct Job {
    pub id: i32,
    pub status: JobStatus,
    pub executor: String,
    pub data: serde_json::Value,
    pub attempt: i32,
    pub max_attempts: i32,
    pub errors: Vec<JobError>,
    pub inserted_at: DateTime<Utc>,
    pub scheduled_at: DateTime<Utc>,
    pub attempted_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub cancelled_at: Option<DateTime<Utc>>,
    pub discarded_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Error)]
pub enum BackendError {
    #[error("Error encoding or decoding data")]
    // Not sure whether we should enforce json here
    EncodeDecodeError(#[from] serde_json::Error),
    #[error("System in bad state")]
    BadStateError,
}

pub struct DefaultBackend {}

#[cfg(test)]
pub(crate) mod test {

    use std::sync::{Arc, Mutex};

    use super::*;

    #[derive(Clone, Debug)]
    pub struct MockBackend {
        enqueue_return: Arc<Mutex<Vec<Result<JobId, BackendError>>>>,
    }

    impl Default for MockBackend {
        fn default() -> Self {
            Self {
                enqueue_return: Arc::new(vec![].into()),
            }
        }
    }

    #[async_trait]
    impl Backend for MockBackend {
        async fn subscribe_new_events(
            &self,
            _executor_name: ExecutorIdentifier,
        ) -> Pin<Box<dyn Stream<Item = Result<Job, BackendError>> + Send>> {
            Box::pin(futures::stream::empty())
        }
        async fn enqueue<'a>(&self, _job: EnqueuableJob<'a>) -> Result<JobId, BackendError> {
            self.enqueue_return
                .lock()
                .unwrap()
                .pop()
                .unwrap_or(Ok(0.into()))
        }
        async fn mark_job_complete(&self, _id: JobId) -> Result<(), BackendError> {
            Ok(())
        }
        async fn mark_job_retryable(
            &self,
            _id: JobId,
            _next_scheduled_at: DateTime<Utc>,
            _error: ExecutionError,
        ) -> Result<(), BackendError> {
            Ok(())
        }
        async fn mark_job_discarded(
            &self,
            _id: JobId,
            _error: ExecutionError,
        ) -> Result<(), BackendError> {
            Ok(())
        }
        async fn mark_job_cancelled(
            &self,
            _id: JobId,
            _error: ExecutionError,
        ) -> Result<(), BackendError> {
            Ok(())
        }
        async fn mark_job_snoozed(
            &self,
            _id: JobId,
            _next_scheduled_at: DateTime<Utc>,
        ) -> Result<(), BackendError> {
            Ok(())
        }
    }

    impl MockBackend {
        pub(crate) fn expect_enqueue_returning(&mut self, result: Result<JobId, BackendError>) {
            self.enqueue_return.lock().unwrap().push(result)
        }
    }
}
