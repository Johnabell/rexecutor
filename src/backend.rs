use async_trait::async_trait;
use chrono::{DateTime, Utc};
use thiserror::Error;

use crate::{
    executor::ExecutorIdentifier,
    job::{Job, JobId, JobStatus},
};

#[async_trait]
pub trait Backend {
    async fn lock_and_load<D: Send>(&self, id: JobId) -> Result<Job<D>, BackendError>;
    async fn ready_jobs(&self) -> Result<Vec<ReadyJob>, BackendError>;
    async fn enqueue<D: Send>(&self, job: EnqueuableJob<D>) -> Result<JobId, BackendError>;
}

#[non_exhaustive]
pub struct EnqueuableJob<E> {
    pub status: JobStatus,
    pub executor: String,
    pub data: E,
    pub max_attempts: u32,
    pub schedule_at: DateTime<Utc>,
}

#[derive(Debug, Eq, PartialEq, Clone, Error)]
pub enum BackendError {
    #[error("Error derserialising data")]
    DecodeError,
}

pub struct ReadyJob {
    pub id: JobId,
    pub executor: ExecutorIdentifier,
}

pub struct DefaultBackend {}

#[cfg(test)]
pub(crate) mod test {
    use super::*;

    pub struct MockBackend {
        enqueue_return: Result<JobId, BackendError>,
    }

    impl Default for MockBackend {
        fn default() -> Self {
            Self {
                enqueue_return: Err(BackendError::DecodeError),
            }
        }
    }

    #[async_trait]
    impl Backend for MockBackend {
        async fn lock_and_load<D: Send>(&self, _id: JobId) -> Result<Job<D>, BackendError> {
            Err(BackendError::DecodeError)
        }
        async fn ready_jobs(&self) -> Result<Vec<ReadyJob>, BackendError> {
            Err(BackendError::DecodeError)
        }
        async fn enqueue<D: Send>(&self, _job: EnqueuableJob<D>) -> Result<JobId, BackendError> {
            self.enqueue_return.clone()
        }
    }

    impl MockBackend {
        pub(crate) fn expect_enqueue_returning(&mut self, result: Result<JobId, BackendError>) {
            self.enqueue_return = result;
        }
    }
}
