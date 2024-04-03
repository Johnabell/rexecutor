use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;

use crate::{
    executor::ExecutorIdentifier,
    job::{Job, JobId},
};

#[async_trait]
pub trait Backend: Clone {
    async fn lock_and_load<D: Send + DeserializeOwned>(
        &self,
        id: JobId,
    ) -> Result<Option<Job<D>>, BackendError>;
    async fn ready_jobs(&self) -> Result<Vec<ReadyJob>, BackendError>;
    async fn enqueue<D: Send + Serialize>(
        &self,
        job: EnqueuableJob<D>,
    ) -> Result<JobId, BackendError>;
    async fn next_job_scheduled_at(&self) -> Result<Option<DateTime<Utc>>, BackendError>;
    async fn mark_job_complete(&self, id: JobId) -> Result<(), BackendError>;
}

// TODO: should this be non_exhaustive?
// #[non_exhaustive]
pub struct EnqueuableJob<E> {
    pub executor: String,
    pub data: E,
    pub max_attempts: u16,
    pub scheduled_at: DateTime<Utc>,
}

#[derive(Debug, Error)]
pub enum BackendError {
    #[error("Error encoding or decoding data")]
    // Not sure whether we should enforce json here
    EncodeDecodeError(#[from] serde_json::Error),
    #[error("System in bad state")]
    BadStateError,
}

pub struct ReadyJob {
    pub id: JobId,
    pub executor: ExecutorIdentifier,
}

pub struct DefaultBackend {}

#[cfg(test)]
pub(crate) mod test {

    use std::sync::{Arc, Mutex};

    use super::*;

    #[derive(Clone)]
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
        async fn lock_and_load<D: Send + DeserializeOwned>(
            &self,
            _id: JobId,
        ) -> Result<Option<Job<D>>, BackendError> {
            Ok(None)
        }
        async fn ready_jobs(&self) -> Result<Vec<ReadyJob>, BackendError> {
            Ok(vec![])
        }
        async fn enqueue<D: Send + Serialize>(
            &self,
            _job: EnqueuableJob<D>,
        ) -> Result<JobId, BackendError> {
            self.enqueue_return
                .lock()
                .unwrap()
                .pop()
                .unwrap_or(Ok(0.into()))
        }
        async fn next_job_scheduled_at(&self) -> Result<Option<DateTime<Utc>>, BackendError> {
            Ok(None)
        }
        async fn mark_job_complete(&self, _id: JobId) -> Result<(), BackendError> {
            Ok(())
        }
    }

    impl MockBackend {
        pub(crate) fn expect_enqueue_returning(&mut self, result: Result<JobId, BackendError>) {
            self.enqueue_return.lock().unwrap().push(result)
        }
    }
}
