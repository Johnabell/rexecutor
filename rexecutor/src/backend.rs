use chrono::{DateTime, Utc};
use futures::Stream;
use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;

use crate::{
    executor::{self, Executor, ExecutorIdentifier},
    job::{Job, JobId},
};

pub trait Backend: Clone {
    fn subscribe_new_events<E>(
        self,
    ) -> impl std::future::Future<
        Output = impl Stream<Item = Result<Job<E::Data>, BackendError>> + Send,
    > + Send
    where
        E: Executor + Send,
        E::Data: DeserializeOwned + Send;
    fn enqueue<D: Send + Serialize>(
        &self,
        job: EnqueuableJob<D>,
    ) -> impl std::future::Future<Output = Result<JobId, BackendError>> + Send;
    fn mark_job_complete(
        &self,
        id: JobId,
    ) -> impl std::future::Future<Output = Result<(), BackendError>> + std::marker::Send;
    fn mark_job_retryable(
        &self,
        id: JobId,
        next_scheduled_at: DateTime<Utc>,
        error: ExecutionError,
    ) -> impl std::future::Future<Output = Result<(), BackendError>> + std::marker::Send;
    fn mark_job_discarded(
        &self,
        id: JobId,
        error: ExecutionError,
    ) -> impl std::future::Future<Output = Result<(), BackendError>> + std::marker::Send;
}

#[derive(Debug)]
pub struct ExecutionError {
    pub error_type: &'static str,
    pub message: String,
}

impl<T> From<T> for ExecutionError
where
    T: executor::ExecutionError,
{
    fn from(value: T) -> Self {
        Self {
            error_type: value.error_type(),
            message: value.to_string(),
        }
    }
}

// TODO: should this be non_exhaustive?
// #[non_exhaustive]
pub struct EnqueuableJob<E> {
    pub executor: String,
    pub data: E,
    pub max_attempts: u16,
    pub scheduled_at: DateTime<Utc>,
    pub tags: Vec<String>,
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

    impl Backend for MockBackend {
        async fn subscribe_new_events<E>(
            self,
        ) -> impl Stream<Item = Result<Job<E::Data>, BackendError>>
        where
            E: Executor + Send,
            E::Data: DeserializeOwned + Send,
        {
            futures::stream::empty()
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
    }

    impl MockBackend {
        pub(crate) fn expect_enqueue_returning(&mut self, result: Result<JobId, BackendError>) {
            self.enqueue_return.lock().unwrap().push(result)
        }
    }
}
