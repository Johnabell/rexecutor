use async_trait::async_trait;

use crate::{
    job::{Job, JobId}, executor::ExecutorIdentifier,
};

#[async_trait]
pub trait Backend {
    async fn lock_and_load<D>(id: JobId) -> Result<Job<D>, BackendError>;
    async fn ready_jobs() -> Result<Vec<ReadyJob>, BackendError>;
    async fn enqueue<D>(job: Job<D>) -> Result<JobId, BackendError>;
}

pub enum BackendError {
    DecodeError,
}

pub struct ReadyJob {
    pub id: JobId,
    pub executor: ExecutorIdentifier,
}
