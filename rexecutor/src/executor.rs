use async_trait::async_trait;
use chrono::TimeDelta;
use serde::{de::DeserializeOwned, Serialize};
use std::{error::Error, fmt::Display};

use crate::{
    backend::{Backend, Query},
    backoff::{BackoffStrategy, Exponential, Jitter, Strategy},
    job::{builder::JobBuilder, query::Where, uniqueness_criteria::UniquenessCriteria, Job, JobId},
    RexecuterError, GLOBAL_BACKEND,
};
type Result<T> = std::result::Result<T, RexecuterError>;

const DEFAULT_BACKOFF_STRATEGY: BackoffStrategy<Exponential> =
    BackoffStrategy::exponential(TimeDelta::seconds(4))
        .with_max(TimeDelta::days(7))
        .with_jitter(Jitter::Relative(0.1));

#[async_trait]
pub trait Executor {
    type Data;
    type Metadata;
    const NAME: &'static str;
    const MAX_ATTEMPTS: u16 = 5;
    const MAX_CONCURRENCY: Option<usize> = None;
    const BLOCKING: bool = false;
    const UNIQUENESS_CRITERIA: Option<UniquenessCriteria<'static>> = None;

    async fn execute(job: Job<Self::Data, Self::Metadata>) -> ExecutionResult;

    fn backoff(job: &Job<Self::Data, Self::Metadata>) -> TimeDelta {
        DEFAULT_BACKOFF_STRATEGY.backoff(job.attempt)
    }

    fn timeout(_job: &Job<Self::Data, Self::Metadata>) -> Option<std::time::Duration> {
        None
    }

    fn builder<'a>() -> JobBuilder<'a, Self>
    where
        Self: Sized,
        Self::Data: Serialize + DeserializeOwned,
        Self::Metadata: Serialize + DeserializeOwned,
    {
        Default::default()
    }

    // Should the job manipulation API always return the job?
    async fn cancel_job(
        job_id: JobId,
        cancellation_reason: Box<dyn CancellationReason>,
    ) -> Result<()> {
        let backend = GLOBAL_BACKEND.get().ok_or(RexecuterError::GlobalBackend)?;
        Self::cancel_job_on_backend(job_id, cancellation_reason, backend.as_ref()).await
    }

    async fn cancel_job_on_backend<B>(
        job_id: JobId,
        cancellation_reason: Box<dyn CancellationReason>,
        backend: &B,
    ) -> Result<()>
    where
        B: Backend + ?Sized + Sync,
    {
        backend
            .mark_job_cancelled(job_id, cancellation_reason.into())
            .await?;
        Ok(())
    }

    async fn rerun_job(job_id: JobId) -> Result<()> {
        let backend = GLOBAL_BACKEND.get().ok_or(RexecuterError::GlobalBackend)?;
        Self::rerun_job_on_backend(job_id, backend.as_ref()).await
    }

    async fn rerun_job_on_backend<B>(job_id: JobId, backend: &B) -> Result<()>
    where
        B: Backend + ?Sized + Sync,
    {
        backend.rerun_job(job_id).await?;
        Ok(())
    }

    async fn query_jobs<'a>(
        query: Where<'a, Self::Data, Self::Metadata>,
    ) -> Result<Vec<Job<Self::Data, Self::Metadata>>>
    where
        Self::Data: 'static + Send + Serialize + DeserializeOwned + Sync,
        Self::Metadata: 'static + Send + Serialize + DeserializeOwned + Sync,
    {
        let backend = GLOBAL_BACKEND.get().ok_or(RexecuterError::GlobalBackend)?;
        Self::query_jobs_on_backend(query, backend.as_ref()).await
    }

    async fn query_jobs_on_backend<'a, B>(
        query: Where<'a, Self::Data, Self::Metadata>,
        backend: &B,
    ) -> Result<Vec<Job<Self::Data, Self::Metadata>>>
    where
        Self::Data: 'static + Send + Serialize + DeserializeOwned + Sync,
        Self::Metadata: 'static + Send + Serialize + DeserializeOwned + Sync,
        B: Backend + ?Sized + Sync,
    {
        Ok(backend
            .query(Query::try_from(query.0)?.for_executor(Self::NAME))
            .await?
            .into_iter()
            .map(TryFrom::try_from)
            .collect::<std::result::Result<Vec<_>, _>>()?)
    }

    async fn update_job(job: Job<Self::Data, Self::Metadata>) -> Result<()>
    where
        Self::Data: 'static + Send + Serialize + Sync,
        Self::Metadata: 'static + Send + Serialize + Sync,
    {
        let backend = GLOBAL_BACKEND.get().ok_or(RexecuterError::GlobalBackend)?;
        Self::update_job_on_backend(job, backend.as_ref()).await
    }

    async fn update_job_on_backend<B>(
        job: Job<Self::Data, Self::Metadata>,
        backend: &B,
    ) -> Result<()>
    where
        B: Backend + ?Sized + Sync,
        Self::Data: 'static + Serialize + Send + Sync,
        Self::Metadata: 'static + Serialize + Send + Sync,
    {
        let job = job.try_into()?;
        backend.update_job(job).await?;
        Ok(())
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub struct ExecutorIdentifier(&'static str);

impl From<&'static str> for ExecutorIdentifier {
    fn from(value: &'static str) -> Self {
        Self(value)
    }
}

impl ExecutorIdentifier {
    pub fn as_str(&self) -> &'static str {
        self.0
    }
}

impl std::ops::Deref for ExecutorIdentifier {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

pub enum ExecutionResult {
    Done,
    Cancelled { reason: Box<dyn CancellationReason> },
    Snooze { delay: TimeDelta },
    Error { error: Box<dyn ExecutionError> },
}

impl<T> From<T> for ExecutionResult
where
    T: ExecutionError + 'static,
{
    fn from(value: T) -> Self {
        Self::Error {
            error: Box::new(value),
        }
    }
}

pub trait ExecutionError: Error + Send {
    fn error_type(&self) -> &'static str;
}

pub trait CancellationReason: Display + Send {}

impl<T> CancellationReason for T where T: Display + Send {}

#[cfg(test)]
pub(crate) mod test {
    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};

    use crate::job::Job;

    use super::*;

    pub(crate) struct SimpleExecutor;

    #[async_trait]
    impl Executor for SimpleExecutor {
        type Data = String;
        type Metadata = String;
        const NAME: &'static str = "simple_executor";
        const MAX_ATTEMPTS: u16 = 2;
        async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
            ExecutionResult::Done
        }
    }

    pub(crate) struct MockReturnExecutor;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub(crate) enum MockExecutionResult {
        Done,
        Cancelled { reason: String },
        Snooze { delay: std::time::Duration },
        Error { error: MockError },
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub(crate) struct MockError(pub String);
    impl std::fmt::Display for MockError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }
    impl Error for MockError {}
    impl ExecutionError for MockError {
        fn error_type(&self) -> &'static str {
            "custom"
        }
    }

    #[async_trait]
    impl Executor for MockReturnExecutor {
        type Data = MockExecutionResult;
        type Metadata = ();
        const NAME: &'static str = "basic_executor";
        const MAX_ATTEMPTS: u16 = 2;
        async fn execute(job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
            match job.data {
                MockExecutionResult::Done => ExecutionResult::Done,
                MockExecutionResult::Cancelled { reason } => ExecutionResult::Cancelled {
                    reason: Box::new(reason),
                },
                MockExecutionResult::Snooze { delay } => ExecutionResult::Snooze {
                    delay: TimeDelta::from_std(delay).unwrap(),
                },
                MockExecutionResult::Error { error } => ExecutionResult::Error {
                    error: Box::new(error),
                },
            }
        }
    }
}
