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

/// The default backoff strategy for exucutor jobs:
///  - exponential backoff with initial backoff of 4 seconds, and
///  - a max backoff of seven days,
///  - with a 10% jitter margin.
const DEFAULT_BACKOFF_STRATEGY: BackoffStrategy<Exponential> =
    BackoffStrategy::exponential(TimeDelta::seconds(4))
        .with_max(TimeDelta::days(7))
        .with_jitter(Jitter::Relative(0.1));

/// An enqueuable execution unit.
#[async_trait]
pub trait Executor {
    /// The type representing the executors arguments/data.
    ///
    /// If this is not needed it can be set to unit `()`.
    type Data;
    /// The type for storing the executors metadata.
    ///
    /// If this is not needed it can be set to unit `()`.
    type Metadata;
    /// The name of the executor.
    ///
    /// This is used to associate the jobs stored in the backend with this particular executor.
    ///
    /// This should be set to a unique value for the backend to ensure no clashes with other
    /// executors running on the same backend.
    ///
    /// The motivation for using a static string here is to enable developers to rename their rust
    /// types for their executor without breaking the integration with the backend.
    const NAME: &'static str;
    /// The maximum number of attempts to try this job before it is discarded.
    ///
    /// When enqueuing any given job this can be overridden via [`JobBuilder::with_max_attempts`].
    const MAX_ATTEMPTS: u16 = 5;
    /// The maximum number of concurrent jobs to be running. If set to [`None`] there will be no
    /// concurrency limit and an arbitrary number can be ran simultaneously.
    const MAX_CONCURRENCY: Option<usize> = None;
    /// This flag should be set to true if the job is computationally expensive.
    ///
    /// This is to prevent a computationally expensive job locking up the asynchronous runtime.
    ///
    /// Under the covers this results in the job executor being ran via
    /// [`tokio::task::spawn_blocking`]. See it's docs for more details about blocking futures.
    const BLOCKING: bool = false;
    /// This can be used to ensure that only unique jobs matching the [`UniquenessCriteria`] are
    /// inserted.
    ///
    /// If there is already a job inserted matching the given constraints there is the option to
    /// either update the current job or do nothing. See the docs of [`UniquenessCriteria`] for
    /// more details.
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

    #[derive(Debug, Clone, Serialize, Deserialize, Hash)]
    pub(crate) enum MockExecutionResult {
        Done,
        Panic,
        Timeout,
        Cancelled { reason: String },
        Snooze { delay: std::time::Duration },
        Error { error: MockError },
    }

    #[derive(Debug, Clone, Serialize, Deserialize, Hash)]
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
                MockExecutionResult::Panic => panic!("job paniced"),
                MockExecutionResult::Timeout => {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    ExecutionResult::Done
                }
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

        fn timeout(job: &Job<Self::Data, Self::Metadata>) -> Option<std::time::Duration> {
            if matches!(job.data, MockExecutionResult::Timeout) {
                Some(std::time::Duration::from_millis(1))
            } else {
                None
            }
        }
    }
}
