//! Definition of the main trait [`Executor`] for defining enqueuable executions units (jobs).
//!
//! Jobs are defined by creating a struct/enum and implementing `Executor` for it.
//!
//! # Example
//!
//! You can define and enqueue a job as follows:
//!
//! ```
//! # use rexecutor::prelude::*;
//! # use chrono::TimeDelta;
//! struct EmailJob;
//!
//! #[async_trait::async_trait]
//! impl Executor for EmailJob {
//!     type Data = String;
//!     type Metadata = String;
//!     const NAME: &'static str = "email_job";
//!     const MAX_ATTEMPTS: u16 = 2;
//!     async fn execute(job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
//!         println!("{} running, with args: {}", Self::NAME, job.data);
//!         /// Do something important with an email
//!         ExecutionResult::Done
//!     }
//! }
//!
//! # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
//! let _ = EmailJob::builder()
//!     .with_data("bob.shuruncle@example.com".to_owned())
//!     .schedule_in(TimeDelta::hours(3))
//!     .enqueue()
//!     .await;
//! # });
//! ```
//!
//! # Unique jobs
//!
//! It is possible to ensure uniqueness of jobs based on certain criteria. This can be defined as
//! part of the implementation of `Executor` via [`Executor::UNIQUENESS_CRITERIA`] or when
//! inserting the job via [`JobBuilder::unique`].
//!
//! For example to ensure that only one unique job is ran every five minutes it is possible to use
//! the following uniqueness criteria.
//!
//! ```
//! # use rexecutor::prelude::*;
//! # use chrono::TimeDelta;
//! struct UniqueJob;
//!
//! #[async_trait::async_trait]
//! impl Executor for UniqueJob {
//!     type Data = ();
//!     type Metadata = ();
//!     const NAME: &'static str = "unique_job";
//!     const MAX_ATTEMPTS: u16 = 1;
//!     const UNIQUENESS_CRITERIA: Option<UniquenessCriteria<'static>> = Some(
//!         UniquenessCriteria::by_executor()
//!             .and_within(TimeDelta::seconds(300))
//!             .on_conflict(Replace::priority().for_statuses(&JobStatus::ALL)),
//!     );
//!     async fn execute(job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
//!         println!("{} running, with args: {:?}", Self::NAME, job.data);
//!         // Do something important
//!         ExecutionResult::Done
//!     }
//! }
//!
//! # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
//! // Only one of these jobs will be enqueued
//! let _ = UniqueJob::builder().enqueue().await;
//! let _ = UniqueJob::builder().enqueue().await;
//! # });
//! ```
//!
//! Additionally it is possible to specify what action should be taken when there is a conflicting
//! job. In the example above the priority is override. For more details of how to use uniqueness
//! see [`UniquenessCriteria`].
//!
//!
//! # Overriding [`Executor`] default values
//!
//! When defining an [`Executor`] you specify the maximum number of attempts via
//! [`Executor::MAX_ATTEMPTS`]. However, when inserting a job it is possible to override this value
//! by calling [`JobBuilder::with_max_attempts`] (if not called the max attempts will be equal to
//! [`Executor::MAX_ATTEMPTS`].
//!
//! Similarly, the executor can define a job uniqueness criteria via
//! [`Executor::UNIQUENESS_CRITERIA`]. However, using [`JobBuilder::unique`] it is possible to
//! override this value for a specific job.
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
///
/// Jobs are defined by creating a struct/enum and implementing `Executor` for it.
///
/// # Example
///
/// You can define and enqueue a job as follows:
///
/// ```
/// # use rexecutor::prelude::*;
/// # use chrono::TimeDelta;
/// struct EmailJob;
///
/// #[async_trait::async_trait]
/// impl Executor for EmailJob {
///     type Data = String;
///     type Metadata = String;
///     const NAME: &'static str = "email_job";
///     const MAX_ATTEMPTS: u16 = 2;
///     async fn execute(job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
///         println!("{} running, with args: {}", Self::NAME, job.data);
///         /// Do something important with an email
///         ExecutionResult::Done
///     }
/// }
///
/// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
/// let _ = EmailJob::builder()
///     .with_data("bob.shuruncle@example.com".to_owned())
///     .schedule_in(TimeDelta::hours(3))
///     .enqueue()
///     .await;
/// # });
/// ```
///
/// # Unique jobs
///
/// It is possible to ensure uniqueness of jobs based on certain criteria. This can be defined as
/// part of the implementation of `Executor` via [`Executor::UNIQUENESS_CRITERIA`] or when
/// inserting the job via [`JobBuilder::unique`].
///
/// For example to ensure that only one unique job is ran every five minutes it is possible to use
/// the following uniqueness criteria.
///
/// ```
/// # use rexecutor::prelude::*;
/// # use chrono::TimeDelta;
/// struct UniqueJob;
///
/// #[async_trait::async_trait]
/// impl Executor for UniqueJob {
///     type Data = ();
///     type Metadata = ();
///     const NAME: &'static str = "unique_job";
///     const MAX_ATTEMPTS: u16 = 1;
///     const UNIQUENESS_CRITERIA: Option<UniquenessCriteria<'static>> = Some(
///         UniquenessCriteria::by_executor()
///             .and_within(TimeDelta::seconds(300))
///             .on_conflict(Replace::priority().for_statuses(&JobStatus::ALL)),
///     );
///     async fn execute(job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
///         println!("{} running, with args: {:?}", Self::NAME, job.data);
///         // Do something important
///         ExecutionResult::Done
///     }
/// }
///
/// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
/// // Only one of these jobs will be enqueued
/// let _ = UniqueJob::builder().enqueue().await;
/// let _ = UniqueJob::builder().enqueue().await;
/// # });
/// ```
///
/// Additionally it is possible to specify what action should be taken when there is a conflicting
/// job. In the example above the priority is override. For more details of how to use uniqueness
/// see [`UniquenessCriteria`].
///
///
/// # Overriding [`Executor`] default values
///
/// When defining an [`Executor`] you specify the maximum number of attempts via
/// [`Executor::MAX_ATTEMPTS`]. However, when inserting a job it is possible to override this value
/// by calling [`JobBuilder::with_max_attempts`] (if not called the max attempts will be equal to
/// [`Executor::MAX_ATTEMPTS`].
///
/// Similarly, the executor can define a job uniqueness criteria via
/// [`Executor::UNIQUENESS_CRITERIA`]. However, using [`JobBuilder::unique`] it is possible to
/// override this value for a specific job.
#[async_trait]
pub trait Executor {
    /// The type representing the executors arguments/data.
    ///
    /// If this is not needed it can be set to unit `()`.
    type Data: Serialize + DeserializeOwned;

    /// The type for storing the executors metadata this is always optional.
    ///
    /// If this is not needed it can be set to unit `()`.
    type Metadata: Serialize + DeserializeOwned;

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
    /// [`tokio::task::spawn_blocking`]. See the docs for more details about blocking futures.
    const BLOCKING: bool = false;

    /// This can be used to ensure that only unique jobs matching the [`UniquenessCriteria`] are
    /// inserted.
    ///
    /// If there is already a job inserted matching the given constraints there is the option to
    /// either update the current job or do nothing. See the docs of [`UniquenessCriteria`] for
    /// more details.
    ///
    /// It is possible to override when inserting a specific job via [`JobBuilder::unique`].
    const UNIQUENESS_CRITERIA: Option<UniquenessCriteria<'static>> = None;

    /// The work this a job should do when executing.
    async fn execute(job: Job<Self::Data, Self::Metadata>) -> ExecutionResult;

    /// Defines the backoff after a failed job.
    ///
    /// The default backoff strategy is
    ///  - exponential backoff with initial backoff of 4 seconds, and
    ///  - a max backoff of seven days,
    ///  - with a 10% jitter margin.
    ///
    /// For some standard backoff strategies see the [`crate::backoff`].
    ///
    /// **Note**: this method is called preemptively before attempting execution.
    fn backoff(job: &Job<Self::Data, Self::Metadata>) -> TimeDelta {
        DEFAULT_BACKOFF_STRATEGY.backoff(job.attempt)
    }

    /// The timeout when executing a specific job.
    ///
    /// If this returns [`None`] (default behaviour) then the job will be ran without a timeout.
    fn timeout(_job: &Job<Self::Data, Self::Metadata>) -> Option<std::time::Duration> {
        None
    }

    /// The builder for inserting jobs.
    ///
    /// For details of the API see [`JobBuilder`].
    fn builder<'a>() -> JobBuilder<'a, Self>
    where
        Self: Sized,
        Self::Data: Serialize + DeserializeOwned,
        Self::Metadata: Serialize + DeserializeOwned,
    {
        Default::default()
    }

    // Should the job manipulation API always return the job?

    /// Cancel a job with the given reason.
    ///
    /// To make use this API and the global backend, [`crate::Rexecutor::set_global_backend`]
    /// should be called. If this hasn't been called, then a [`RexecuterError::GlobalBackend`]
    /// will be returned.
    ///
    /// # Example
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    /// # let job_id = 0.into();
    /// # use chrono::TimeDelta;
    /// # pub(crate) struct SimpleExecutor;
    /// #
    /// # #[async_trait::async_trait]
    /// # impl Executor for SimpleExecutor {
    /// #     type Data = String;
    /// #     type Metadata = String;
    /// #     const NAME: &'static str = "simple_executor";
    /// #     const MAX_ATTEMPTS: u16 = 2;
    /// #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
    /// #         ExecutionResult::Done
    /// #     }
    /// # }
    /// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
    /// let reason = Box::new("No longer needed");
    /// let result = SimpleExecutor::cancel_job(job_id, reason).await;
    /// # });
    /// ```
    async fn cancel_job(
        job_id: JobId,
        cancellation_reason: Box<dyn CancellationReason>,
    ) -> Result<()> {
        let backend = GLOBAL_BACKEND.get().ok_or(RexecuterError::GlobalBackend)?;
        Self::cancel_job_on_backend(job_id, cancellation_reason, backend.as_ref()).await
    }

    /// Cancel a job with the given reason on the provided backend.
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

    /// Rerun a completed or discarded job.
    ///
    /// To make use this API and the global backend, [`crate::Rexecutor::set_global_backend`]
    /// should be called. If this hasn't been called, then a [`RexecuterError::GlobalBackend`]
    /// will be returned.
    ///
    /// # Example
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    /// # let job_id = 0.into();
    /// # use chrono::TimeDelta;
    /// # pub(crate) struct SimpleExecutor;
    /// #
    /// # #[async_trait::async_trait]
    /// # impl Executor for SimpleExecutor {
    /// #     type Data = String;
    /// #     type Metadata = String;
    /// #     const NAME: &'static str = "simple_executor";
    /// #     const MAX_ATTEMPTS: u16 = 2;
    /// #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
    /// #         ExecutionResult::Done
    /// #     }
    /// # }
    /// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
    /// let result = SimpleExecutor::rerun_job(job_id).await;
    /// # });
    /// ```
    async fn rerun_job(job_id: JobId) -> Result<()> {
        let backend = GLOBAL_BACKEND.get().ok_or(RexecuterError::GlobalBackend)?;
        Self::rerun_job_on_backend(job_id, backend.as_ref()).await
    }

    /// Rerun a completed or discarded job on the provided backend.
    async fn rerun_job_on_backend<B>(job_id: JobId, backend: &B) -> Result<()>
    where
        B: Backend + ?Sized + Sync,
    {
        backend.rerun_job(job_id).await?;
        Ok(())
    }

    /// Query the jobs for this executor using the provided query.
    ///
    /// For details of the query API see [`Where`].
    ///
    /// To make use this API and the global backend, [`crate::Rexecutor::set_global_backend`]
    /// should be called. If this hasn't been called, then a [`RexecuterError::GlobalBackend`]
    /// will be returned.
    ///
    /// # Example
    ///
    /// To query all completed jobs for the `SimpleExecutor`:
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    /// # use chrono::TimeDelta;
    /// # pub(crate) struct SimpleExecutor;
    /// #
    /// # #[async_trait::async_trait]
    /// # impl Executor for SimpleExecutor {
    /// #     type Data = String;
    /// #     type Metadata = String;
    /// #     const NAME: &'static str = "simple_executor";
    /// #     const MAX_ATTEMPTS: u16 = 2;
    /// #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
    /// #         ExecutionResult::Done
    /// #     }
    /// # }
    /// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
    /// let result = SimpleExecutor::query_jobs(Where::status_equals(JobStatus::Complete)).await;
    /// # });
    /// ```
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

    /// Query the jobs for this executor using the provided query on the provided backend.
    ///
    /// For details of the query API see [`Where`].
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

    /// Update the given job
    ///
    /// To make use this API and the global backend, [`crate::Rexecutor::set_global_backend`]
    /// should be called. If this hasn't been called, then a [`RexecuterError::GlobalBackend`]
    /// will be returned.
    ///
    /// # Example
    ///
    /// To query all completed jobs for the `SimpleExecutor`:
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    /// # use chrono::TimeDelta;
    /// # pub(crate) struct SimpleExecutor;
    /// #
    /// # #[async_trait::async_trait]
    /// # impl Executor for SimpleExecutor {
    /// #     type Data = String;
    /// #     type Metadata = String;
    /// #     const NAME: &'static str = "simple_executor";
    /// #     const MAX_ATTEMPTS: u16 = 2;
    /// #     async fn execute(mut job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
    /// job.max_attempts = 5;
    /// let result = SimpleExecutor::update_job(job).await;
    /// #         ExecutionResult::Done
    /// #     }
    /// # }
    /// ```
    async fn update_job(job: Job<Self::Data, Self::Metadata>) -> Result<()>
    where
        Self::Data: 'static + Send + Serialize + Sync,
        Self::Metadata: 'static + Send + Serialize + Sync,
    {
        let backend = GLOBAL_BACKEND.get().ok_or(RexecuterError::GlobalBackend)?;
        Self::update_job_on_backend(job, backend.as_ref()).await
    }

    /// Update the given job
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

/// An identifier for an executor.
#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub struct ExecutorIdentifier(&'static str);

impl From<&'static str> for ExecutorIdentifier {
    fn from(value: &'static str) -> Self {
        Self(value)
    }
}

impl ExecutorIdentifier {
    /// Returns a view of this [`ExecutorIdentifier`] as a string slice.
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

/// The return value of a job execution.
///
/// The value returns determines what will happen to the completed job.
///
/// If the jobs completes successfully [`ExecutionResult::Done`] should be returned. This will
/// result in the job status being set to [`crate::job::JobStatus::Complete`].
///
/// If the job resulted in a error, then [`ExecutionResult::Error`] can be returned. This will set
/// the job status equal to [`crate::job::JobStatus::Retryable`] if the job's `attempt` value is
/// less than its `max_attempts` value. The retry will be scheduled after the `TimeDelta` returned
/// from the [`Executor::backoff`].
///
/// If the job should be attempted later then it is possible to snooze the job by returning
/// [`ExecutionResult::Snooze`] providing the delay to wait until trying the job again. Unlike
/// returning an error this does not increment the job's attempt field.
///
/// If the job should not attempt to be retried and did not complete successfully then
/// [`ExecutionResult::Cancel`] can be returned passing in the cancellation reason.
pub enum ExecutionResult {
    /// The job completed successfully and should be marked as completed.
    Done,
    /// The job did not complete successfully and should not be retried.
    Cancel {
        /// The reason the job should be cancelled. This will be stored in the `error` field on the
        /// job.
        reason: Box<dyn CancellationReason>,
    },
    /// The job should be retried after the `delay`.
    ///
    /// Note unlike returning an error returning `Snooze` will not increment the job's attempt
    /// number.
    Snooze {
        /// The delay to wait until attempting this job again.
        delay: TimeDelta,
    },
    /// The job's execution resulted in an error and the job should either be retired if
    /// `job.attempt < job.max_attempts` or be discarded if `job.attempt == job.max_attempts`.
    Error {
        /// The error that occurred when attempting to execute this job.
        error: Box<dyn ExecutionError>,
    },
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

/// Errors returned from jobs as part of [`ExecutionResult::Error`] should implement this trait.
/// The error type can be later used for identifying particular failures in the job's errors array.
pub trait ExecutionError: Error + Send {
    /// An identifier for the type of error that occurred while execution the job.
    fn error_type(&self) -> &'static str;
}

/// Anything that can be used as a cancellation recons. Generally this should not need to be
/// implemented and instead users of the library should be able to rely on the blanket
/// implementations.
pub trait CancellationReason: Display + Send {}

impl<T> CancellationReason for T where T: Display + Send {}

#[cfg(test)]
pub(crate) mod test {
    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};

    use crate::{
        backend::{BackendError, MockBackend},
        job::JobStatus,
    };

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
                MockExecutionResult::Cancelled { reason } => ExecutionResult::Cancel {
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

    #[tokio::test]
    async fn cancel_job_error() {
        let job_id = 0.into();
        let mut backend = MockBackend::default();
        backend
            .expect_mark_job_cancelled()
            .returning(move |_, _| Err(BackendError::BadState));

        let reason = Box::new("No longer needed");
        let result = SimpleExecutor::cancel_job_on_backend(job_id, reason, &backend).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn cancel_job_success() {
        let job_id = 0.into();
        let mut backend = MockBackend::default();
        backend
            .expect_mark_job_cancelled()
            .returning(move |_, _| Ok(()));

        let reason = Box::new("No longer needed");
        let result = SimpleExecutor::cancel_job_on_backend(job_id, reason, &backend).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn rerun_job_error() {
        let job_id = 0.into();
        let mut backend = MockBackend::default();
        backend
            .expect_rerun_job()
            .returning(move |_| Err(BackendError::BadState));

        let result = SimpleExecutor::rerun_job_on_backend(job_id, &backend).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn rerun_job_success() {
        let job_id = 0.into();
        let mut backend = MockBackend::default();
        backend.expect_rerun_job().returning(move |_| Ok(()));

        let result = SimpleExecutor::rerun_job_on_backend(job_id, &backend).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn query_jobs_error() {
        let mut backend = MockBackend::default();
        backend
            .expect_query()
            .returning(move |_| Err(BackendError::BadState));
        let query = Where::status_equals(JobStatus::Cancelled);

        let result = SimpleExecutor::query_jobs_on_backend(query, &backend).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn query_jobs_success_empty() {
        let mut backend = MockBackend::default();
        backend.expect_query().returning(move |_| Ok(vec![]));
        let query = Where::status_equals(JobStatus::Cancelled);

        let result = SimpleExecutor::query_jobs_on_backend(query, &backend).await;

        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn query_jobs_success() {
        let mut backend = MockBackend::default();
        backend.expect_query().returning(move |_| {
            Ok(vec![
                crate::backend::Job::mock_job::<SimpleExecutor>().with_data("Data")
            ])
        });
        let query = Where::status_equals(JobStatus::Cancelled);

        let result = SimpleExecutor::query_jobs_on_backend(query, &backend).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn update_job_error() {
        let job = crate::backend::Job::mock_job::<SimpleExecutor>()
            .with_data("Data")
            .try_into()
            .unwrap();
        let mut backend = MockBackend::default();
        backend
            .expect_update_job()
            .returning(move |_| Err(BackendError::BadState));

        let result = SimpleExecutor::update_job_on_backend(job, &backend).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn update_job_success() {
        let job = crate::backend::Job::mock_job::<SimpleExecutor>()
            .with_data("Data")
            .try_into()
            .unwrap();
        let mut backend = MockBackend::default();
        backend.expect_update_job().returning(move |_| Ok(()));

        let result = SimpleExecutor::update_job_on_backend(job, &backend).await;

        assert!(result.is_ok());
    }

    #[test]
    fn default_timeout_is_none() {
        let job = crate::backend::Job::mock_job::<SimpleExecutor>()
            .with_data("Data")
            .try_into()
            .unwrap();
        assert!(SimpleExecutor::timeout(&job).is_none());
    }
}
