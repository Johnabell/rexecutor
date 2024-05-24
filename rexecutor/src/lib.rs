use std::{hash::Hash, marker::PhantomData};

pub mod backend;
pub mod backoff;
mod cron_runner;
pub mod executor;
pub mod global_backend;
pub mod job;
pub mod prelude;
pub mod pruner;

use backend::{Backend, BackendError};
use chrono::{TimeZone, Utc};
use cron_runner::CronRunner;
use executor::Executor;
use global_backend::GlobalBackend;
use job::runner::JobRunner;
use pruner::{runner::PrunerRunner, PrunerConfig};
use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

trait InternalRexecutorState {}

#[doc(hidden)]
pub struct GlobalSet;
#[doc(hidden)]
pub struct GlobalUnset;
impl InternalRexecutorState for GlobalUnset {}
impl InternalRexecutorState for GlobalSet {}

#[derive(Debug)]
#[allow(private_bounds)]
pub struct Rexecutor<B: Backend, State: InternalRexecutorState> {
    backend: B,
    cancellation_token: CancellationToken,
    _state: PhantomData<State>,
}

impl<B> Default for Rexecutor<B, GlobalUnset>
where
    B: Backend + Default,
{
    fn default() -> Self {
        Self::new(Default::default())
    }
}

impl<B> Rexecutor<B, GlobalUnset>
where
    B: Backend,
{
    pub fn new(backend: B) -> Self {
        Self {
            cancellation_token: Default::default(),
            backend,
            _state: PhantomData,
        }
    }
}

#[allow(dead_code)]
pub struct DropGuard(tokio_util::sync::DropGuard);

impl<B> Rexecutor<B, GlobalUnset>
where
    B: Backend + Send + 'static + Sync + Clone,
{
    pub fn set_global_backend(self) -> Result<Rexecutor<B, GlobalSet>, RexecuterError> {
        GlobalBackend::set(self.backend.clone())?;

        Ok(Rexecutor {
            cancellation_token: self.cancellation_token,
            backend: self.backend,
            _state: PhantomData,
        })
    }
}

#[allow(private_bounds)]
impl<B, State> Rexecutor<B, State>
where
    B: Backend + Send + 'static + Sync + Clone,
    State: InternalRexecutorState,
{
    pub fn with_executor<E>(self) -> Self
    where
        E: Executor + 'static + Sync + Send,
        E::Data: Send + DeserializeOwned,
        E::Metadata: Serialize + DeserializeOwned + Send,
    {
        JobRunner::<B, E>::new(self.backend.clone()).spawn(self.cancellation_token.clone());
        self
    }

    /// Setup a cron job to run on the given schedule with the given data.
    ///
    /// Note this will run the schedule according to UTC time on a schedule in another timezone use
    /// [`Rexecutor::with_cron_executor_for_timezone`].
    ///
    /// # Example
    ///
    /// To setup a cron jobs to run every day at midnight you can use the following code.
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    /// # use rexecutor::backend::{Backend, memory::InMemoryBackend};
    /// struct CronJob;
    /// #[async_trait::async_trait]
    /// impl Executor for CronJob {
    ///     type Data = String;
    ///     type Metadata = ();
    ///     const NAME: &'static str = "cron_job";
    ///     const MAX_ATTEMPTS: u16 = 1;
    ///     async fn execute(job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
    ///         /// Do something important
    ///         ExecutionResult::Done
    ///     }
    /// }
    /// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
    /// let schedule = cron::Schedule::try_from("0 0 0 * * *").unwrap();
    ///
    /// let backend = InMemoryBackend::new();
    /// Rexecutor::new(backend).with_cron_executor::<CronJob>(schedule, "important data".to_owned());
    /// # });
    /// ```
    pub fn with_cron_executor<E>(self, schedule: cron::Schedule, data: E::Data) -> Self
    where
        E: Executor + 'static + Sync + Send,
        E::Data: Send + Sync + Serialize + DeserializeOwned + Clone + Hash,
        E::Metadata: Serialize + DeserializeOwned + Send + Sync,
    {
        self.with_cron_executor_for_timezone::<E, _>(schedule, data, Utc)
    }

    /// Setup a cron job to run on the given schedule with the given data in the given timezome.
    pub fn with_cron_executor_for_timezone<E, Z>(
        self,
        schedule: cron::Schedule,
        data: E::Data,
        timezone: Z,
    ) -> Self
    where
        E: Executor + 'static + Sync + Send,
        E::Data: Send + Sync + Serialize + DeserializeOwned + Clone + Hash,
        E::Metadata: Serialize + DeserializeOwned + Send + Sync,
        Z: TimeZone + Send + 'static,
    {
        CronRunner::<B, E>::new(self.backend.clone(), schedule, data)
            .spawn(timezone, self.cancellation_token.clone());

        self.with_executor::<E>()
    }

    pub fn with_job_pruner(self, config: PrunerConfig) -> Self {
        PrunerRunner::new(self.backend.clone(), config).spawn(self.cancellation_token.clone());
        self
    }

    pub fn graceful_shutdown(self) {
        tracing::debug!("Shutting down Rexecutor tasks");
        self.cancellation_token.cancel();
    }

    pub fn drop_guard(self) -> DropGuard {
        DropGuard(self.cancellation_token.drop_guard())
    }
}

// TODO: split errors
#[derive(Debug, Error)]
pub enum RexecuterError {
    #[error("Failed to gracefully shut down")]
    GracefulShutdownFailed,
    #[error("Error communicating with the backend")]
    BackendError(#[from] BackendError),
    #[error("Error setting global backend")]
    GlobalBackend,
    #[error("Error encoding or decoding value")]
    EncodeError(#[from] serde_json::Error),
    #[error("Error reading cron expression")]
    CronExpressionError(#[from] cron::error::Error),
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use super::*;
    use crate::{
        backend::{Job, MockBackend},
        executor::test::{MockError, MockExecutionResult, MockReturnExecutor},
        job::JobStatus,
        pruner::Pruner,
    };

    #[tokio::test]
    async fn run_job_error_in_stream() {
        let mut backend = MockBackend::default();
        let sender = backend.expect_subscribe_to_ready_jobs_with_stream();
        let backend = Arc::new(backend);

        let _guard = Rexecutor::<_, _>::new(backend.clone())
            .with_executor::<MockReturnExecutor>()
            .drop_guard();

        tokio::task::yield_now().await;

        sender.send(Err(BackendError::BadState)).unwrap();

        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn run_job_success() {
        let mut backend = MockBackend::default();
        backend.expect_mark_job_complete().returning(|_| Ok(()));

        let job = Job::mock_job::<MockReturnExecutor>().with_data(MockExecutionResult::Done);

        run_job(backend, job).await;
    }

    #[tokio::test]
    async fn run_job_success_error_marking_success() {
        let mut backend = MockBackend::default();
        backend
            .expect_mark_job_complete()
            .returning(|_| Err(BackendError::BadState));

        let job = Job::mock_job::<MockReturnExecutor>().with_data(MockExecutionResult::Done);

        run_job(backend, job).await;
    }

    #[tokio::test]
    async fn run_job_retryable() {
        let mut backend = MockBackend::default();
        backend
            .expect_mark_job_retryable()
            .returning(|_, _, _| Ok(()));

        let job = Job::mock_job::<MockReturnExecutor>().with_data(MockExecutionResult::Error {
            error: MockError("oh no".to_owned()),
        });

        run_job(backend, job).await;
    }

    #[tokio::test]
    async fn run_job_retryable_error_marking_retryable() {
        let mut backend = MockBackend::default();
        backend
            .expect_mark_job_retryable()
            .returning(|_, _, _| Err(BackendError::BadState));

        let job = Job::mock_job::<MockReturnExecutor>().with_data(MockExecutionResult::Error {
            error: MockError("oh no".to_owned()),
        });

        run_job(backend, job).await;
    }

    #[tokio::test]
    async fn run_job_retryable_timeout() {
        let mut backend = MockBackend::default();
        backend
            .expect_mark_job_retryable()
            .returning(|_, _, _| Ok(()));

        let job = Job::mock_job::<MockReturnExecutor>().with_data(MockExecutionResult::Timeout);

        run_job(backend, job).await;
    }

    #[tokio::test]
    async fn run_job_discarded() {
        let mut backend = MockBackend::default();
        backend.expect_mark_job_discarded().returning(|_, _| Ok(()));

        let job = Job::mock_job::<MockReturnExecutor>()
            .with_data(MockExecutionResult::Error {
                error: MockError("oh no".to_owned()),
            })
            .with_max_attempts(1)
            .with_attempt(1);

        run_job(backend, job).await;
    }

    #[tokio::test]
    async fn run_job_discarded_error_marking_job_discarded() {
        let mut backend = MockBackend::default();
        backend
            .expect_mark_job_discarded()
            .returning(|_, _| Err(BackendError::BadState));

        let job = Job::mock_job::<MockReturnExecutor>()
            .with_data(MockExecutionResult::Error {
                error: MockError("oh no".to_owned()),
            })
            .with_max_attempts(1)
            .with_attempt(1);

        run_job(backend, job).await;
    }

    #[tokio::test]
    async fn run_job_discarded_panic() {
        let mut backend = MockBackend::default();
        backend.expect_mark_job_discarded().returning(|_, _| Ok(()));

        let job = Job::mock_job::<MockReturnExecutor>()
            .with_data(MockExecutionResult::Panic)
            .with_max_attempts(1)
            .with_attempt(1);

        run_job(backend, job).await;
    }

    #[tokio::test]
    async fn run_job_discarded_panic_error_marking_job_discarded() {
        let mut backend = MockBackend::default();
        backend
            .expect_mark_job_discarded()
            .returning(|_, _| Err(BackendError::BadState));

        let job = Job::mock_job::<MockReturnExecutor>()
            .with_data(MockExecutionResult::Panic)
            .with_max_attempts(1)
            .with_attempt(1);

        run_job(backend, job).await;
    }

    #[tokio::test]
    async fn run_job_snoozed() {
        let mut backend = MockBackend::default();
        backend.expect_mark_job_snoozed().returning(|_, _| Ok(()));

        let job = Job::mock_job::<MockReturnExecutor>()
            .with_data(MockExecutionResult::Snooze {
                delay: Duration::from_secs(10),
            })
            .with_max_attempts(1)
            .with_attempt(1);

        run_job(backend, job).await;
    }

    #[tokio::test]
    async fn run_job_snoozed_error_marking_job_snoozed() {
        let mut backend = MockBackend::default();
        backend
            .expect_mark_job_snoozed()
            .returning(|_, _| Err(BackendError::BadState));

        let job = Job::mock_job::<MockReturnExecutor>()
            .with_data(MockExecutionResult::Snooze {
                delay: Duration::from_secs(10),
            })
            .with_max_attempts(1)
            .with_attempt(1);

        run_job(backend, job).await;
    }

    #[tokio::test]
    async fn run_job_cancelled() {
        let mut backend = MockBackend::default();
        backend.expect_mark_job_cancelled().returning(|_, _| Ok(()));

        let job = Job::mock_job::<MockReturnExecutor>()
            .with_data(MockExecutionResult::Cancelled {
                reason: "No need anymore".to_owned(),
            })
            .with_max_attempts(1)
            .with_attempt(1);

        run_job(backend, job).await;
    }

    #[tokio::test]
    async fn run_job_cancelled_error_marking_job_cancelled() {
        let mut backend = MockBackend::default();
        backend
            .expect_mark_job_cancelled()
            .returning(|_, _| Err(BackendError::BadState));

        let job = Job::mock_job::<MockReturnExecutor>()
            .with_data(MockExecutionResult::Cancelled {
                reason: "No need anymore".to_owned(),
            })
            .with_max_attempts(1)
            .with_attempt(1);

        run_job(backend, job).await;
    }

    #[tokio::test]
    async fn cron_job() {
        let every_second = cron::Schedule::try_from("* * * * * *").unwrap();
        let mut backend = MockBackend::default();
        let _sender = backend.expect_subscribe_to_ready_jobs_with_stream();
        backend.expect_enqueue().returning(|_| Ok(0.into()));
        let backend = Arc::new(backend);

        let _guard = Rexecutor::new(backend.clone())
            .with_cron_executor::<MockReturnExecutor>(every_second, MockExecutionResult::Done)
            .drop_guard();

        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn cron_job_error() {
        let every_second = cron::Schedule::try_from("* * * * * *").unwrap();
        let mut backend = MockBackend::default();
        let _sender = backend.expect_subscribe_to_ready_jobs_with_stream();
        backend
            .expect_enqueue()
            .returning(|_| Err(BackendError::BadState));
        let backend = Arc::new(backend);

        let _guard = Rexecutor::new(backend.clone())
            .with_cron_executor::<MockReturnExecutor>(every_second, MockExecutionResult::Done)
            .drop_guard();

        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn with_pruner() {
        let schedule = cron::Schedule::try_from("* * * * * *").unwrap();
        let mut backend = MockBackend::default();
        backend.expect_prune_jobs().returning(|_| Ok(()));
        let backend = Arc::new(backend);

        let pruner = PrunerConfig::new(schedule)
            .with_pruner(Pruner::max_length(5, JobStatus::Complete).only::<MockReturnExecutor>());

        let _guard = Rexecutor::new(backend.clone())
            .with_job_pruner(pruner)
            .drop_guard();

        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn with_pruner_error() {
        let schedule = cron::Schedule::try_from("* * * * * *").unwrap();
        let mut backend = MockBackend::default();
        backend
            .expect_prune_jobs()
            .returning(|_| Err(BackendError::BadState));
        let backend = Arc::new(backend);

        let pruner = PrunerConfig::new(schedule)
            .with_pruner(Pruner::max_length(5, JobStatus::Complete).only::<MockReturnExecutor>());

        let _guard = Rexecutor::new(backend.clone())
            .with_job_pruner(pruner)
            .drop_guard();

        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    async fn run_job(mut backend: MockBackend, job: Job) {
        let sender = backend.expect_subscribe_to_ready_jobs_with_stream();
        let backend = Arc::new(backend);
        let _guard = Rexecutor::new(backend.clone())
            .with_executor::<MockReturnExecutor>()
            .drop_guard();

        tokio::task::yield_now().await;
        sender.send(Ok(job)).unwrap();
        tokio::task::yield_now().await;
    }
}
