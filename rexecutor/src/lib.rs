use std::{hash::Hash, marker::PhantomData, sync::Arc};

pub mod backend;
pub mod backoff;
mod cron_runner;
pub mod executor;
pub mod job;
pub mod prelude;
pub mod pruner;

use backend::{Backend, BackendError};
use cron_runner::CronRunner;
use executor::Executor;
use job::runner::JobRunner;
use pruner::{PrunerConfig, PrunerRunner};
use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;
use tokio::{
    sync::{mpsc, OnceCell},
    task::JoinHandle,
};

trait InternalRexecutorState {}

pub struct GlobalSet;
pub struct GlobalUnset;
impl InternalRexecutorState for GlobalUnset {}
impl InternalRexecutorState for GlobalSet {}

#[derive(Debug)]
#[allow(private_bounds)]
pub struct Rexecutor<B: Backend, State: InternalRexecutorState> {
    executors: Vec<ExecutorHandle>,
    backend: B,
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

#[derive(Debug)]
struct ExecutorHandle {
    sender: mpsc::UnboundedSender<Message>,
    handle: Option<JoinHandle<()>>,
}

impl ExecutorHandle {
    async fn graceful_shutdown(&mut self) -> Result<(), RexecuterError> {
        self.sender
            .send(Message::Terminate)
            .map_err(|_| RexecuterError::GracefulShutdownFailed)?;
        if let Some(handle) = self.handle.take() {
            handle
                .await
                .map_err(|_| RexecuterError::GracefulShutdownFailed)?;
        }
        Ok(())
    }
}

enum Message {
    Terminate,
}

impl<B> Rexecutor<B, GlobalUnset>
where
    B: Backend,
{
    pub fn new(backend: B) -> Self {
        Self {
            executors: Default::default(),
            backend,
            _state: PhantomData,
        }
    }
}

static GLOBAL_BACKEND: OnceCell<Arc<dyn Backend + 'static + Sync + Send>> = OnceCell::const_new();

impl<B> Rexecutor<B, GlobalUnset>
where
    B: Backend + Send + 'static + Sync + Clone,
{
    pub fn set_global_backend(self) -> Result<Rexecutor<B, GlobalSet>, RexecuterError> {
        GLOBAL_BACKEND
            .set(Arc::new(self.backend.clone()))
            .map_err(|err| {
                tracing::error!(%err, "Couldn't set global backend {err}");
                RexecuterError::GlobalBackend
            })?;

        Ok(Rexecutor {
            executors: self.executors,
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
    pub fn with_executor<E>(mut self) -> Self
    where
        E: Executor + 'static + Sync + Send,
        E::Data: Send + DeserializeOwned,
        E::Metadata: Serialize + DeserializeOwned + Send,
    {
        let handle = JobRunner::<B, E>::new(self.backend.clone()).spawn();
        self.executors.push(handle);

        self
    }

    pub fn with_cron_executor<E>(mut self, schedule: cron::Schedule, data: E::Data) -> Self
    where
        E: Executor + 'static + Sync + Send,
        E::Data: Send + Sync + Serialize + DeserializeOwned + Clone + Hash,
        E::Metadata: Serialize + DeserializeOwned + Send + Sync,
    {
        let handle = CronRunner::<B, E>::new(self.backend.clone(), schedule, data).spawn();
        self.executors.push(handle);

        self.with_executor::<E>()
    }

    pub fn with_job_pruner(mut self, config: PrunerConfig) -> Self {
        let handle = PrunerRunner::new(self.backend.clone(), config).spawn();
        self.executors.push(handle);
        self
    }

    pub async fn graceful_shutdown(mut self) -> Result<Vec<()>, RexecuterError> {
        tracing::debug!("Shutting down Rexecutor tasks");
        futures::future::join_all(
            self.executors
                .iter_mut()
                .map(ExecutorHandle::graceful_shutdown),
        )
        .await
        .into_iter()
        .collect()
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
    use std::time::Duration;

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
        let sender = backend.expect_subscribe_to_new_events_with_stream();
        let backend = Arc::new(backend);

        let handle = Rexecutor::<_, _>::new(backend.clone()).with_executor::<MockReturnExecutor>();

        tokio::task::yield_now().await;

        sender.send(Err(BackendError::BadStateError)).unwrap();

        tokio::task::yield_now().await;

        handle.graceful_shutdown().await.unwrap();
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
            .returning(|_| Err(BackendError::BadStateError));

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
            .returning(|_, _, _| Err(BackendError::BadStateError));

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
            .returning(|_, _| Err(BackendError::BadStateError));

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
            .returning(|_, _| Err(BackendError::BadStateError));

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
            .returning(|_, _| Err(BackendError::BadStateError));

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
            .returning(|_, _| Err(BackendError::BadStateError));

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
        let _sender = backend.expect_subscribe_to_new_events_with_stream();
        backend.expect_enqueue().returning(|_| Ok(0.into()));
        let backend = Arc::new(backend);

        let handle = Rexecutor::new(backend.clone())
            .with_cron_executor::<MockReturnExecutor>(every_second, MockExecutionResult::Done);

        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_secs(1)).await;

        handle.graceful_shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cron_job_error() {
        let every_second = cron::Schedule::try_from("* * * * * *").unwrap();
        let mut backend = MockBackend::default();
        let _sender = backend.expect_subscribe_to_new_events_with_stream();
        backend
            .expect_enqueue()
            .returning(|_| Err(BackendError::BadStateError));
        let backend = Arc::new(backend);

        let handle = Rexecutor::new(backend.clone())
            .with_cron_executor::<MockReturnExecutor>(every_second, MockExecutionResult::Done);

        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_secs(1)).await;

        handle.graceful_shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn with_pruner() {
        let schedule = cron::Schedule::try_from("* * * * * *").unwrap();
        let mut backend = MockBackend::default();
        backend.expect_prune_jobs().returning(|_| Ok(()));
        let backend = Arc::new(backend);

        let pruner = PrunerConfig::new(schedule)
            .with_pruner(Pruner::max_length(5, JobStatus::Complete).only::<MockReturnExecutor>());

        let handle = Rexecutor::new(backend.clone()).with_job_pruner(pruner);

        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_secs(1)).await;

        handle.graceful_shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn with_pruner_error() {
        let schedule = cron::Schedule::try_from("* * * * * *").unwrap();
        let mut backend = MockBackend::default();
        backend
            .expect_prune_jobs()
            .returning(|_| Err(BackendError::BadStateError));
        let backend = Arc::new(backend);

        let pruner = PrunerConfig::new(schedule)
            .with_pruner(Pruner::max_length(5, JobStatus::Complete).only::<MockReturnExecutor>());

        let handle = Rexecutor::new(backend.clone()).with_job_pruner(pruner);

        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_secs(1)).await;

        handle.graceful_shutdown().await.unwrap();
    }

    async fn run_job(mut backend: MockBackend, job: Job) {
        let sender = backend.expect_subscribe_to_new_events_with_stream();
        let backend = Arc::new(backend);
        let handle = Rexecutor::new(backend.clone()).with_executor::<MockReturnExecutor>();

        tokio::task::yield_now().await;
        sender.send(Ok(job)).unwrap();
        tokio::task::yield_now().await;

        handle.graceful_shutdown().await.unwrap();
    }
}
