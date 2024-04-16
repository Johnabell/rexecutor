use std::{hash::Hash, marker::PhantomData, sync::Arc};

pub mod backend;
pub mod backoff;
mod cron_runner;
pub mod executor;
pub mod job;
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
pub struct Rexecuter<B: Backend, State: InternalRexecutorState> {
    executors: Vec<ExecutorHandle>,
    backend: B,
    _state: PhantomData<State>,
}

impl<B> Default for Rexecuter<B, GlobalUnset>
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

impl<B> Rexecuter<B, GlobalUnset>
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

impl<B> Rexecuter<B, GlobalUnset>
where
    B: Backend + Send + 'static + Sync + Clone,
{
    pub fn set_global_backend(self) -> Result<Rexecuter<B, GlobalSet>, RexecuterError> {
        GLOBAL_BACKEND
            .set(Arc::new(self.backend.clone()))
            .map_err(|err| {
                tracing::error!(?err, "Couldn't set global backend {err}");
                RexecuterError::GlobalBackend
            })?;

        Ok(Rexecuter {
            executors: self.executors,
            backend: self.backend,
            _state: PhantomData,
        })
    }
}

#[allow(private_bounds)]
impl<B, State> Rexecuter<B, State>
where
    B: Backend + Send + 'static + Sync + Clone,
    State: InternalRexecutorState,
{
    pub fn with_executor<E>(mut self) -> Self
    where
        E: Executor + 'static + Sync + Send,
        E::Data: Send + DeserializeOwned,
    {
        let handle = JobRunner::<B, E>::new(self.backend.clone()).spawn();
        self.executors.push(handle);

        self
    }

    pub fn with_cron_executor<E>(mut self, schedule: cron::Schedule, data: E::Data) -> Self
    where
        E: Executor + 'static + Sync + Send,
        E::Data: Send + Sync + Serialize + DeserializeOwned + Clone + Hash,
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
    use super::*;
    use crate::{backend::test::MockBackend, executor::test::SimpleExecutor};

    #[tokio::test]
    async fn setup() {
        let _handle = Rexecuter::<MockBackend, _>::default().with_executor::<SimpleExecutor>();
    }
}
