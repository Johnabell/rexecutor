use std::{collections::HashMap, sync::Arc};

pub mod backend;
pub mod executor;
pub mod job;
pub mod notifier;

use backend::{Backend, BackendError};
use executor::Executor;
use futures::StreamExt;
use job::runner::JobRunner;
use serde::de::DeserializeOwned;
use thiserror::Error;
use tokio::{
    sync::{mpsc, OnceCell},
    task::JoinHandle,
};

#[derive(Debug)]
pub struct Rexecuter<B: Backend> {
    executors: HashMap<&'static str, ExecutorHandle>,
    backend: B,
}

impl<B> Default for Rexecuter<B>
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

enum WakeMessage {
    Wake,
}

impl<B> Rexecuter<B>
where
    B: Backend,
{
    pub fn new(backend: B) -> Self {
        Self {
            executors: Default::default(),
            backend,
        }
    }
}

static GLOBAL_BACKEND: OnceCell<Arc<dyn Backend + 'static + Sync + Send>> = OnceCell::const_new();

pub struct PrunerConfig {}

impl<B> Rexecuter<B>
where
    B: Backend + Send + 'static + Sync + Clone,
{
    pub fn with_executor<E>(mut self) -> Self
    where
        E: Executor + 'static + Sync + Send,
        E::Data: Send + DeserializeOwned,
    {
        let (sender, mut rx) = mpsc::unbounded_channel();

        let handle = tokio::spawn({
            let backend = self.backend.clone();
            async move {
                let stream = backend.clone().subscribe_new_events(E::NAME.into()).await;
                let runner = JobRunner::<B, E>::new(backend);
                tokio::pin!(stream);
                loop {
                    tokio::select! {
                        message = stream.next() => {
                            match message {
                                Some(Ok(job)) => match job.try_into() {
                                    Ok(job) => runner.execute_job(job).await,
                                    Err(error) => tracing::error!(?error, "Failed to decode job: {error}"),
                                },
                                _ => tracing::warn!("Failed to get from stream")
                            }
                        },
                        _ = rx.recv() => {
                            break;
                        }
                    }
                }
                tracing::debug!("Shutting down Rexecutor job runner for {}", E::NAME);
            }
        });

        self.executors.insert(
            E::NAME,
            ExecutorHandle {
                sender,
                handle: Some(handle),
            },
        );
        self
    }

    // TODO: make this only possible to call once
    pub fn set_global_backend(self) -> Result<Self, RexecuterError> {
        GLOBAL_BACKEND
            .set(Arc::new(self.backend.clone()))
            .map_err(|err| {
                tracing::error!(?err, "Couldn't set global backend {err}");
                RexecuterError::GlobalBackend
            })?;

        Ok(self)
    }

    pub fn with_job_pruner(self, _config: PrunerConfig) -> Self {
        // TODO implement the pruner
        self
    }

    pub async fn graceful_shutdown(mut self) -> Result<Vec<()>, RexecuterError> {
        tracing::debug!("Shutting down Rexecutor tasks");
        futures::future::join_all(
            self.executors
                .values_mut()
                .map(ExecutorHandle::graceful_shutdown),
        )
        .await
        .into_iter()
        .collect()
    }
}

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{backend::test::MockBackend, executor::test::SimpleExecutor};

    #[tokio::test]
    async fn setup() {
        let _handle = Rexecuter::<MockBackend>::default().with_executor::<SimpleExecutor>();
    }
}
