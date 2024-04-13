use std::{hash::Hash, marker::PhantomData, ops::Sub, sync::Arc, time::Duration};

pub mod backend;
pub mod executor;
pub mod job;
pub mod notifier;

use backend::{Backend, BackendError};
use chrono::{TimeDelta, Utc};
use executor::Executor;
use futures::StreamExt;
use job::runner::JobRunner;
use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;
use tokio::{
    sync::{mpsc, OnceCell},
    task::JoinHandle,
};

use crate::job::uniqueness_criteria::UniquenessCriteria;

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

enum WakeMessage {
    Wake,
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

pub struct PrunerConfig {}

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
        let (sender, mut rx) = mpsc::unbounded_channel();

        let handle = tokio::spawn({
            let backend = self.backend.clone();
            async move {
                backend
                    .clone()
                    .subscribe_new_events(E::NAME.into())
                    .await
                    .take_until(rx.recv())
                    .for_each_concurrent(E::MAX_CONCURRENCY, {
                        |message| async {
                            let runner = JobRunner::<B, E>::new(backend.clone());
                            match message {
                                Ok(job) => match job.try_into() {
                                    Ok(job) => runner.execute_job(job).await,
                                    Err(error) => {
                                        tracing::error!(?error, "Failed to decode job: {error}")
                                    }
                                },
                                _ => tracing::warn!("Failed to get from stream"),
                            }
                        }
                    })
                    .await;
                tracing::debug!("Shutting down Rexecutor job runner for {}", E::NAME);
            }
        });

        self.executors.push(ExecutorHandle {
            sender,
            handle: Some(handle),
        });
        self
    }

    pub fn with_cron_executor<E>(mut self, schedule: cron::Schedule, data: E::Data) -> Self
    where
        E: Executor + 'static + Sync + Send,
        E::Data: Send + Serialize + DeserializeOwned + Clone + Hash,
    {
        let (sender, mut rx) = mpsc::unbounded_channel();
        let handle = tokio::spawn({
            let backend = self.backend.clone();
            async move {
                loop {
                    let next = schedule
                        .upcoming(Utc)
                        .next()
                        .expect("No future scheduled time for cron job");
                    let delay = next
                        .sub(Utc::now())
                        .sub(TimeDelta::milliseconds(10))
                        .to_std()
                        .unwrap_or(Duration::ZERO);
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {
                            let criteria = UniquenessCriteria::default()
                                .by_duration(TimeDelta::zero())
                                .by_key(&data)
                                .by_executor();

                            let _ = E::builder()
                                .schedule_at(next)
                                .with_data(data.clone())
                                .unique(criteria)
                                .enqueue_to_backend(&backend)
                                .await
                                .inspect_err(|err| {
                                    tracing::error!(?err, "Failed to enqueue cron job {} with {err}", E::NAME);
                                });
                            let delay = next - Utc::now();
                            if delay > TimeDelta::zero() {
                                tokio::time::sleep(delay.to_std().unwrap()).await;
                            }
                        },
                        _ = rx.recv() => {
                            break;
                        },
                    }
                }
                tracing::debug!("Shutting down cron scheduler for {}", E::NAME);
            }
        });

        self.executors.push(ExecutorHandle {
            sender,
            handle: Some(handle),
        });

        self.with_executor::<E>()
    }

    pub fn with_job_pruner(self, _config: PrunerConfig) -> Self {
        // TODO implement the pruner
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
