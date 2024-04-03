use std::{
    any::TypeId,
    collections::HashMap,
    ops::{Deref, Sub},
};

pub mod backend;
pub mod executor;
pub mod job;

use backend::{Backend, BackendError};
use chrono::Utc;
use executor::Executor;
use job::{runner::JobRunner, JobId};
use serde::de::DeserializeOwned;
use thiserror::Error;
use tokio::{
    sync::{
        mpsc,
        oneshot::{self, Sender},
        OnceCell, RwLock,
    },
    task::JoinHandle,
};

#[derive(Debug)]
pub struct Rexecuter<B: Backend> {
    executors: HashMap<&'static str, ExecutorHandle>,
    receiver: Option<oneshot::Receiver<()>>,
    waker_receiver: mpsc::UnboundedReceiver<WakeMessage>,
    waker_sender: mpsc::UnboundedSender<WakeMessage>,
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
    JobWaiting(JobId),
    Terminate,
}

enum WakeMessage {
    Wake,
}

static EXECUTORS: OnceCell<RwLock<HashMap<TypeId, mpsc::UnboundedSender<WakeMessage>>>> =
    OnceCell::const_new();

impl<B> Rexecuter<B>
where
    B: Backend,
{
    const DEFAULT_DELAY: std::time::Duration = std::time::Duration::from_secs(5);

    pub fn new(backend: B) -> Self {
        let (waker_sender, waker_receiver) = mpsc::unbounded_channel();
        Self {
            executors: Default::default(),
            receiver: None,
            waker_sender,
            waker_receiver,
            backend,
        }
    }
}

pub struct PrunerConfig {}

impl<B> Rexecuter<B>
where
    B: Backend + Send + 'static + Sync,
{
    pub async fn with_executor<E>(mut self) -> Self
    where
        E: Executor + 'static + Sync + Send,
        E::Data: Send + DeserializeOwned,
    {
        EXECUTORS
            .get_or_init(|| async { Default::default() })
            .await
            .write()
            .await
            .insert(TypeId::of::<E>(), self.waker_sender.clone());

        let (sender, mut rx) = mpsc::unbounded_channel();

        let handle = tokio::spawn({
            let backend = self.backend.clone();
            async move {
                let runner = JobRunner::<B, E>::new(backend);
                while let Some(message) = rx.recv().await {
                    match message {
                        Message::JobWaiting(id) => runner.run(id).await,
                        Message::Terminate => break,
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
    pub fn spawn(mut self) -> RexecuterHandle {
        let (sender, receiver) = oneshot::channel();
        self.receiver = Some(receiver);

        let handle = tokio::spawn(async move { self.run().await });

        RexecuterHandle {
            sender: Some(sender),
            handle: Some(handle),
        }
    }

    pub fn with_job_pruner(self, _config: PrunerConfig) -> Self {
        // TODO implement this pruner
        self
    }

    async fn run(mut self) {
        loop {
            let _ = self
                .tick()
                .await
                .inspect_err(|error| tracing::error!(?error, "Failed to get jobs"));
            // Sleep till next tick
            let delay = match self
                .backend
                .next_job_scheduled_at()
                .await
                .inspect_err(|error| tracing::error!(?error, "Failed to get next scheduled at"))
            {
                Ok(Some(time)) => time
                    .sub(Utc::now())
                    .to_std()
                    .unwrap_or(std::time::Duration::ZERO)
                    .min(Self::DEFAULT_DELAY),
                _ => Self::DEFAULT_DELAY,
            };
            tokio::select! {
                _ = self.receiver.as_mut().unwrap() => {
                    let _ = self.shutdown().await;
                    break;
                },
                _ = self.waker_receiver.recv() => { },
                _ = tokio::time::sleep(delay) => { },

            }
        }
    }

    async fn shutdown(mut self) -> Result<Vec<()>, RexecuterError> {
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

    async fn tick(&mut self) -> Result<(), BackendError> {
        self.backend
            .ready_jobs()
            .await?
            .into_iter()
            .for_each(|job| {
                match self.executors.get(job.executor.deref()) {
                    // TODO decide how to handle errors here
                    Some(handle) => handle.sender.send(Message::JobWaiting(job.id)).unwrap(),
                    None => tracing::warn!(?job.executor, "No executor found for job {:?}", job.id),
                }
            });
        Ok(())
    }
}

pub(crate) fn wake_rexecutor<E: Executor + 'static>() {
    EXECUTORS.get().iter().for_each(|inner| {
        tokio::spawn(async {
            match inner.read().await.get(&TypeId::of::<E>()) {
                Some(waker) => {
                    if let Err(err) = waker.send(WakeMessage::Wake) {
                        tracing::error!(?err, "Failed to send executor wake message")
                    }
                }
                None => tracing::error!("No executor running for {:?}", TypeId::of::<E>()),
            }
        });
    });
}
pub struct RexecuterHandle {
    sender: Option<Sender<()>>,
    handle: Option<JoinHandle<()>>,
}

impl RexecuterHandle {
    pub async fn graceful_shutdown(&mut self) -> Result<(), RexecuterError> {
        if let Some(sender) = self.sender.take() {
            sender
                .send(())
                .map_err(|_| RexecuterError::GracefulShutdownFailed)?;
        }
        if let Some(handle) = self.handle.take() {
            handle
                .await
                .map_err(|_| RexecuterError::GracefulShutdownFailed)?;
        }
        Ok(())
    }
}
// Do we want this
impl Drop for RexecuterHandle {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(());
        }
    }
}

#[derive(Debug, Error)]
pub enum RexecuterError {
    #[error("Failed to gracefully shut down")]
    GracefulShutdownFailed,
    #[error("Error communicating with the backend")]
    BackendError(#[from] BackendError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{backend::test::MockBackend, executor::test::SimpleExecutor};

    #[tokio::test]
    async fn setup() {
        let _handle = Rexecuter::<MockBackend>::default()
            .with_executor::<SimpleExecutor>()
            .await
            .spawn();
    }
}
