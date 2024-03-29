use std::{collections::HashMap, ops::Deref};

pub mod backend;
pub mod executor;
pub mod job;

use backend::{Backend, BackendError};
use executor::Executor;
use job::JobId;
use thiserror::Error;
use tokio::{
    sync::{
        mpsc,
        oneshot::{self, Sender},
    },
    task::JoinHandle,
};

#[derive(Default, Debug)]
pub struct Rexecuter<B: Backend> {
    executors: HashMap<&'static str, ExecutorHandle>,
    receiver: Option<oneshot::Receiver<()>>,
    backend: B,
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

impl<B> Rexecuter<B>
where
    B: Backend + Send + 'static,
{
    const DEFAULT_DELAY: std::time::Duration = std::time::Duration::from_secs(5);
    pub fn with_executor<E: Executor>(mut self) -> Self {
        let (sender, mut rx) = mpsc::unbounded_channel();

        let handle = tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                match message {
                    Message::JobWaiting(_id) => {
                        //read job
                        //decode
                        //execute
                    }
                    Message::Terminate => break,
                }
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

    async fn run(mut self) {
        loop {
            let _ = self
                .tick()
                .await
                .map_err(|error| tracing::error!(?error, "Failed to get jobs"));
            tokio::select! {
                _ = self.receiver.as_mut().unwrap() => {
                    let _ = self.shutdown().await;
                    break;
                }
                _ = tokio::time::sleep(Self::DEFAULT_DELAY) => {
                }

            }
        }
    }

    async fn shutdown(mut self) -> Result<Vec<()>, RexecuterError> {
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

#[derive(Debug, Eq, PartialEq, Clone, Error)]
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
            .spawn();
    }
}
