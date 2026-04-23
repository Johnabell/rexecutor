use std::ops::Sub;

use chrono::{DateTime, Utc};
use rexecutor::{backend::BackendError, executor::ExecutorIdentifier};

use crate::RexecutorPgBackend;

pub struct ReadyJobStream {
    pub(crate) backend: RexecutorPgBackend,
    pub(crate) executor_identifier: ExecutorIdentifier,
    pub(crate) receiver: tokio::sync::mpsc::UnboundedReceiver<DateTime<Utc>>,
}

impl ReadyJobStream {
    const DEFAULT_DELAY: std::time::Duration = std::time::Duration::from_secs(30);
    const DELTA: std::time::Duration = std::time::Duration::from_millis(15);

    pub async fn next(&mut self) -> Result<rexecutor::backend::Job, BackendError> {
        loop {
            let delay = match self
                .backend
                .next_available_job_scheduled_at_for_executor(self.executor_identifier.as_str())
                .await
                .map_err(|_| BackendError::BadState)?
            {
                Some(timestamp) => timestamp
                    .sub(Utc::now())
                    .to_std()
                    .unwrap_or(Self::DELTA)
                    .min(Self::DEFAULT_DELAY),
                _ => Self::DEFAULT_DELAY,
            };
            if delay <= Self::DELTA
                && let Some(job) = self
                    .backend
                    .load_job_mark_as_executing_for_executor(self.executor_identifier.as_str())
                    .await
                    .map_err(|_| BackendError::BadState)?
            {
                return job.try_into();
            }
            tokio::select! {
                _ = self.receiver.recv() => { },
                _ = tokio::time::sleep(delay) => { },

            }
        }
    }
}
