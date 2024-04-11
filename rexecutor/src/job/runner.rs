use std::{marker::PhantomData, time::Duration};

use chrono::{TimeDelta, Utc};
use serde::de::DeserializeOwned;
use tokio::task::JoinError;
use tracing::{instrument, Instrument};

use crate::{
    backend::Backend,
    executor::{CancellationReason, ExecutionError, ExecutionResult, Executor},
};

use super::{ErrorType, Job, JobId};

pub(crate) struct JobRunner<B, E>
where
    B: Backend,
    E: Executor + 'static,
    E::Data: Send + DeserializeOwned,
{
    backend: B,
    _executor: PhantomData<E>,
}

impl<B, E> JobRunner<B, E>
where
    B: Backend + Send + 'static + Sync,
    E: Executor + 'static + Sync,
    E::Data: Send + DeserializeOwned,
{
    pub(crate) fn new(backend: B) -> Self {
        Self {
            backend,
            _executor: PhantomData,
        }
    }

    #[instrument(skip(self, job), fields(job_id))]
    pub async fn execute_job(&self, job: Job<E::Data>) {
        let is_final_attempt = job.is_final_attempt();
        let job_id = job.id;
        let delay = E::backoff(&job);
        let timeout = E::timeout(&job);

        tracing::Span::current().record("job_id", &tracing::field::debug(&job_id));

        let fut = tokio::time::timeout(timeout, E::execute(job)).in_current_span();
        let result = if E::BLOCKING {
            tracing::debug!(%job_id, "Executing blocking job {job_id}");
            tokio::task::spawn_blocking(|| futures::executor::block_on(fut))
        } else {
            tracing::debug!(%job_id, "Executing job {job_id}");
            tokio::spawn(fut)
        };

        match result.await {
            Ok(Ok(ExecutionResult::Done)) => self.handle_job_complete(job_id).await,
            Ok(Ok(ExecutionResult::Cancelled { reason })) => {
                self.handle_job_cancelled(job_id, reason).await
            }
            Ok(Ok(ExecutionResult::Snooze { .. })) => {
                todo!("Snooze job")
            }
            Ok(Ok(ExecutionResult::Error { error })) => {
                self.handle_job_error(is_final_attempt, job_id, delay, error)
                    .await
            }
            Ok(Err(_elaped)) => {
                self.handle_job_error(is_final_attempt, job_id, delay, timeout)
                    .await
            }
            Err(error) => {
                self.handle_job_error(is_final_attempt, job_id, delay, error)
                    .await
            }
        }
    }

    async fn handle_job_complete(&self, job_id: JobId) {
        tracing::info!(%job_id, "Job complete {job_id}");
        let _ = self
            .backend
            .mark_job_complete(job_id)
            .await
            .inspect_err(|err| {
                tracing::error!(
                    ?err,
                    %job_id,
                    "Failed to mark job {job_id} as complete, error: {err:?}",
                )
            });
    }

    async fn handle_job_cancelled(
        &self,
        job_id: JobId,
        error: impl Into<crate::backend::ExecutionError>,
    ) {
        let error = error.into();
        tracing::info!(
            %job_id,
            ?error,
            "Job {job_id} failed and will be discarded: error type: {:?}, message: {}",
            error.error_type,
            error.message
        );
        let _ = self
            .backend
            .mark_job_cancelled(job_id, error)
            .await
            .inspect_err(|err| {
                tracing::error!(
                    ?err,
                    %job_id,
                    "Failed to mark job {job_id} as discarded, error: {err:?}",
                )
            });
    }

    async fn handle_job_error(
        &self,
        is_final_attempt: bool,
        job_id: JobId,
        delay: TimeDelta,
        error: impl Into<crate::backend::ExecutionError>,
    ) {
        let error = error.into();
        if is_final_attempt {
            tracing::error!(
                %job_id,
                ?error,
                "Job {job_id} failed and will be discarded: error type: {:?}, message: {}",
                error.error_type,
                error.message
            );
            let _ = self
                .backend
                .mark_job_discarded(job_id, error)
                .await
                .inspect_err(|err| {
                    tracing::error!(
                        ?err,
                        %job_id,
                        "Failed to mark job {job_id} as discarded, error: {err:?}",
                    )
                });
        } else {
            let next_scheduled_at = Utc::now() + delay;
            tracing::warn!(
                %job_id,
                ?error,
                "Job {job_id} failed and will be retried at {next_scheduled_at}: error type: {:?}, message: {}",
                error.error_type,
                error.message
            );
            let _ = self
                .backend
                .mark_job_retryable(job_id, Utc::now() + delay, error)
                .await
                .inspect_err(|err| {
                    tracing::error!(
                        ?err,
                        %job_id,
                        "Failed to mark job {job_id} as retryable, error: {err:?}",
                    )
                });
        }
    }
}

impl From<JoinError> for crate::backend::ExecutionError {
    fn from(value: JoinError) -> Self {
        let msg = value.to_string();
        let message = match value.try_into_panic() {
            Ok(panic) => panic
                .downcast_ref::<&str>()
                .map(ToString::to_string)
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or(msg),
            Err(_) => msg,
        };
        Self {
            error_type: ErrorType::Panic,
            message,
        }
    }
}

impl From<Box<dyn ExecutionError>> for crate::backend::ExecutionError {
    fn from(value: Box<dyn ExecutionError>) -> Self {
        Self {
            error_type: ErrorType::Other(value.error_type().to_string()),
            message: value.to_string(),
        }
    }
}

impl From<Box<dyn CancellationReason>> for crate::backend::ExecutionError {
    fn from(value: Box<dyn CancellationReason>) -> Self {
        Self {
            error_type: ErrorType::Cancelled,
            message: value.to_string(),
        }
    }
}

impl From<Duration> for crate::backend::ExecutionError {
    fn from(value: Duration) -> Self {
        Self {
            error_type: ErrorType::Timeout,
            message: format!("Job failed to complete within timeout: {value:?}"),
        }
    }
}
