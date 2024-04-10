use std::{marker::PhantomData, time::Duration};

use chrono::{TimeDelta, Utc};
use serde::de::DeserializeOwned;
use tokio::task::JoinError;
use tracing::{instrument, Instrument};

use crate::{
    backend::Backend,
    executor::{ExecutionError, ExecutionResult, Executor},
};

use super::{Job, JobId};

const ERROR_TYPE_PANIC: &'static str = "panic";
const ERROR_TYPE_TIMEOUT: &'static str = "timeout";

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
            Ok(Ok(ExecutionResult::Cancelled { .. })) => {
                todo!("Mark job as cancelled")
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
        tracing::debug!(%job_id, "Job complete {job_id}");
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
                "Job {job_id} failed and will be discarded: error type: {}, message: {}",
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
            tracing::warn!(
                %job_id,
                ?error,
                "Job {job_id} failed and will be retried in {delay}: error type: {}, message: {}",
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
            error_type: ERROR_TYPE_PANIC,
            message,
        }
    }
}

impl From<Box<dyn ExecutionError>> for crate::backend::ExecutionError {
    fn from(value: Box<dyn ExecutionError>) -> Self {
        Self {
            error_type: value.error_type(),
            message: value.to_string(),
        }
    }
}

impl From<Duration> for crate::backend::ExecutionError {
    fn from(value: Duration) -> Self {
        Self {
            error_type: ERROR_TYPE_TIMEOUT,
            message: format!("Job failed to complete within timeout: {value:?}"),
        }
    }
}
