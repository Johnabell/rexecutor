use std::marker::PhantomData;

use serde::de::DeserializeOwned;
use tracing::{instrument, Instrument};

use crate::{
    backend::Backend,
    executor::{ExecutionResult, Executor},
};

use super::{Job, JobId};

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

    pub async fn run(&self, job_id: JobId) {
        tracing::debug!(%job_id, "Loading job for execution {job_id}");
        match self.backend.lock_and_load::<E::Data>(job_id).await {
            Ok(Some(job)) => self.execute_job(job).await,
            Ok(None) => tracing::debug!("No job available to execute"),
            // TODO handle error here mark for retry
            Err(err) => tracing::error!(?err, "Failed to read job"),
        }
    }

    #[instrument(skip(self, job), fields(job_id))]
    pub async fn execute_job(&self, job: Job<E::Data>) {
        let job_id = job.id;
        tracing::Span::current().record("job_id", &tracing::field::debug(&job_id));
        let fut = tokio::time::timeout(E::timeout(&job), E::execute(job)).in_current_span();
        let result = if E::BLOCKING {
            tracing::debug!(%job_id, "Executing blocking job {job_id}");
            tokio::task::spawn_blocking(|| futures::executor::block_on(fut))
        } else {
            tracing::debug!(%job_id, "Executing job {job_id}");
            tokio::spawn(fut)
        };
        match result.await {
            Ok(Ok(ExecutionResult::Done)) => {
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
            Ok(Ok(ExecutionResult::Cancelled { .. })) => {
                todo!("Mark job as cancelled")
            }
            Ok(Ok(ExecutionResult::Snooze { .. })) => {
                todo!("Snooze job")
            }
            Ok(Ok(ExecutionResult::Error { .. })) => {
                todo!("Reschedule and record error")
            }
            Ok(Err(_elaped)) => {
                todo!("Handle timeout")
            }
            Err(_e) => {
                // Job paniced
                todo!("Reschedule and record error")
            }
        }
    }
}
