use std::pin::Pin;

use async_stream::stream;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;
use rexecutor::{
    backend::{Backend, BackendError, EnqueuableJob, ExecutionError, Job, Query},
    executor::ExecutorIdentifier,
    job::JobId,
    pruner::PruneSpec,
};
use tokio::sync::mpsc;
use tracing::instrument;

use crate::{stream::ReadyJobStream, RexecutorPgBackend};

#[async_trait]
impl Backend for RexecutorPgBackend {
    #[instrument(skip(self))]
    async fn subscribe_new_events(
        &self,
        executor_identifier: ExecutorIdentifier,
    ) -> Pin<Box<dyn Stream<Item = Result<Job, BackendError>> + Send>> {
        let (sender, receiver) = mpsc::unbounded_channel();
        self.subscribers
            .write()
            .await
            .entry(executor_identifier.as_str())
            .or_default()
            .push(sender);

        let mut stream: ReadyJobStream = ReadyJobStream {
            receiver,
            backend: self.clone(),
            executor_identifier,
        };
        Box::pin(stream! {
            loop {
                yield stream.next().await;
            }
        })
    }
    async fn enqueue<'a>(&self, job: EnqueuableJob<'a>) -> Result<JobId, BackendError> {
        if job.uniqueness_criteria.is_some() {
            self.insert_unique_job(job).await
        } else {
            self.insert_job(job).await
        }
        // TODO error handling
        .map_err(|_| BackendError::BadStateError)
    }
    async fn mark_job_complete(&self, id: JobId) -> Result<(), BackendError> {
        self._mark_job_complete(id)
            .await
            .map_err(|_| BackendError::BadStateError)
    }
    async fn mark_job_retryable(
        &self,
        id: JobId,
        next_scheduled_at: DateTime<Utc>,
        error: ExecutionError,
    ) -> Result<(), BackendError> {
        self._mark_job_retryable(id, next_scheduled_at, error)
            .await
            .map_err(|_| BackendError::BadStateError)
    }
    async fn mark_job_discarded(
        &self,
        id: JobId,
        error: ExecutionError,
    ) -> Result<(), BackendError> {
        self._mark_job_discarded(id, error)
            .await
            .map_err(|_| BackendError::BadStateError)
    }
    async fn mark_job_cancelled(
        &self,
        id: JobId,
        error: ExecutionError,
    ) -> Result<(), BackendError> {
        self._mark_job_cancelled(id, error)
            .await
            .map_err(|_| BackendError::BadStateError)
    }
    async fn mark_job_snoozed(
        &self,
        id: JobId,
        next_scheduled_at: DateTime<Utc>,
    ) -> Result<(), BackendError> {
        self._mark_job_snoozed(id, next_scheduled_at)
            .await
            .map_err(|_| BackendError::BadStateError)
    }
    async fn prune_jobs(&self, spec: &PruneSpec) -> Result<(), BackendError> {
        self.delete_from_spec(spec)
            .await
            .map_err(|_| BackendError::BadStateError)
    }
    async fn rerun_job(&self, id: JobId) -> Result<(), BackendError> {
        self.rerun(id)
            .await
            .map_err(|_| BackendError::BadStateError)
    }
    async fn update_job(&self, job: Job) -> Result<(), BackendError> {
        self.update(job)
            .await
            .map_err(|_| BackendError::BadStateError)
    }
    async fn query<'a>(&self, query: Query<'a>) -> Result<Vec<Job>, BackendError> {
        self
            .run_query(query)
            .await
            .map_err(|_| BackendError::BadStateError)?
            .into_iter()
            .map(TryFrom::try_from)
            .collect()
    }
}

#[cfg(test)]
mod test {
    use crate::JobStatus;
    use std::ops::Deref;

    use crate::types::Job;

    use super::*;
    use chrono::TimeDelta;
    use serde_json::Value;
    use sqlx::PgPool;

    const EXECUTOR: &str = "executor";

    impl From<PgPool> for RexecutorPgBackend {
        fn from(pool: PgPool) -> Self {
            Self {
                pool,
                subscribers: Default::default(),
            }
        }
    }

    impl RexecutorPgBackend {
        async fn with_mock_job(&self, scheduled_at: DateTime<Utc>) -> JobId {
            let job = EnqueuableJob {
                executor: EXECUTOR.to_owned(),
                data: Value::String("data".to_owned()),
                metadata: Value::String("metadata".to_owned()),
                max_attempts: 5,
                scheduled_at,
                tags: Default::default(),
                priority: 0,
                uniqueness_criteria: None,
            };

            self.enqueue(job).await.unwrap()
        }

        async fn all_jobs(&self) -> sqlx::Result<Vec<Job>> {
            sqlx::query_as!(
                Job,
                r#"SELECT
                    id,
                    status AS "status: JobStatus",
                    executor,
                    data,
                    metadata,
                    attempt,
                    max_attempts,
                    priority,
                    tags,
                    errors,
                    inserted_at,
                    scheduled_at,
                    attempted_at,
                    completed_at,
                    cancelled_at,
                    discarded_at
                FROM rexecutor_jobs
                "#
            )
            .fetch_all(self.deref())
            .await
        }
    }

    // TODO: add tests for ignoring running, cancelled, complete, and discarded jobs

    #[sqlx::test]
    async fn load_job_mark_as_executing_for_executor_returns_none_when_db_empty(pool: PgPool) {
        let backend: RexecutorPgBackend = pool.into();

        let job = backend
            .load_job_mark_as_executing_for_executor(EXECUTOR)
            .await
            .unwrap();

        assert!(job.is_none());
    }

    #[sqlx::test]
    async fn load_job_mark_as_executing_for_executor_returns_job_when_ready_for_execution(
        pool: PgPool,
    ) {
        let backend: RexecutorPgBackend = pool.into();
        let _ = backend.with_mock_job(Utc::now()).await;

        let job = backend
            .load_job_mark_as_executing_for_executor(EXECUTOR)
            .await
            .unwrap();

        assert!(job.is_some());
    }

    #[sqlx::test]
    async fn load_job_mark_as_executing_for_executor_returns_job_when_job_scheduled_in_past(
        pool: PgPool,
    ) {
        let backend: RexecutorPgBackend = pool.into();
        let _ = backend
            .with_mock_job(Utc::now() - TimeDelta::hours(3))
            .await;

        let job = backend
            .load_job_mark_as_executing_for_executor(EXECUTOR)
            .await
            .unwrap();

        assert!(job.is_some());
    }

    #[sqlx::test]
    async fn enqueue_test(pool: PgPool) {
        let backend: RexecutorPgBackend = pool.into();
        let job = EnqueuableJob {
            executor: "executor".to_owned(),
            data: Value::String("data".to_owned()),
            metadata: Value::String("metadata".to_owned()),
            max_attempts: 5,
            scheduled_at: Utc::now(),
            tags: Default::default(),
            priority: 0,
            uniqueness_criteria: None,
        };

        let result = backend.enqueue(job).await;

        assert!(result.is_ok());

        let all_jobs = backend.all_jobs().await.unwrap();

        assert_eq!(all_jobs.len(), 1);
    }
}
