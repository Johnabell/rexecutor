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

use crate::{RexecutorPgBackend, map_err, stream::ReadyJobStream};

impl RexecutorPgBackend {
    fn handle_update(result: sqlx::Result<u64>, job_id: JobId) -> Result<(), BackendError> {
        match result {
            Ok(0) => Err(BackendError::JobNotFound(job_id)),
            Ok(1) => Ok(()),
            Ok(_) => Err(BackendError::BadState),
            Err(error) => Err(map_err(error)),
        }
    }
}

#[async_trait]
impl Backend for RexecutorPgBackend {
    #[instrument(skip(self))]
    async fn subscribe_ready_jobs(
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
        .map_err(map_err)
    }
    async fn mark_job_complete(&self, id: JobId) -> Result<(), BackendError> {
        let result = self._mark_job_complete(id).await;
        Self::handle_update(result, id)
    }
    async fn mark_job_retryable(
        &self,
        id: JobId,
        next_scheduled_at: DateTime<Utc>,
        error: ExecutionError,
    ) -> Result<(), BackendError> {
        let result = self._mark_job_retryable(id, next_scheduled_at, error).await;
        Self::handle_update(result, id)
    }
    async fn mark_job_discarded(
        &self,
        id: JobId,
        error: ExecutionError,
    ) -> Result<(), BackendError> {
        let result = self._mark_job_discarded(id, error).await;
        Self::handle_update(result, id)
    }
    async fn mark_job_cancelled(
        &self,
        id: JobId,
        error: ExecutionError,
    ) -> Result<(), BackendError> {
        let result = self._mark_job_cancelled(id, error).await;
        Self::handle_update(result, id)
    }
    async fn mark_job_snoozed(
        &self,
        id: JobId,
        next_scheduled_at: DateTime<Utc>,
    ) -> Result<(), BackendError> {
        let result = self._mark_job_snoozed(id, next_scheduled_at).await;
        Self::handle_update(result, id)
    }
    async fn prune_jobs(&self, spec: &PruneSpec) -> Result<(), BackendError> {
        self.delete_from_spec(spec).await.map_err(map_err)
    }
    async fn rerun_job(&self, id: JobId) -> Result<(), BackendError> {
        let result = self.rerun(id).await;
        Self::handle_update(result, id)
    }
    async fn update_job(&self, job: Job) -> Result<(), BackendError> {
        let id = job.id.into();
        let result = self.update(job).await;
        Self::handle_update(result, id)
    }
    async fn query<'a>(&self, query: Query<'a>) -> Result<Vec<Job>, BackendError> {
        self.run_query(query)
            .await
            .map_err(map_err)?
            .into_iter()
            .map(TryFrom::try_from)
            .collect()
    }
}

#[cfg(test)]
mod test {
    use crate::JobStatus;
    use crate::types::Job;

    use super::*;
    use chrono::TimeDelta;
    use rexecutor::job::ErrorType;
    use serde_json::Value;
    use sqlx::PgPool;

    impl From<PgPool> for RexecutorPgBackend {
        fn from(pool: PgPool) -> Self {
            Self {
                pool,
                subscribers: Default::default(),
            }
        }
    }

    struct MockJob<'a>(EnqueuableJob<'a>);

    impl<'a> From<MockJob<'a>> for EnqueuableJob<'a> {
        fn from(value: MockJob<'a>) -> Self {
            value.0
        }
    }

    impl<'a> Default for MockJob<'a> {
        fn default() -> Self {
            Self(EnqueuableJob {
                executor: "executor".to_owned(),
                data: Value::String("data".to_owned()),
                metadata: Value::String("metadata".to_owned()),
                max_attempts: 5,
                scheduled_at: Utc::now(),
                tags: Default::default(),
                priority: 0,
                uniqueness_criteria: None,
            })
        }
    }

    impl<'a> MockJob<'a> {
        const EXECUTOR: &'static str = "executor";

        async fn enqueue(self, backend: impl Backend) -> JobId {
            backend.enqueue(self.0).await.unwrap()
        }

        fn with_scheduled_at(self, scheduled_at: DateTime<Utc>) -> Self {
            Self(EnqueuableJob {
                scheduled_at,
                ..self.0
            })
        }
    }

    impl RexecutorPgBackend {
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
            .fetch_all(&self.pool)
            .await
        }
    }

    rexecutor::backend::testing::test_suite!(
        attr: sqlx::test,
        args: (pool: PgPool),
        backend: RexecutorPgBackend::from_pool(pool).await.unwrap()
    );

    #[sqlx::test]
    async fn load_job_mark_as_executing_for_executor_returns_none_when_db_empty(pool: PgPool) {
        let backend: RexecutorPgBackend = pool.into();

        let job = backend
            .load_job_mark_as_executing_for_executor(MockJob::EXECUTOR)
            .await
            .unwrap();

        assert!(job.is_none());
    }

    #[sqlx::test]
    async fn load_job_mark_as_executing_for_executor_returns_job_when_ready_for_execution(
        pool: PgPool,
    ) {
        let backend: RexecutorPgBackend = pool.into();

        let job_id = MockJob::default().enqueue(&backend).await;

        let job = backend
            .load_job_mark_as_executing_for_executor(MockJob::EXECUTOR)
            .await
            .unwrap()
            .expect("Should return a job");

        assert_eq!(job.id, job_id);
        assert_eq!(job.status, JobStatus::Executing);
    }

    #[sqlx::test]
    async fn load_job_mark_as_executing_for_executor_does_not_return_executing_jobs(pool: PgPool) {
        let backend: RexecutorPgBackend = pool.into();

        MockJob::default().enqueue(&backend).await;

        let _ = backend
            .load_job_mark_as_executing_for_executor(MockJob::EXECUTOR)
            .await
            .unwrap()
            .expect("Should return a job");

        let job = backend
            .load_job_mark_as_executing_for_executor(MockJob::EXECUTOR)
            .await
            .unwrap();

        assert!(job.is_none());
    }

    #[sqlx::test]
    async fn load_job_mark_as_executing_for_executor_returns_retryable_jobs(pool: PgPool) {
        let backend: RexecutorPgBackend = pool.into();

        let job_id = MockJob::default().enqueue(&backend).await;
        backend
            .mark_job_retryable(
                job_id,
                Utc::now(),
                ExecutionError {
                    error_type: ErrorType::Panic,
                    message: "Oh dear".to_owned(),
                },
            )
            .await
            .unwrap();

        let job = backend
            .load_job_mark_as_executing_for_executor(MockJob::EXECUTOR)
            .await
            .unwrap()
            .expect("Should return a job");

        assert_eq!(job.id, job_id);
        assert_eq!(job.status, JobStatus::Executing);
    }

    #[sqlx::test]
    async fn load_job_mark_as_executing_for_executor_returns_job_when_job_scheduled_in_past(
        pool: PgPool,
    ) {
        let backend: RexecutorPgBackend = pool.into();
        let job_id = MockJob::default()
            .with_scheduled_at(Utc::now() - TimeDelta::hours(3))
            .enqueue(&backend)
            .await;

        let job = backend
            .load_job_mark_as_executing_for_executor(MockJob::EXECUTOR)
            .await
            .unwrap()
            .expect("Should return a job");

        assert_eq!(job.id, job_id);
        assert_eq!(job.status, JobStatus::Executing);
    }

    #[sqlx::test]
    async fn load_job_mark_as_executing_for_executor_returns_oldest_scheduled_at_executable_job(
        pool: PgPool,
    ) {
        let backend: RexecutorPgBackend = pool.into();
        let expected_job_id = MockJob::default()
            .with_scheduled_at(Utc::now() - TimeDelta::hours(3))
            .enqueue(&backend)
            .await;

        let _ = MockJob::default().enqueue(&backend).await;

        let job_id = MockJob::default().enqueue(&backend).await;
        backend.mark_job_complete(job_id).await.unwrap();

        let job_id = MockJob::default().enqueue(&backend).await;
        backend
            .mark_job_discarded(
                job_id,
                ExecutionError {
                    error_type: ErrorType::Panic,
                    message: "Oh dear".to_owned(),
                },
            )
            .await
            .unwrap();

        let job_id = MockJob::default().enqueue(&backend).await;
        backend
            .mark_job_cancelled(
                job_id,
                ExecutionError {
                    error_type: ErrorType::Cancelled,
                    message: "Not needed".to_owned(),
                },
            )
            .await
            .unwrap();

        let job = backend
            .load_job_mark_as_executing_for_executor(MockJob::EXECUTOR)
            .await
            .unwrap()
            .expect("Should return a job");

        assert_eq!(job.id, expected_job_id);
        assert_eq!(job.status, JobStatus::Executing);
    }

    #[sqlx::test]
    async fn enqueue_test(pool: PgPool) {
        let backend: RexecutorPgBackend = pool.into();
        let job = MockJob::default();

        let result = backend.enqueue(job.into()).await;

        assert!(result.is_ok());

        let all_jobs = backend.all_jobs().await.unwrap();

        assert_eq!(all_jobs.len(), 1);
    }
}
