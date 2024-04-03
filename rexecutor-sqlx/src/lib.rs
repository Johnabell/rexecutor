use std::ops::Deref;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use rexecutor::{
    backend::{Backend, BackendError, EnqueuableJob, ReadyJob},
    job::JobId,
};
use serde::{de::DeserializeOwned, Serialize};
use sqlx::PgPool;

mod types;

#[derive(Clone, Debug)]
pub struct RexecutorPgBackend(PgPool);

impl std::ops::Deref for RexecutorPgBackend {
    type Target = PgPool;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<PgPool> for RexecutorPgBackend {
    fn from(value: PgPool) -> Self {
        Self(value)
    }
}

#[async_trait]
impl Backend for RexecutorPgBackend {
    async fn lock_and_load<D: Send + DeserializeOwned>(
        &self,
        id: JobId,
    ) -> Result<Option<rexecutor::job::Job<D>>, BackendError> {
        let job = self
            .load_job_mark_as_executing(id)
            .await
            .map_err(|_| BackendError::BadStateError)?;
        job.map(TryFrom::try_from).transpose()
    }
    async fn ready_jobs(&self) -> Result<Vec<ReadyJob>, BackendError> {
        self.get_ready_jobs()
            .await
            .map_err(|_| BackendError::BadStateError)
    }
    async fn enqueue<D: Send + Serialize>(
        &self,
        job: EnqueuableJob<D>,
    ) -> Result<JobId, BackendError> {
        self.insert_job(job)
            .await
            // TODO error handling
            .map_err(|_| BackendError::BadStateError)
    }
    async fn next_job_scheduled_at(&self) -> Result<Option<DateTime<Utc>>, BackendError> {
        // TODO add index for this
        self.next_available_job_scheduled_at()
            .await
            .map_err(|_| BackendError::BadStateError)
    }
    async fn mark_job_complete(&self, id: JobId) -> Result<(), BackendError> {
        self._mark_job_complete(id)
            .await
            .map_err(|_| BackendError::BadStateError)
    }
}

use types::*;

impl RexecutorPgBackend {
    const DELTA: Duration = Duration::microseconds(10);

    async fn load_job_mark_as_executing(&self, id: JobId) -> sqlx::Result<Option<Job>> {
        sqlx::query_as!(
            Job,
            r#"UPDATE rexecutor_jobs
            SET status = 'executing', attempted_at = timezone('UTC'::text, now())
            WHERE id = $1 AND status != 'executing'
            RETURNING
                id,
                status AS "status: JobStatus",
                executor,
                data,
                attempt,
                max_attempts,
                errors,
                inserted_at,
                scheduled_at,
                attempted_at,
                completed_at,
                cancelled_at,
                discarded_at
            "#,
            i32::from(id)
        )
        .fetch_optional(self.deref())
        .await
    }

    async fn insert_job<D>(&self, job: EnqueuableJob<D>) -> sqlx::Result<JobId>
    where
        D: Serialize + Send,
    {
        let data = sqlx::query!(
            r#"INSERT INTO rexecutor_jobs (
                executor,
                data,
                max_attempts,
                scheduled_at
            ) VALUES ($1, $2, $3, $4)
            RETURNING id
            "#,
            job.executor,
            // TODO: unwrap
            serde_json::to_value(job.data).unwrap(),
            job.max_attempts as i32,
            job.scheduled_at
        )
        .fetch_one(self.deref())
        .await?;
        Ok(data.id.into())
    }

    async fn _mark_job_complete(&self, id: JobId) -> sqlx::Result<()> {
        sqlx::query!(
            "UPDATE rexecutor_jobs SET status = 'complete' WHERE id = $1",
            i32::from(id),
        )
        .execute(self.deref())
        .await?;
        Ok(())
    }

    async fn update_job(&self, job: Job) -> sqlx::Result<()> {
        sqlx::query!(
            r#"UPDATE rexecutor_jobs SET
                status = $2,
                data = $3,
                attempt = $4,
                max_attempts = $5,
                errors = $6,
                scheduled_at = $7,
                attempted_at = $8,
                completed_at = $9,
                cancelled_at = $10,
                discarded_at = $11
            WHERE id = $1
            "#,
            job.id,
            job.status as _,
            job.data,
            job.attempt,
            job.max_attempts,
            &job.errors,
            job.scheduled_at,
            job.attempted_at,
            job.completed_at,
            job.cancelled_at,
            job.discarded_at,
        )
        .execute(self.deref())
        .await?;
        Ok(())
    }

    async fn next_available_job_scheduled_at(&self) -> sqlx::Result<Option<DateTime<Utc>>> {
        Ok(sqlx::query!(
            r#"SELECT scheduled_at
            FROM rexecutor_jobs
            WHERE status in ('scheduled', 'retryable')
            ORDER BY scheduled_at
            LIMIT 1
            "#
        )
        .fetch_optional(self.deref())
        .await?
        .map(|data| data.scheduled_at))
    }

    async fn get_ready_jobs(&self) -> sqlx::Result<Vec<ReadyJob>> {
        let cut_of = Utc::now() + Self::DELTA;
        Ok(sqlx::query!(
            r#"SELECT id, executor
            FROM rexecutor_jobs
            WHERE status in ('scheduled', 'retryable')
            AND scheduled_at < $1
            ORDER BY scheduled_at
            "#,
            cut_of
        )
        .fetch_all(self.deref())
        .await?
        .into_iter()
        .map(|data| ReadyJob {
            id: data.id.into(),
            executor: data.executor.into(),
        })
        .collect())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::Duration;
    use sqlx::PgPool;

    impl RexecutorPgBackend {
        async fn with_mock_job(&self, scheduled_at: DateTime<Utc>) -> JobId {
            let job = EnqueuableJob {
                executor: "executor".to_owned(),
                data: "data".to_owned(),
                max_attempts: 5,
                scheduled_at,
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
                    attempt,
                    max_attempts,
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

    #[sqlx::test]
    async fn ready_jobs_returns_empty_vec_when_db_empty(pool: PgPool) {
        let backend: RexecutorPgBackend = pool.into();

        let jobs = backend.ready_jobs().await.unwrap();

        assert!(jobs.is_empty());
    }

    #[sqlx::test]
    async fn ready_jobs_returns_empty_jobs_scheduled_in_past(pool: PgPool) {
        let backend: RexecutorPgBackend = pool.into();
        backend.with_mock_job(Utc::now() - Duration::hours(1)).await;
        backend.with_mock_job(Utc::now()).await;

        let jobs = backend.ready_jobs().await.unwrap();

        assert_eq!(jobs.len(), 2);
    }

    // TODO: add tests for ignoring running, cancelled, complete, and discarded jobs

    #[sqlx::test]
    async fn lock_and_load_returns_none_when_there_is_no_job_for_the_id(pool: PgPool) {
        let backend: RexecutorPgBackend = pool.into();
        let job_id = 0.into();

        let job = backend.lock_and_load::<()>(job_id).await.unwrap();

        assert!(job.is_none());
    }

    #[sqlx::test]
    async fn lock_and_load_returns_available_job(pool: PgPool) {
        let backend: RexecutorPgBackend = pool.into();
        let job_id = backend.with_mock_job(Utc::now()).await;

        let job = backend.lock_and_load::<String>(job_id).await.unwrap();

        assert!(job.is_some());
    }

    #[sqlx::test]
    async fn enqueue_test(pool: PgPool) {
        let backend: RexecutorPgBackend = pool.into();
        let job = EnqueuableJob {
            executor: "executor".to_owned(),
            data: "data".to_owned(),
            max_attempts: 5,
            scheduled_at: Utc::now(),
        };

        let result = backend.enqueue(job).await;

        assert!(result.is_ok());

        let all_jobs = backend.all_jobs().await.unwrap();

        assert_eq!(all_jobs.len(), 1);
    }

    #[sqlx::test]
    async fn next_job_scheduled_at_returns_none_when_db_empty(pool: PgPool) {
        let backend: RexecutorPgBackend = pool.into();

        let next_job_scheduled_at = backend.next_job_scheduled_at().await.unwrap();

        assert!(next_job_scheduled_at.is_none());
    }

    #[sqlx::test]
    async fn next_job_scheduled_at_returns_none_when_no_future_jobs(pool: PgPool) {
        let backend: RexecutorPgBackend = pool.into();
        let id = backend.with_mock_job(Utc::now() - Duration::hours(3)).await;
        backend.mark_job_complete(id).await.unwrap();

        let next_job_scheduled_at = backend.next_job_scheduled_at().await.unwrap();

        assert!(next_job_scheduled_at.is_none());
    }

    #[sqlx::test]
    async fn next_job_scheduled_at_returns_timestamp_of_next_job_to_run(pool: PgPool) {
        let backend: RexecutorPgBackend = pool.into();
        let scheduled_at = Utc::now() + Duration::hours(1);
        backend.with_mock_job(scheduled_at).await;
        backend.with_mock_job(Utc::now() + Duration::hours(2)).await;
        backend.with_mock_job(Utc::now() + Duration::hours(3)).await;

        let next_job_scheduled_at = backend.next_job_scheduled_at().await.unwrap().unwrap();

        assert_eq!(scheduled_at, next_job_scheduled_at);
    }

    #[sqlx::test]
    async fn next_job_scheduled_at_returns_timestamp_of_next_job_to_run_even_when_in_past(
        pool: PgPool,
    ) {
        let backend: RexecutorPgBackend = pool.into();
        let scheduled_at = Utc::now() - Duration::hours(1);
        backend.with_mock_job(scheduled_at).await;
        backend.with_mock_job(Utc::now() + Duration::hours(2)).await;
        backend.with_mock_job(Utc::now() + Duration::hours(3)).await;

        let next_job_scheduled_at = backend.next_job_scheduled_at().await.unwrap().unwrap();

        assert_eq!(scheduled_at, next_job_scheduled_at);
    }
}
