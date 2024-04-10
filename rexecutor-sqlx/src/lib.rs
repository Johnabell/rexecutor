use std::{
    collections::HashMap,
    marker::PhantomData,
    ops::{Deref, Sub},
    sync::Arc,
};

use async_stream::stream;
use chrono::{DateTime, Utc};
use futures::Stream;
use rexecutor::{
    backend::{Backend, BackendError, EnqueuableJob, ExecutionError},
    executor::Executor,
    job::JobId,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sqlx::{postgres::PgListener, PgPool};
use tokio::sync::{mpsc, RwLock};

mod types;

#[derive(Clone, Debug)]
pub struct RexecutorPgBackend {
    pool: PgPool,
    subscribers: Arc<RwLock<HashMap<&'static str, Vec<mpsc::UnboundedSender<DateTime<Utc>>>>>>,
}

pub struct RexecutorPgBackendRef<'a>(&'a PgPool);

impl std::ops::Deref for RexecutorPgBackend {
    type Target = PgPool;

    fn deref(&self) -> &Self::Target {
        &self.pool
    }
}

impl From<PgPool> for RexecutorPgBackend {
    fn from(pool: PgPool) -> Self {
        Self {
            pool,
            subscribers: Default::default(),
        }
    }
}

impl From<&PgPool> for RexecutorPgBackend {
    fn from(value: &PgPool) -> Self {
        Self {
            pool: value.to_owned(),
            subscribers: Default::default(),
        }
    }
}

impl Backend for RexecutorPgBackend {
    #[instrument(skip(self))]
    async fn subscribe_new_events<E>(
        self,
    ) -> impl Stream<Item = Result<rexecutor::job::Job<E::Data>, BackendError>>
    where
        E: Executor + Send,
        E::Data: DeserializeOwned + Send,
    {
        let (sender, receiver) = mpsc::unbounded_channel();
        self.subscribers
            .write()
            .await
            .entry(E::NAME)
            .or_default()
            .push(sender);

        let mut stream: ReadyJobStream<E> = ReadyJobStream {
            receiver,
            backend: self.clone(),
            _executor: PhantomData,
        };
        stream! {
            loop {
                yield stream.next().await;
            }
        }
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
}

#[derive(Deserialize, Debug)]
struct Notification {
    executor: String,
    scheduled_at: DateTime<Utc>,
}

pub struct ReadyJobStream<E>
where
    E: Executor,
{
    backend: RexecutorPgBackend,
    receiver: tokio::sync::mpsc::UnboundedReceiver<DateTime<Utc>>,
    _executor: PhantomData<E>,
}

impl<E> ReadyJobStream<E>
where
    E: Executor,
    E::Data: DeserializeOwned,
{
    const DEFAULT_DELAY: std::time::Duration = std::time::Duration::from_secs(5);
    const DELTA: std::time::Duration = std::time::Duration::from_millis(10);

    pub async fn next(&mut self) -> Result<rexecutor::job::Job<E::Data>, BackendError> {
        loop {
            let delay = match self
                .backend
                .next_available_job_scheduled_at_for_executor(E::NAME)
                .await
                .map_err(|_| BackendError::BadStateError)?
            {
                Some(timestamp) => timestamp
                    .sub(Utc::now())
                    .to_std()
                    .unwrap_or(Self::DELTA)
                    .min(Self::DEFAULT_DELAY),
                _ => Self::DEFAULT_DELAY,
            };
            if delay <= Self::DELTA {
                if let Some(job) = self
                    .backend
                    .load_job_mark_as_executing_for_executor(E::NAME)
                    .await
                    .map_err(|_| BackendError::BadStateError)?
                {
                    return job.try_into();
                }
            }
            tokio::select! {
                _ = self.receiver.recv() => { },
                _ = tokio::time::sleep(delay) => { },

            }
        }
    }
}

use tracing::instrument;
use types::*;

impl RexecutorPgBackend {
    pub async fn new(pool: PgPool) -> Result<Self, BackendError> {
        let this = Self {
            pool,
            subscribers: Default::default(),
        };
        let mut listener = PgListener::connect_with(&this)
            .await
            .map_err(|_| BackendError::BadStateError)?;
        listener
            .listen("public.rexecutor_scheduled")
            .await
            .map_err(|_| BackendError::BadStateError)?;

        tokio::spawn({
            let subscribers = this.subscribers.clone();
            async move {
                while let Ok(notification) = listener.recv().await {
                    let notification =
                        serde_json::from_str::<Notification>(notification.payload()).unwrap();

                    subscribers
                        .read()
                        .await
                        .get(&notification.executor.as_str())
                        .into_iter()
                        .flatten()
                        .for_each(|sender| {
                            let _ = sender.send(notification.scheduled_at);
                        });
                }
            }
        });

        Ok(this)
    }

    async fn load_job_mark_as_executing_for_executor(
        &self,
        executor: &str,
    ) -> sqlx::Result<Option<Job>> {
        // TODO the condition here should probably simply be not executing, unless we want a
        // special mechanism for handling rerunning cancelled, completed, and discarded jobs.
        sqlx::query_as!(
            Job,
            r#"UPDATE rexecutor_jobs
            SET
                status = 'executing',
                attempted_at = timezone('UTC'::text, now()),
                attempt = attempt + 1
            WHERE id IN (
                SELECT id from rexecutor_jobs
                WHERE scheduled_at - timezone('UTC'::text, now()) < '00:00:00.1'
                AND status in ('scheduled', 'retryable')
                AND executor = $1
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
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
            executor
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
                scheduled_at,
                tags
            ) VALUES ($1, $2, $3, $4, $5)
            RETURNING id
            "#,
            job.executor,
            // TODO: unwrap
            serde_json::to_value(job.data).unwrap(),
            job.max_attempts as i32,
            job.scheduled_at,
            &job.tags,
        )
        .fetch_one(self.deref())
        .await?;
        Ok(data.id.into())
    }

    async fn _mark_job_complete(&self, id: JobId) -> sqlx::Result<()> {
        sqlx::query!(
            r#"UPDATE rexecutor_jobs
            SET
                status = 'complete',
                completed_at = timezone('UTC'::text, now())
            WHERE id = $1"#,
            i32::from(id),
        )
        .execute(self.deref())
        .await?;
        Ok(())
    }

    async fn _mark_job_retryable(
        &self,
        id: JobId,
        next_scheduled_at: DateTime<Utc>,
        error: ExecutionError,
    ) -> sqlx::Result<()> {
        sqlx::query!(
            r#"UPDATE rexecutor_jobs
            SET
                status = 'retryable',
                scheduled_at = $4,
                errors = ARRAY_APPEND(
                    errors,
                    jsonb_build_object(
                        'attempt', attempt,
                        'error_type', $2::text,
                        'details', $3::text,
                        'recorded_at', timezone('UTC'::text, now())::timestamptz
                    )
                )
            WHERE id = $1"#,
            i32::from(id),
            error.error_type,
            error.message,
            next_scheduled_at,
        )
        .execute(self.deref())
        .await?;
        Ok(())
    }

    async fn _mark_job_discarded(&self, id: JobId, error: ExecutionError) -> sqlx::Result<()> {
        sqlx::query!(
            r#"UPDATE rexecutor_jobs
            SET
                status = 'discarded',
                discarded_at = timezone('UTC'::text, now()),
                errors = ARRAY_APPEND(
                    errors,
                    jsonb_build_object(
                        'attempt', attempt,
                        'error_type', $2::text,
                        'details', $3::text,
                        'recorded_at', timezone('UTC'::text, now())::timestamptz
                    )
                )
            WHERE id = $1"#,
            i32::from(id),
            error.error_type,
            error.message,
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

    async fn next_available_job_scheduled_at_for_executor(
        &self,
        executor: &'static str,
    ) -> sqlx::Result<Option<DateTime<Utc>>> {
        Ok(sqlx::query!(
            r#"SELECT scheduled_at
            FROM rexecutor_jobs
            WHERE status in ('scheduled', 'retryable')
            AND executor = $1
            ORDER BY scheduled_at
            LIMIT 1
            "#,
            executor
        )
        .fetch_optional(self.deref())
        .await?
        .map(|data| data.scheduled_at))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::TimeDelta;
    use sqlx::PgPool;

    const EXECUTOR: &str = "executor";

    impl RexecutorPgBackend {
        async fn with_mock_job(&self, scheduled_at: DateTime<Utc>) -> JobId {
            let job = EnqueuableJob {
                executor: EXECUTOR.to_owned(),
                data: "data".to_owned(),
                max_attempts: 5,
                scheduled_at,
                tags: Default::default(),
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
            data: "data".to_owned(),
            max_attempts: 5,
            scheduled_at: Utc::now(),
            tags: Default::default(),
        };

        let result = backend.enqueue(job).await;

        assert!(result.is_ok());

        let all_jobs = backend.all_jobs().await.unwrap();

        assert_eq!(all_jobs.len(), 1);
    }
}
