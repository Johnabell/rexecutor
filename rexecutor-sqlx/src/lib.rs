use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    ops::{Deref, Sub},
    pin::Pin,
    sync::Arc,
};

use async_stream::stream;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;
use rexecutor::{
    backend::{Backend, BackendError, EnqueuableJob, ExecutionError},
    executor::ExecutorIdentifier,
    job::{uniqueness_criteria::UniquenessCriteria, JobId},
};
use serde::Deserialize;
use sqlx::{
    postgres::{PgListener, PgPoolOptions},
    types::Text,
    PgPool, Postgres, QueryBuilder, Row,
};
use tokio::sync::{mpsc, RwLock};

mod types;

type Subscriber = mpsc::UnboundedSender<DateTime<Utc>>;

#[derive(Clone, Debug)]
pub struct RexecutorPgBackend {
    pool: PgPool,
    subscribers: Arc<RwLock<HashMap<&'static str, Vec<Subscriber>>>>,
}

pub struct RexecutorPgBackendRef<'a>(&'a PgPool);

impl std::ops::Deref for RexecutorPgBackend {
    type Target = PgPool;

    fn deref(&self) -> &Self::Target {
        &self.pool
    }
}

#[async_trait]
impl Backend for RexecutorPgBackend {
    #[instrument(skip(self))]
    async fn subscribe_new_events(
        &self,
        excutor_identifier: ExecutorIdentifier,
    ) -> Pin<Box<dyn Stream<Item = Result<rexecutor::backend::Job, BackendError>> + Send>> {
        let (sender, receiver) = mpsc::unbounded_channel();
        self.subscribers
            .write()
            .await
            .entry(excutor_identifier.as_str())
            .or_default()
            .push(sender);

        let mut stream: ReadyJobStream = ReadyJobStream {
            receiver,
            backend: self.clone(),
            excutor_identifier,
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
}

#[derive(Deserialize, Debug)]
struct Notification {
    executor: String,
    scheduled_at: DateTime<Utc>,
}

pub struct ReadyJobStream {
    backend: RexecutorPgBackend,
    excutor_identifier: ExecutorIdentifier,
    receiver: tokio::sync::mpsc::UnboundedReceiver<DateTime<Utc>>,
}

impl ReadyJobStream {
    const DEFAULT_DELAY: std::time::Duration = std::time::Duration::from_secs(30);
    const DELTA: std::time::Duration = std::time::Duration::from_millis(15);

    pub async fn next(&mut self) -> Result<rexecutor::backend::Job, BackendError> {
        loop {
            let delay = match self
                .backend
                .next_available_job_scheduled_at_for_executor(self.excutor_identifier.as_str())
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
                    .load_job_mark_as_executing_for_executor(self.excutor_identifier.as_str())
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
    /// Creates a new [RexecutorPgBackend] from a db connection string.
    pub async fn from_db_url(db_url: &str) -> Result<Self, BackendError> {
        let pool = PgPoolOptions::new()
            .connect(db_url)
            .await
            .map_err(|_| BackendError::BadStateError)?;
        Self::from_pool(pool).await
    }
    pub async fn from_pool(pool: PgPool) -> Result<Self, BackendError> {
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

                    match subscribers
                        .read()
                        .await
                        .get(&notification.executor.as_str())
                    {
                        Some(subscribers) => subscribers.iter().for_each(|sender| {
                            let _ = sender.send(notification.scheduled_at);
                        }),
                        None => {
                            tracing::warn!("No executors running for {}", notification.executor)
                        }
                    }
                }
            }
        });

        Ok(this)
    }

    pub async fn run_migrations(&self) -> Result<(), BackendError> {
        tracing::info!("Running RexecutorPgBackend migrations");
        sqlx::migrate!()
            .run(&self.pool)
            .await
            .map_err(|_| BackendError::BadStateError)
    }

    async fn load_job_mark_as_executing_for_executor(
        &self,
        executor: &str,
    ) -> sqlx::Result<Option<Job>> {
        // TODO implement special mechanism for rerunning cancelled, completed, and discarded jobs.
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
                ORDER BY priority, scheduled_at
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
                priority,
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

    async fn insert_job<'a>(&self, job: EnqueuableJob<'a>) -> sqlx::Result<JobId> {
        let data = sqlx::query!(
            r#"INSERT INTO rexecutor_jobs (
                executor,
                data,
                max_attempts,
                scheduled_at,
                priority,
                tags
            ) VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id
            "#,
            job.executor,
            job.data,
            job.max_attempts as i32,
            job.scheduled_at,
            job.priority as i32,
            &job.tags,
        )
        .fetch_one(self.deref())
        .await?;
        Ok(data.id.into())
    }

    async fn insert_unique_job<'a>(&self, job: EnqueuableJob<'a>) -> sqlx::Result<JobId> {
        let Some(uniqueness_criteria) = job.uniqueness_criteria else {
            panic!();
        };
        let mut tx = self.begin().await?;
        let unique_identifier = uniqueness_criteria.unique_identifier(job.executor.as_str());
        sqlx::query!("SELECT pg_advisory_xact_lock($1)", unique_identifier)
            .execute(&mut *tx)
            .await?;
        match uniqueness_criteria
            .query(unique_identifier, job.scheduled_at)
            .build()
            .fetch_optional(&mut *tx)
            .await?
        {
            None => {
                let data = sqlx::query!(
                    r#"INSERT INTO rexecutor_jobs (
                        executor,
                        data,
                        max_attempts,
                        scheduled_at,
                        priority,
                        tags,
                        uniqueness_key
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                    RETURNING id
                    "#,
                    job.executor,
                    job.data,
                    job.max_attempts as i32,
                    job.scheduled_at,
                    job.priority as i32,
                    &job.tags,
                    unique_identifier,
                )
                .fetch_one(&mut *tx)
                .await?;
                tx.commit().await?;
                Ok(data.id.into())
            }
            Some(val) => {
                tx.rollback().await?;
                Ok(val.get::<i32, _>(0).into())
            }
        }
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
            Text(ErrorType::from(error.error_type)) as _,
            error.message,
            next_scheduled_at,
        )
        .execute(self.deref())
        .await?;
        Ok(())
    }

    async fn _mark_job_snoozed(
        &self,
        id: JobId,
        next_scheduled_at: DateTime<Utc>,
    ) -> sqlx::Result<()> {
        sqlx::query!(
            r#"UPDATE rexecutor_jobs
            SET
                status = (CASE WHEN attempt = 1 THEN 'scheduled' ELSE 'retryable' END)::rexecutor_job_state,
                scheduled_at = $2,
                attempt = attempt - 1
            WHERE id = $1"#,
            i32::from(id),
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
            Text(ErrorType::from(error.error_type)) as _,
            error.message,
        )
        .execute(self.deref())
        .await?;
        Ok(())
    }

    async fn _mark_job_cancelled(&self, id: JobId, error: ExecutionError) -> sqlx::Result<()> {
        sqlx::query!(
            r#"UPDATE rexecutor_jobs
            SET
                status = 'cancelled',
                cancelled_at = timezone('UTC'::text, now()),
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
            Text(ErrorType::from(error.error_type)) as _,
            error.message,
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

trait Unique {
    fn unique_identifier(&self, executor_identifier: &'_ str) -> i64;
    fn query(&self, key: i64, scheduled_at: DateTime<Utc>) -> QueryBuilder<'_, Postgres>;
}

impl<'a> Unique for UniquenessCriteria<'a> {
    fn unique_identifier(&self, executor_identifier: &'_ str) -> i64 {
        let mut state = fxhash::FxHasher64::default();
        self.key.hash(&mut state);
        if self.executor {
            executor_identifier.hash(&mut state);
        }
        self.statuses.hash(&mut state);
        state.finish() as i64
    }

    fn query(&self, key: i64, scheduled_at: DateTime<Utc>) -> QueryBuilder<'_, Postgres> {
        let mut builder =
            QueryBuilder::new("SELECT id FROM rexecutor_jobs WHERE uniqueness_key = ");
        builder.push_bind(key);
        if let Some(duration) = self.duration {
            let cutoff = scheduled_at - duration;
            builder.push(" AND scheduled_at >= ").push_bind(cutoff);
        }

        builder
    }
}

#[cfg(test)]
mod test {
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
                    attempt,
                    max_attempts,
                    priority,
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
