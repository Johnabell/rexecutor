//! A postgres backend for Rexecutor built on [`sqlx`].
#![deny(missing_docs)]

use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Utc};
use rexecutor::{
    backend::{BackendError, EnqueuableJob, ExecutionError, Query},
    job::{JobId, uniqueness_criteria::Resolution},
    pruner::PruneSpec,
};
use serde::Deserialize;
use sqlx::{
    PgPool, QueryBuilder, Row,
    postgres::{PgListener, PgPoolOptions},
    types::Text,
};
use tokio::sync::{RwLock, mpsc};

use crate::{query::ToQuery, unique::Unique};

mod backend;
mod query;
mod stream;
mod types;
mod unique;

type Subscriber = mpsc::UnboundedSender<DateTime<Utc>>;

/// A postgres implementation of a [`rexecutor::backend::Backend`].
#[derive(Clone, Debug)]
pub struct RexecutorPgBackend {
    pool: PgPool,
    subscribers: Arc<RwLock<HashMap<&'static str, Vec<Subscriber>>>>,
}

#[derive(Deserialize, Debug)]
struct Notification {
    executor: String,
    scheduled_at: DateTime<Utc>,
}

use types::*;

fn map_err(error: sqlx::Error) -> BackendError {
    match error {
        sqlx::Error::Io(err) => BackendError::Io(err),
        sqlx::Error::Tls(err) => BackendError::Io(std::io::Error::other(err)),
        sqlx::Error::Protocol(err) => BackendError::Io(std::io::Error::other(err)),
        sqlx::Error::AnyDriverError(err) => BackendError::Io(std::io::Error::other(err)),
        sqlx::Error::PoolTimedOut => BackendError::Io(std::io::Error::other(error)),
        sqlx::Error::PoolClosed => BackendError::Io(std::io::Error::other(error)),
        _ => BackendError::BadState,
    }
}

impl RexecutorPgBackend {
    /// Creates a new [`RexecutorPgBackend`] from a db connection string.
    pub async fn from_db_url(db_url: &str) -> Result<Self, BackendError> {
        let pool = PgPoolOptions::new()
            .connect(db_url)
            .await
            .map_err(map_err)?;
        Self::from_pool(pool).await
    }
    /// Create a new [`RexecutorPgBackend`] from an existing [`PgPool`].
    pub async fn from_pool(pool: PgPool) -> Result<Self, BackendError> {
        let this = Self {
            pool,
            subscribers: Default::default(),
        };
        let mut listener = PgListener::connect_with(&this.pool)
            .await
            .map_err(map_err)?;
        listener
            .listen("public.rexecutor_scheduled")
            .await
            .map_err(map_err)?;

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

    /// This can be used to run the [`RexecutorPgBackend`]'s migrations.
    pub async fn run_migrations(&self) -> Result<(), BackendError> {
        tracing::info!("Running RexecutorPgBackend migrations");
        sqlx::migrate!()
            .run(&self.pool)
            .await
            .map_err(|err| BackendError::Io(std::io::Error::other(err)))
    }

    async fn load_job_mark_as_executing_for_executor(
        &self,
        executor: &str,
    ) -> sqlx::Result<Option<Job>> {
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
            "#,
            executor
        )
        .fetch_optional(&self.pool)
        .await
    }

    async fn insert_job<'a>(&self, job: EnqueuableJob<'a>) -> sqlx::Result<JobId> {
        let data = sqlx::query!(
            r#"INSERT INTO rexecutor_jobs (
                executor,
                data,
                metadata,
                max_attempts,
                scheduled_at,
                priority,
                tags
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id
            "#,
            job.executor,
            job.data,
            job.metadata,
            job.max_attempts as i32,
            job.scheduled_at,
            job.priority as i32,
            &job.tags,
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(data.id.into())
    }

    async fn insert_unique_job<'a>(&self, job: EnqueuableJob<'a>) -> sqlx::Result<JobId> {
        let Some(uniqueness_criteria) = job.uniqueness_criteria else {
            panic!();
        };
        let mut tx = self.pool.begin().await?;
        let unique_identifier = uniqueness_criteria.unique_identifier(job.executor.as_str());
        sqlx::query!("SELECT pg_advisory_xact_lock($1)", unique_identifier)
            .execute(&mut *tx)
            .await?;
        match uniqueness_criteria
            .query(job.executor.as_str(), job.scheduled_at)
            .build()
            .fetch_optional(&mut *tx)
            .await?
        {
            None => {
                let data = sqlx::query!(
                    r#"INSERT INTO rexecutor_jobs (
                        executor,
                        data,
                        metadata,
                        max_attempts,
                        scheduled_at,
                        priority,
                        tags,
                        uniqueness_key
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    RETURNING id
                    "#,
                    job.executor,
                    job.data,
                    job.metadata,
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
                let job_id = val.get::<i32, _>(0);
                let status = val.get::<JobStatus, _>(1);
                match uniqueness_criteria.on_conflict {
                    Resolution::Replace(replace)
                        if replace
                            .for_statuses
                            .iter()
                            .map(|js| JobStatus::from(*js))
                            .any(|js| js == status) =>
                    {
                        let mut builder = QueryBuilder::new("UPDATE rexecutor_jobs SET ");
                        let mut seperated = builder.separated(", ");
                        if replace.scheduled_at {
                            seperated.push("scheduled_at = ");
                            seperated.push_bind_unseparated(job.scheduled_at);
                        }
                        if replace.data {
                            seperated.push("data = ");
                            seperated.push_bind_unseparated(job.data);
                        }
                        if replace.metadata {
                            seperated.push("metadata = ");
                            seperated.push_bind_unseparated(job.metadata);
                        }
                        if replace.priority {
                            seperated.push("priority = ");
                            seperated.push_bind_unseparated(job.priority as i32);
                        }
                        if replace.max_attempts {
                            seperated.push("max_attempts = ");
                            seperated.push_bind_unseparated(job.max_attempts as i32);
                        }
                        builder.push(" WHERE id  = ");
                        builder.push_bind(job_id);
                        builder.build().execute(&mut *tx).await?;
                        tx.commit().await?;
                    }
                    _ => {
                        tx.rollback().await?;
                    }
                }
                Ok(job_id.into())
            }
        }
    }

    async fn _mark_job_complete(&self, id: JobId) -> sqlx::Result<u64> {
        Ok(sqlx::query!(
            r#"UPDATE rexecutor_jobs
            SET
                status = 'complete',
                completed_at = timezone('UTC'::text, now())
            WHERE id = $1"#,
            i32::from(id),
        )
        .execute(&self.pool)
        .await?
        .rows_affected())
    }

    async fn _mark_job_retryable(
        &self,
        id: JobId,
        next_scheduled_at: DateTime<Utc>,
        error: ExecutionError,
    ) -> sqlx::Result<u64> {
        Ok(sqlx::query!(
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
        .execute(&self.pool)
        .await?
        .rows_affected())
    }

    async fn _mark_job_snoozed(
        &self,
        id: JobId,
        next_scheduled_at: DateTime<Utc>,
    ) -> sqlx::Result<u64> {
        Ok(sqlx::query!(
            r#"UPDATE rexecutor_jobs
            SET
                status = (CASE WHEN attempt = 1 THEN 'scheduled' ELSE 'retryable' END)::rexecutor_job_state,
                scheduled_at = $2,
                attempt = attempt - 1
            WHERE id = $1"#,
            i32::from(id),
            next_scheduled_at,
        )
        .execute(&self.pool)
        .await?
        .rows_affected())
    }

    async fn _mark_job_discarded(&self, id: JobId, error: ExecutionError) -> sqlx::Result<u64> {
        Ok(sqlx::query!(
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
        .execute(&self.pool)
        .await?
        .rows_affected())
    }

    async fn _mark_job_cancelled(&self, id: JobId, error: ExecutionError) -> sqlx::Result<u64> {
        Ok(sqlx::query!(
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
        .execute(&self.pool)
        .await?
        .rows_affected())
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
        .fetch_optional(&self.pool)
        .await?
        .map(|data| data.scheduled_at))
    }

    async fn delete_from_spec(&self, spec: &PruneSpec) -> sqlx::Result<()> {
        let result = spec.query().build().execute(&self.pool).await?;
        tracing::debug!(
            ?spec,
            "Clean up query completed {} rows removed",
            result.rows_affected()
        );
        Ok(())
    }

    async fn rerun(&self, id: JobId) -> sqlx::Result<u64> {
        // Currently this increments the max attempts and reschedules the job.
        Ok(sqlx::query!(
            r#"UPDATE rexecutor_jobs
            SET
                status = (CASE WHEN attempt = 1 THEN 'scheduled' ELSE 'retryable' END)::rexecutor_job_state,
                scheduled_at = $2,
                completed_at = null,
                cancelled_at = null,
                discarded_at = null,
                max_attempts = max_attempts + 1
            WHERE id = $1"#,
            i32::from(id),
            Utc::now(),
        )
        .execute(&self.pool)
        .await?
        .rows_affected())
    }

    async fn update(&self, job: rexecutor::backend::Job) -> sqlx::Result<u64> {
        Ok(sqlx::query!(
            r#"UPDATE rexecutor_jobs
            SET
                data = $2,
                metadata = $3,
                max_attempts = $4,
                scheduled_at = $5,
                priority = $6,
                tags = $7
            WHERE id = $1"#,
            job.id,
            job.data,
            job.metadata,
            job.max_attempts as i32,
            job.scheduled_at,
            job.priority as i32,
            &job.tags,
        )
        .execute(&self.pool)
        .await?
        .rows_affected())
    }

    async fn run_query<'a>(&self, query: Query<'a>) -> sqlx::Result<Vec<Job>> {
        query
            .query()
            .build_query_as::<Job>()
            .fetch_all(&self.pool)
            .await
    }
}
