use std::{error::Error, pin::Pin};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;
use redis::{
    aio::ConnectionManager, AsyncCommands, Client, Commands, JsonAsyncCommands, JsonCommands,
    RedisError, RedisResult, ToRedisArgs,
};
use redis_macros::{Json, ToRedisArgs};
use rexecutor::{
    backend::{Backend, BackendError, EnqueuableJob, ExecutionError, Job, Query},
    executor::ExecutorIdentifier,
    job::{JobId, JobStatus},
    pruner::PruneSpec,
};
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Clone)]
pub struct RexecutorRedisBackend {
    conn: ConnectionManager,
    namespace: NameSpace,
}

fn map_err(error: RedisError) -> BackendError {
    dbg!(error);
    BackendError::BadState
}

impl RexecutorRedisBackend {
    pub async fn from_url(redis_url: &str, namespace: impl ToString) -> Result<Self, BackendError> {
        let client = Client::open(redis_url).map_err(map_err)?;

        Ok(Self {
            conn: ConnectionManager::new(client).await.map_err(map_err)?,
            namespace: NameSpace(namespace.to_string()),
        })
    }

    pub async fn next_id(&self) -> RedisResult<i32> {
        self.conn.clone().incr(self.namespace.id(), 1).await
    }
}

#[derive(Clone)]
struct NameSpace(String);

impl NameSpace {
    fn id(&self) -> NameSpacedKey<'_> {
        NameSpacedKey {
            namespace: &self.0,
            kind: KeyType::Id,
        }
    }

    fn job(&self, id: i32) -> NameSpacedKey<'_> {
        NameSpacedKey {
            namespace: &self.0,
            kind: KeyType::Job(id),
        }
    }
}

struct NameSpacedKey<'a> {
    namespace: &'a str,
    kind: KeyType,
}

impl std::fmt::Display for NameSpacedKey<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.namespace)?;
        match self.kind {
            KeyType::Job(id) => write!(f, ":job:{}", id),
            KeyType::Id => write!(f, ":id_counter"),
        }
    }
}

impl<'a> ToRedisArgs for NameSpacedKey<'a> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        out.write_arg_fmt(self);
    }
}

enum KeyType {
    Job(i32),
    Id,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct KeyedJob {
    job: Job,
    pub(crate) key: Option<i64>,
}
pub(crate) fn into_job(job: EnqueuableJob<'_>, id: i32) -> KeyedJob {
    KeyedJob {
        key: job.uniqueness_criteria.and_then(|uq| uq.key),
        job: Job {
            id,
            status: JobStatus::Scheduled,
            executor: job.executor,
            data: job.data,
            metadata: job.metadata,
            attempt: 0,
            max_attempts: job.max_attempts as i32,
            priority: job.priority as i32,
            tags: job.tags,
            errors: vec![],
            inserted_at: Utc::now(),
            scheduled_at: job.scheduled_at,
            attempted_at: None,
            completed_at: None,
            cancelled_at: None,
            discarded_at: None,
        },
    }
}

// #[derive(Debug, FromRedisValue)]
// pub(crate) struct RedisJob<D, M> where D: Serialize + Deserialize {
//     pub id: i32,
//     pub status: JobStatus,
//     pub executor: String,
//     pub data: D,
//     pub metadata: M,
//     pub attempt: i32,
//     pub max_attempts: i32,
//     pub priority: i32,
//     pub tags: Vec<String>,
//     pub errors: Vec<JobError>,
//     pub inserted_at: DateTime<Utc>,
//     pub scheduled_at: DateTime<Utc>,
//     pub attempted_at: Option<DateTime<Utc>>,
//     pub completed_at: Option<DateTime<Utc>>,
//     pub cancelled_at: Option<DateTime<Utc>>,
//     pub discarded_at: Option<DateTime<Utc>>,
// }
//
// #[derive(Debug, Clone, Copy, PartialEq, Eq, FromRedisValue)]
// pub(crate) enum JobStatus {
//     Complete,
//     Executing,
//     Scheduled,
//     Retryable,
//     Cancelled,
//     Discarded,
// }
//
// /// A record of an error that was returned from a job.
// #[derive(Debug, PartialEq, Eq, Clone)]
// pub struct JobError {
//     /// The attempt when this error occurred.
//     pub attempt: u16,
//     /// The type of error that occurred.
//     pub error_type: ErrorType,
//     /// The details of the error.
//     pub details: String,
//     /// The time at which this error was recorded.
//     pub recorded_at: DateTime<Utc>,
// }
//
// /// Calcification of the types of errors that can cause a job to fail to execute successfully.
// #[derive(Debug, PartialEq, Eq, Clone)]
// pub enum ErrorType {
//     /// The execution of the job resulted in a `panic`.
//     Panic,
//     /// The job failed to complete before it timed out.
//     ///
//     /// The timeout can be set using [`crate::executor::Executor::timeout`].
//     Timeout,
//     /// The job was cancelled.
//     ///
//     /// This is achieved by returning [`crate::executor::ExecutionResult::Cancel`] from
//     /// [`crate::executor::Executor`].
//     Cancelled,
//     /// A customer error type.
//     ///
//     /// The details of which will be taken from the error returned as part of
//     /// [`crate::executor::ExecutionResult::Error`] from [`crate::executor::Executor`].
//     Other(String),
// }

#[async_trait]
impl Backend for RexecutorRedisBackend {
    async fn subscribe_ready_jobs(
        &self,
        executor_identifier: ExecutorIdentifier,
    ) -> Pin<Box<dyn Stream<Item = Result<Job, BackendError>> + Send>> {
        todo!()
    }
    async fn enqueue<'a>(&self, job: EnqueuableJob<'a>) -> Result<JobId, BackendError> {
        let id = self.next_id().await.map_err(map_err)?;
        self.conn
            .clone()
            .set(self.namespace.job(id), serde_json::to_string(&into_job(job, id))?)
            .await
            .map_err(map_err)?;
        Ok(id.into())
    }
    async fn mark_job_complete(&self, id: JobId) -> Result<(), BackendError> {
        Err(BackendError::JobNotFound(id))
    }
    async fn mark_job_retryable(
        &self,
        id: JobId,
        next_scheduled_at: DateTime<Utc>,
        error: ExecutionError,
    ) -> Result<(), BackendError> {
        Err(BackendError::JobNotFound(id))
    }
    async fn mark_job_discarded(
        &self,
        id: JobId,
        error: ExecutionError,
    ) -> Result<(), BackendError> {
        Err(BackendError::JobNotFound(id))
    }
    async fn mark_job_cancelled(
        &self,
        id: JobId,
        error: ExecutionError,
    ) -> Result<(), BackendError> {
        Err(BackendError::JobNotFound(id))
    }
    async fn mark_job_snoozed(
        &self,
        id: JobId,
        next_scheduled_at: DateTime<Utc>,
    ) -> Result<(), BackendError> {
        Err(BackendError::JobNotFound(id))
    }
    async fn prune_jobs(&self, prune_spec: &PruneSpec) -> Result<(), BackendError> {
        todo!()
    }
    async fn rerun_job(&self, job_id: JobId) -> Result<(), BackendError> {
        Err(BackendError::JobNotFound(job_id))
    }
    async fn update_job(&self, job: Job) -> Result<(), BackendError> {
        Err(BackendError::JobNotFound(job.id.into()))
    }
    async fn query<'a>(&self, query: Query<'a>) -> Result<Vec<Job>, BackendError> {
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rexecutor::test_suite;

    const DEFAULT_URL: &str = "redis://127.0.0.1";

    test_suite!(for: RexecutorRedisBackend::from_url(DEFAULT_URL, "rexecutor:test").await.unwrap());

    #[tokio::test]
    async fn test() {
        let backend = RexecutorRedisBackend::from_url(DEFAULT_URL, "rexecutor:test")
            .await
            .unwrap();
    }
}
