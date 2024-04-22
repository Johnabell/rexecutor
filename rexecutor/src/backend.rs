use std::pin::Pin;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;
use serde::Serialize;
use thiserror::Error;

use crate::{
    executor::ExecutorIdentifier,
    job::{self, uniqueness_criteria::UniquenessCriteria, ErrorType, JobError, JobId, JobStatus},
    pruner::PruneSpec,
};
#[async_trait]
pub trait Backend: std::fmt::Debug {
    async fn subscribe_new_events(
        &self,
        executor_identifier: ExecutorIdentifier,
    ) -> Pin<Box<dyn Stream<Item = Result<Job, BackendError>> + Send>>;
    async fn enqueue<'a>(&self, job: EnqueuableJob<'a>) -> Result<JobId, BackendError>;
    async fn mark_job_complete(&self, id: JobId) -> Result<(), BackendError>;
    async fn mark_job_retryable(
        &self,
        id: JobId,
        next_scheduled_at: DateTime<Utc>,
        error: ExecutionError,
    ) -> Result<(), BackendError>;
    async fn mark_job_discarded(
        &self,
        id: JobId,
        error: ExecutionError,
    ) -> Result<(), BackendError>;
    async fn mark_job_cancelled(
        &self,
        id: JobId,
        error: ExecutionError,
    ) -> Result<(), BackendError>;
    async fn mark_job_snoozed(
        &self,
        id: JobId,
        next_scheduled_at: DateTime<Utc>,
    ) -> Result<(), BackendError>;
    async fn prune_jobs(&self, prune_spec: &PruneSpec) -> Result<(), BackendError>;
    async fn rerun_job(&self, job_id: JobId) -> Result<(), BackendError>;
    async fn update_job(&self, job: Job) -> Result<(), BackendError>;
    async fn query<'a>(&self, query: Query<'a>) -> Result<Vec<Job>, BackendError>;
}

#[derive(Debug)]
pub struct ExecutionError {
    pub error_type: ErrorType,
    pub message: String,
}

// TODO: should this be non_exhaustive?
// #[non_exhaustive]
#[derive(Debug)]
pub struct EnqueuableJob<'a> {
    pub executor: String,
    pub data: serde_json::Value,
    pub metadata: serde_json::Value,
    pub max_attempts: u16,
    pub scheduled_at: DateTime<Utc>,
    pub tags: Vec<String>,
    pub priority: u16,
    pub uniqueness_criteria: Option<UniquenessCriteria<'a>>,
}

#[derive(Debug)]
pub struct Job {
    pub id: i32,
    pub status: JobStatus,
    pub executor: String,
    pub data: serde_json::Value,
    pub metadata: serde_json::Value,
    pub attempt: i32,
    pub max_attempts: i32,
    pub priority: i32,
    pub tags: Vec<String>,
    pub errors: Vec<JobError>,
    pub inserted_at: DateTime<Utc>,
    pub scheduled_at: DateTime<Utc>,
    pub attempted_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub cancelled_at: Option<DateTime<Utc>>,
    pub discarded_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Query<'a> {
    Not(Box<Query<'a>>),
    And(Vec<Query<'a>>),
    Or(Vec<Query<'a>>),
    ExecutorEqual(&'static str),
    DataEquals(serde_json::Value),
    MetadataEquals(serde_json::Value),
    IdEquals(JobId),
    IdIn(&'a [JobId]),
    StatusEqual(JobStatus),
    TagsAllOf(&'a [&'a str]),
    TagsOneOf(&'a [&'a str]),
    ScheduledAtBefore(DateTime<Utc>),
    ScheduledAtAfter(DateTime<Utc>),
    ScheduledAtEqual(DateTime<Utc>),
}

impl<'a> Query<'a> {
    pub(crate) fn for_executor(self, executor: &'static str) -> Self {
        Self::And(vec![Query::ExecutorEqual(executor), self])
    }
}

impl<'a, D, M> TryFrom<job::query::Query<'a, D, M>> for Query<'a>
where
    D: Serialize,
    M: Serialize,
{
    type Error = serde_json::Error;

    fn try_from(value: job::query::Query<'a, D, M>) -> Result<Self, Self::Error> {
        Ok(match value {
            job::query::Query::Not(query) => Self::Not(Box::new((*query).try_into()?)),
            job::query::Query::And(queries) => Self::And(
                queries
                    .into_iter()
                    .map(TryFrom::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            job::query::Query::Or(queries) => Self::Or(
                queries
                    .into_iter()
                    .map(TryFrom::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            job::query::Query::DataEquals(data) => Self::DataEquals(serde_json::to_value(data)?),
            job::query::Query::MetadataEquals(metadata) => {
                Self::MetadataEquals(serde_json::to_value(metadata)?)
            }
            job::query::Query::IdEquals(id) => Self::IdEquals(id),
            job::query::Query::IdIn(ids) => Self::IdIn(ids),
            job::query::Query::StatusEqual(status) => Self::StatusEqual(status),
            job::query::Query::TagsAllOf(tags) => Self::TagsAllOf(tags),
            job::query::Query::TagsOneOf(tags) => Self::TagsOneOf(tags),
            job::query::Query::ScheduledAtBefore(scheduled_at) => {
                Self::ScheduledAtBefore(scheduled_at)
            }
            job::query::Query::ScheduledAtAfter(scheduled_at) => {
                Self::ScheduledAtAfter(scheduled_at)
            }
            job::query::Query::ScheduledAtEqual(scheduled_at) => {
                Self::ScheduledAtEqual(scheduled_at)
            }
        })
    }
}

impl<D, M> TryFrom<job::Job<D, M>> for Job
where
    D: Serialize,
    M: Serialize,
{
    type Error = serde_json::Error;

    fn try_from(value: job::Job<D, M>) -> Result<Self, Self::Error> {
        let data = serde_json::to_value(value.data)?;
        let metadata = serde_json::to_value(value.metadata)?;
        Ok(Self {
            id: value.id.into(),
            status: value.status,
            executor: value.executor,
            data,
            metadata,
            attempt: value.attempt as _,
            attempted_at: value.attempted_at,
            max_attempts: value.max_attempts as _,
            tags: value.tags,
            priority: value.priority as _,
            errors: value.errors,
            inserted_at: value.inserted_at,
            scheduled_at: value.scheduled_at,
            completed_at: value.completed_at,
            cancelled_at: value.cancelled_at,
            discarded_at: value.discarded_at,
        })
    }
}

#[derive(Debug, Error)]
pub enum BackendError {
    #[error("Error encoding or decoding data")]
    // Not sure whether we should enforce json here
    EncodeDecodeError(#[from] serde_json::Error),
    #[error("System in bad state")]
    BadStateError,
}

pub struct DefaultBackend {}

#[cfg(test)]
pub(crate) mod test {

    use std::sync::{Arc, Mutex};

    use super::*;

    #[derive(Clone, Debug)]
    pub struct MockBackend {
        enqueue_return: Arc<Mutex<Vec<Result<JobId, BackendError>>>>,
    }

    impl Default for MockBackend {
        fn default() -> Self {
            Self {
                enqueue_return: Arc::new(vec![].into()),
            }
        }
    }

    #[async_trait]
    impl Backend for MockBackend {
        async fn subscribe_new_events(
            &self,
            _executor_name: ExecutorIdentifier,
        ) -> Pin<Box<dyn Stream<Item = Result<Job, BackendError>> + Send>> {
            Box::pin(futures::stream::empty())
        }
        async fn enqueue<'a>(&self, _job: EnqueuableJob<'a>) -> Result<JobId, BackendError> {
            self.enqueue_return
                .lock()
                .unwrap()
                .pop()
                .unwrap_or(Ok(0.into()))
        }
        async fn mark_job_complete(&self, _id: JobId) -> Result<(), BackendError> {
            Ok(())
        }
        async fn mark_job_retryable(
            &self,
            _id: JobId,
            _next_scheduled_at: DateTime<Utc>,
            _error: ExecutionError,
        ) -> Result<(), BackendError> {
            Ok(())
        }
        async fn mark_job_discarded(
            &self,
            _id: JobId,
            _error: ExecutionError,
        ) -> Result<(), BackendError> {
            Ok(())
        }
        async fn mark_job_cancelled(
            &self,
            _id: JobId,
            _error: ExecutionError,
        ) -> Result<(), BackendError> {
            Ok(())
        }
        async fn mark_job_snoozed(
            &self,
            _id: JobId,
            _next_scheduled_at: DateTime<Utc>,
        ) -> Result<(), BackendError> {
            Ok(())
        }
        async fn prune_jobs(&self, _spec: &PruneSpec) -> Result<(), BackendError> {
            Ok(())
        }
        async fn rerun_job(&self, _id: JobId) -> Result<(), BackendError> {
            Ok(())
        }
        async fn update_job(&self, _job: Job) -> Result<(), BackendError> {
            Ok(())
        }
        async fn query<'a>(&self, _query: Query<'a>) -> Result<Vec<Job>, BackendError> {
            Ok(vec![])
        }
    }

    impl MockBackend {
        pub(crate) fn expect_enqueue_returning(&mut self, result: Result<JobId, BackendError>) {
            self.enqueue_return.lock().unwrap().push(result)
        }
    }
}
