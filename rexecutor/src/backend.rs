use std::{ops::Deref, pin::Pin};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;
use serde::Serialize;
use thiserror::Error;
#[cfg(test)]
use tokio::sync::mpsc;
#[cfg(test)]
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::{
    executor::ExecutorIdentifier,
    job::{self, uniqueness_criteria::UniquenessCriteria, ErrorType, JobError, JobId, JobStatus},
    pruner::PruneSpec,
};
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait Backend {
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

#[async_trait]
impl<T, S> Backend for S
where
    T: Backend,
    S: Deref<Target = T> + Sync,
{
    async fn subscribe_new_events(
        &self,
        executor_identifier: ExecutorIdentifier,
    ) -> Pin<Box<dyn Stream<Item = Result<Job, BackendError>> + Send>> {
        self.deref().subscribe_new_events(executor_identifier).await
    }
    async fn enqueue<'a>(&self, job: EnqueuableJob<'a>) -> Result<JobId, BackendError> {
        self.deref().enqueue(job).await
    }
    async fn mark_job_complete(&self, id: JobId) -> Result<(), BackendError> {
        self.deref().mark_job_complete(id).await
    }
    async fn mark_job_retryable(
        &self,
        id: JobId,
        next_scheduled_at: DateTime<Utc>,
        error: ExecutionError,
    ) -> Result<(), BackendError> {
        self.deref()
            .mark_job_retryable(id, next_scheduled_at, error)
            .await
    }
    async fn mark_job_discarded(
        &self,
        id: JobId,
        error: ExecutionError,
    ) -> Result<(), BackendError> {
        self.deref().mark_job_discarded(id, error).await
    }
    async fn mark_job_cancelled(
        &self,
        id: JobId,
        error: ExecutionError,
    ) -> Result<(), BackendError> {
        self.deref().mark_job_cancelled(id, error).await
    }
    async fn mark_job_snoozed(
        &self,
        id: JobId,
        next_scheduled_at: DateTime<Utc>,
    ) -> Result<(), BackendError> {
        self.deref().mark_job_snoozed(id, next_scheduled_at).await
    }
    async fn prune_jobs(&self, prune_spec: &PruneSpec) -> Result<(), BackendError> {
        self.deref().prune_jobs(prune_spec).await
    }
    async fn rerun_job(&self, job_id: JobId) -> Result<(), BackendError> {
        self.deref().rerun_job(job_id).await
    }
    async fn update_job(&self, job: Job) -> Result<(), BackendError> {
        self.deref().update_job(job).await
    }
    async fn query<'a>(&self, query: Query<'a>) -> Result<Vec<Job>, BackendError> {
        self.deref().query(query).await
    }
}

#[cfg(test)]
impl MockBackend {
    pub(crate) fn expect_subscribe_to_new_events_with_stream(
        &mut self,
    ) -> mpsc::UnboundedSender<Result<Job, BackendError>> {
        let (sender, receiver) = mpsc::unbounded_channel();
        let stream = Box::pin(UnboundedReceiverStream::from(receiver));
        self.expect_subscribe_new_events().return_once(|_| stream);

        sender
    }
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

#[derive(Debug, Clone, Eq, PartialEq)]
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
    use super::*;
    use crate::executor::Executor;
    use chrono::TimeDelta;

    impl Job {
        pub(crate) fn mock_job<T: Executor>() -> Self {
            let now = Utc::now();
            Self {
                id: 0,
                executor: T::NAME.to_owned(),
                status: JobStatus::Scheduled,
                data: serde_json::Value::Null,
                metadata: serde_json::Value::Null,
                attempt: 0,
                max_attempts: 3,
                priority: 0,
                tags: vec![],
                errors: vec![],
                inserted_at: now,
                scheduled_at: now + TimeDelta::hours(2),
                attempted_at: None,
                completed_at: None,
                cancelled_at: None,
                discarded_at: None,
            }
        }
        pub(crate) fn raw_job() -> Self {
            let now = Utc::now();
            Self {
                id: 0,
                executor: "executor".to_owned(),
                status: JobStatus::Scheduled,
                data: serde_json::Value::Null,
                metadata: serde_json::Value::Null,
                attempt: 0,
                max_attempts: 3,
                priority: 0,
                tags: vec![],
                errors: vec![],
                inserted_at: now,
                scheduled_at: now + TimeDelta::hours(2),
                attempted_at: None,
                completed_at: None,
                cancelled_at: None,
                discarded_at: None,
            }
        }
    }

    impl Job {
        pub(crate) fn with_data<D>(self, data: D) -> Self
        where
            D: Serialize,
        {
            Self {
                data: serde_json::to_value(data).unwrap(),
                ..self
            }
        }

        pub(crate) fn with_raw_metadata(self, metadata: serde_json::Value) -> Self {
            Self { metadata, ..self }
        }

        pub(crate) fn with_raw_data(self, data: serde_json::Value) -> Self {
            Self { data, ..self }
        }

        pub(crate) fn with_max_attempts(self, max_attempts: i32) -> Self {
            Self {
                max_attempts,
                ..self
            }
        }

        pub(crate) fn with_attempt(self, attempt: i32) -> Self {
            Self { attempt, ..self }
        }
    }
}
