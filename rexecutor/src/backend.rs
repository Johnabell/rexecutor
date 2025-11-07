//! The definition of the rexecutor backend.
//!
//! Rexecutor has been designed to have pluggable backends. This module provides the types and
//! definitions required to implement a backend for rexecutor.
//!
//! Critically, a backend needs to implement the [`Backend`] trait.
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

pub mod memory;
pub(crate) mod queryable;
pub mod testing;

/// The trait which need to be implemented to create a backend for rexecutor.
///
/// Note: the decision was taken to have a subscribe to ready jobs function to enable implementers
/// to make best use of their backend technology such as pgnotify or other DB level subscriptions.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait Backend {
    /// Returns a stream of the jobs ready to be executed.
    ///
    /// Note: this should avoid giving the same jobs to multiple instances of rexecutor. This is
    /// important for the correctness of the library when running in a cluster a locking mechanism
    /// should be used to avoid multiple instances processing the same job.
    async fn subscribe_ready_jobs(
        &self,
        executor_identifier: ExecutorIdentifier,
    ) -> Pin<Box<dyn Stream<Item = Result<Job, BackendError>> + Send>>;

    /// Enqueue a job to the backend.
    ///
    /// Note: if this is scheduled for now or in the past this should immediately be passed to one
    /// of the subscribers.
    async fn enqueue<'a>(&self, job: EnqueuableJob<'a>) -> Result<JobId, BackendError>;

    /// Mark a job as having been completed.
    async fn mark_job_complete(&self, id: JobId) -> Result<(), BackendError>;

    /// Mark a job as retryable and schedule to run it again at the given instance.
    ///
    /// Note this should record the error as part of the jobs errors array.
    ///
    /// When the job next runs its attempt count should have been incremented.
    async fn mark_job_retryable(
        &self,
        id: JobId,
        next_scheduled_at: DateTime<Utc>,
        error: ExecutionError,
    ) -> Result<(), BackendError>;

    /// Mark a job as discarded (this is the state where the job has failed to complete
    /// successfully after reaching its max_attempts.
    ///
    /// Note this should record the error as part of the jobs errors array.
    async fn mark_job_discarded(
        &self,
        id: JobId,
        error: ExecutionError,
    ) -> Result<(), BackendError>;

    /// Mark a job as cancelled.
    ///
    /// Note this should record the error as part of the jobs errors array.
    async fn mark_job_cancelled(
        &self,
        id: JobId,
        error: ExecutionError,
    ) -> Result<(), BackendError>;

    /// Mark a job as snoozed.
    ///
    /// Note: this should not increment the attempt count.
    async fn mark_job_snoozed(
        &self,
        id: JobId,
        next_scheduled_at: DateTime<Utc>,
    ) -> Result<(), BackendError>;

    /// Remove the jobs according to the provided [`PruneSpec`].
    async fn prune_jobs(&self, prune_spec: &PruneSpec) -> Result<(), BackendError>;

    /// Rerun the given job.
    ///
    /// This should work for jobs that have completed, been discarded, or cancelled.
    async fn rerun_job(&self, job_id: JobId) -> Result<(), BackendError>;

    /// Update the job with the updated details in the [`Job`].
    async fn update_job(&self, job: Job) -> Result<(), BackendError>;

    /// Query for jobs according to the specification in the [`Query`].
    async fn query<'a>(&self, query: Query<'a>) -> Result<Vec<Job>, BackendError>;
}

#[async_trait]
impl<T, S> Backend for S
where
    T: Backend,
    S: Deref<Target = T> + Sync,
{
    async fn subscribe_ready_jobs(
        &self,
        executor_identifier: ExecutorIdentifier,
    ) -> Pin<Box<dyn Stream<Item = Result<Job, BackendError>> + Send>> {
        self.deref().subscribe_ready_jobs(executor_identifier).await
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
    pub(crate) fn expect_subscribe_to_ready_jobs_with_stream(
        &mut self,
    ) -> mpsc::UnboundedSender<Result<Job, BackendError>> {
        let (sender, receiver) = mpsc::unbounded_channel();
        let stream = Box::pin(UnboundedReceiverStream::from(receiver));
        self.expect_subscribe_ready_jobs().return_once(|_| stream);

        sender
    }
}

/// A record of an error that was returned from a job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionError {
    /// The type of error that occurred.
    pub error_type: ErrorType,
    /// The details of the error.
    pub message: String,
}

/// The details required to enqueue a job.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EnqueuableJob<'a> {
    /// The name of the executor which is responsible for running this job.
    ///
    /// Determined via [`crate::executor::Executor::NAME`].
    pub executor: String,
    /// The encoded data of this job.
    pub data: serde_json::Value,
    /// The encoded metadata of this job.
    pub metadata: serde_json::Value,
    /// The maximum number of times to attempt this job before discarding it.
    pub max_attempts: u16,
    /// The timestamp when the job should next be executed.
    pub scheduled_at: DateTime<Utc>,
    /// The tags associated with this job.
    pub tags: Vec<String>,
    /// The priority of the job.
    ///
    /// The lower the number the higher the priority, i.e. the highest priority jobs are those with
    /// priority zero.
    ///
    /// The priority is used to determine which jobs should be ran first when many jobs are
    /// scheduled to run at the same time. The priority is applied to all jobs for the same
    /// executor and not used to prioritise between different executors. Instead all executors will
    /// attempt to execute their highest priority jobs simultaneously.
    pub priority: u16,
    /// The uniqueness criteria used to determine whether a given job should be considered a
    /// duplicate.
    ///
    /// Additionally this includes details of what action should be taken when a conflict is
    /// identified. For more details see [`UniquenessCriteria`].
    pub uniqueness_criteria: Option<UniquenessCriteria<'a>>,
}

/// The backend representation of the job.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Job {
    /// The id of the job.
    pub id: i32,
    /// The current status of the job.
    pub status: JobStatus,
    /// The name of the executor which is responsible for running this job.
    ///
    /// Determined via [`crate::executor::Executor::NAME`].
    pub executor: String,
    /// The encoded data for running this job.
    pub data: serde_json::Value,
    /// The encoded metadata for this job.
    pub metadata: serde_json::Value,
    /// The current attempt of the job.
    pub attempt: i32,
    /// The maximum number of times to attempt this job before discarding it.
    pub max_attempts: i32,
    /// The priority of the job.
    ///
    /// The lower the number the higher the priority, i.e. the highest priority jobs are those with
    /// priority zero.
    ///
    /// The priority is used to determine which jobs should be ran first when many jobs are
    /// scheduled to run at the same time. The priority is applied to all jobs for the same
    /// executor and not used to prioritise between different executors. Instead all executors will
    /// attempt to execute their highest priority jobs simultaneously.
    pub priority: i32,
    /// The tags associated with this job.
    pub tags: Vec<String>,
    /// Any errors that have occurred in previous attempts to run this job.
    pub errors: Vec<JobError>,
    /// The timestamp when this job was inserted.
    pub inserted_at: DateTime<Utc>,
    /// The timestamp when the job should next be executed.
    pub scheduled_at: DateTime<Utc>,
    /// The timestamp of the last attempt to execute the job.
    pub attempted_at: Option<DateTime<Utc>>,
    /// The timestamp when the job completed successfully.
    pub completed_at: Option<DateTime<Utc>>,
    /// The timestamp when the job was cancelled.
    pub cancelled_at: Option<DateTime<Utc>>,
    /// The timestamp when the job was discarded.
    pub discarded_at: Option<DateTime<Utc>>,
}

/// Recursive data structure for defining job queries for use with the backend.
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Query<'a> {
    /// Represents the negation of the inner query.
    Not(Box<Query<'a>>),
    /// Combines queries with logical `and`.
    And(Vec<Query<'a>>),
    /// Combines queries with logical `or`.
    Or(Vec<Query<'a>>),
    /// Query based on the name of the executor.
    ExecutorEqual(&'a str),
    /// Query based on equality with the job's encoded data.
    DataEquals(serde_json::Value),
    /// Query based on equality with the job's encoded metadata.
    MetadataEquals(serde_json::Value),
    /// Query based on equality with the job's id.
    IdEquals(JobId),
    /// Query for jobs where matching one of the contained ids.
    IdIn(&'a [JobId]),
    /// Query based on equality with the given [`JobStatus`].
    StatusEqual(JobStatus),
    /// Query for jobs where all of the job's tags are in the provided set of tags.
    TagsAllOf(&'a [&'a str]),
    /// Query for jobs where one of the job's tags are in the provided set of tags.
    TagsOneOf(&'a [&'a str]),
    /// Query for jobs where the job's `scheduled_at` field is before the given [`DateTime`].
    ScheduledAtBefore(DateTime<Utc>),
    /// Query for jobs where the job's `scheduled_at` field is after the given [`DateTime`].
    ScheduledAtAfter(DateTime<Utc>),
    /// Query for jobs where the job's `scheduled_at` field is equal to the given [`DateTime`].
    ScheduledAtEqual(DateTime<Utc>),
}

impl<'a> Query<'a> {
    /// Updates this query to only apply to jobs for the current executor.
    pub(crate) fn for_executor(mut self, executor: &'static str) -> Self {
        if let Self::And(ref mut queries) = self {
            queries.push(Query::ExecutorEqual(executor));
            self
        } else {
            Self::And(vec![Query::ExecutorEqual(executor), self])
        }
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

/// The errors that could be returned from the backend.
#[derive(Debug, Error)]
pub enum BackendError {
    /// A serialization/deserialization type error.
    #[error("Error encoding or decoding data")]
    EncodeDecode(#[from] serde_json::Error),
    /// The backend in an inconsistent state.
    #[error("System in bad state")]
    BadState,
    /// No jobs was found matching the criteria provided.
    #[error("Job not found: {0}")]
    JobNotFound(JobId),
    /// There was an error doing IO with the backend
    #[error(transparent)]
    Io(std::io::Error),
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::executor::{test::SimpleExecutor, Executor};
    use assert_matches::assert_matches;
    use chrono::TimeDelta;
    use std::sync::Arc;

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

        pub(crate) fn with_tags(self, tags: Vec<String>) -> Self {
            Self { tags, ..self }
        }

        pub(crate) fn with_scheduled_at(self, scheduled_at: DateTime<Utc>) -> Self {
            Self {
                scheduled_at,
                ..self
            }
        }

        pub(crate) fn with_status(self, status: JobStatus) -> Self {
            Self { status, ..self }
        }
    }

    #[test]
    fn query_try_from() {
        let date = Utc::now();
        let data = "data".to_owned();
        let metadata = "metadata".to_owned();
        let job_id = 42.into();
        let job_ids = [53.into(), 56.into()];
        let query: Query = job::query::Query::Not(Box::new(job::query::Query::And(vec![
            job::query::Query::DataEquals(&data),
            job::query::Query::MetadataEquals(&metadata),
            job::query::Query::IdEquals(job_id),
            job::query::Query::IdIn(&job_ids),
            job::query::Query::StatusEqual(JobStatus::Complete),
            job::query::Query::TagsAllOf(&["one", "two"]),
            job::query::Query::TagsOneOf(&["three", "four"]),
            job::query::Query::ScheduledAtBefore(date),
            job::query::Query::Or(vec![
                job::query::Query::ScheduledAtAfter(date),
                job::query::Query::ScheduledAtEqual(date),
            ]),
        ])))
        .try_into()
        .unwrap();

        assert_matches!(query, Query::Not(inner) => {
            assert_matches!(*inner, Query::And(queries) => {
                assert_matches!(&queries[..], &[
                    Query::DataEquals(ref encoded_data),
                    Query::MetadataEquals(ref encoded_metadata),
                    Query::IdEquals(jid),
                    Query::IdIn(jids),
                    Query::StatusEqual(JobStatus::Complete),
                    Query::TagsAllOf(&["one", "two"]),
                    Query::TagsOneOf(&["three", "four"]),
                    Query::ScheduledAtBefore(scheduled_before),
                    Query::Or(ref inner),
                ] => {
                    assert_eq!(encoded_data, &serde_json::Value::String(data.clone()));
                    assert_eq!(encoded_metadata, &serde_json::Value::String(metadata.clone()));
                    assert_eq!(jid, job_id);
                    assert_eq!(jids, job_ids);
                    assert_eq!(scheduled_before, date);
                    assert_matches!(&inner[..], [
                        Query::ScheduledAtAfter(scheduled_after),
                        Query::ScheduledAtEqual(scheduled_at),
                    ] => {
                        assert_eq!(scheduled_after, &date);
                        assert_eq!(scheduled_at, &date);
                    });
                });
            });
        });
    }

    #[test]
    fn query_by_executor_already_and() {
        let query = Query::And(vec![
            Query::StatusEqual(JobStatus::Complete),
            Query::TagsAllOf(&["one", "two"]),
        ])
        .for_executor("executor");

        assert_matches!(query, Query::And(queries) => {
            assert_matches!(&queries[..], &[
                Query::StatusEqual(JobStatus::Complete),
                Query::TagsAllOf(&["one", "two"]),
                Query::ExecutorEqual("executor"),
            ]);
        });
    }

    #[test]
    fn query_by_executor_not_and() {
        let query = Query::Or(vec![
            Query::StatusEqual(JobStatus::Complete),
            Query::TagsAllOf(&["one", "two"]),
        ])
        .for_executor("executor");

        assert_matches!(query, Query::And(queries) => {
            assert_matches!(&queries[..], [
                Query::ExecutorEqual("executor"),
                Query::Or(inner),
            ] => {
                assert_matches!(&inner[..], &[
                    Query::StatusEqual(JobStatus::Complete),
                    Query::TagsAllOf(&["one", "two"]),
                ]);
            });
        });
    }

    #[tokio::test]
    async fn smart_pointer_backend() {
        let mut backend = MockBackend::default();
        let job = Job::mock_job::<SimpleExecutor>();
        backend.expect_rerun_job().return_once(|_| Ok(()));
        backend.expect_update_job().return_once(|_| Ok(()));
        backend.expect_query().return_once(|_| Ok(vec![]));

        let smart_pointer_backend = Arc::new(backend);

        assert!(smart_pointer_backend.rerun_job(42.into()).await.is_ok());
        assert!(smart_pointer_backend.update_job(job).await.is_ok());
        assert!(smart_pointer_backend
            .query(Query::ExecutorEqual(""))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn box_backend() {
        let mut backend = MockBackend::default();
        let job = Job::mock_job::<SimpleExecutor>();
        backend.expect_rerun_job().return_once(|_| Ok(()));
        backend.expect_update_job().return_once(|_| Ok(()));
        backend.expect_query().return_once(|_| Ok(vec![]));

        let smart_pointer_backend = Box::new(backend);

        assert!(smart_pointer_backend.rerun_job(42.into()).await.is_ok());
        assert!(smart_pointer_backend.update_job(job).await.is_ok());
        assert!(smart_pointer_backend
            .query(Query::ExecutorEqual(""))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn reference_backend() {
        let mut backend = MockBackend::default();
        let job = Job::mock_job::<SimpleExecutor>();
        backend.expect_rerun_job().return_once(|_| Ok(()));
        backend.expect_update_job().return_once(|_| Ok(()));
        backend.expect_query().return_once(|_| Ok(vec![]));

        let smart_pointer_backend = &backend;

        assert!(smart_pointer_backend.rerun_job(42.into()).await.is_ok());
        assert!(smart_pointer_backend.update_job(job).await.is_ok());
        assert!(smart_pointer_backend
            .query(Query::ExecutorEqual(""))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn mut_reference_backend() {
        let mut backend = MockBackend::default();
        let job = Job::mock_job::<SimpleExecutor>();
        backend.expect_rerun_job().return_once(|_| Ok(()));
        backend.expect_update_job().return_once(|_| Ok(()));
        backend.expect_query().return_once(|_| Ok(vec![]));

        let smart_pointer_backend = &mut backend;

        assert!(smart_pointer_backend.rerun_job(42.into()).await.is_ok());
        assert!(smart_pointer_backend.update_job(job).await.is_ok());
        assert!(smart_pointer_backend
            .query(Query::ExecutorEqual(""))
            .await
            .is_ok());
    }
}
