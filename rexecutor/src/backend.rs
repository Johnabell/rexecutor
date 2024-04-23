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

    use chrono::TimeDelta;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::UnboundedReceiverStream;

    use crate::executor::Executor;

    use super::*;
    type StreamSender = mpsc::UnboundedSender<Result<Job, BackendError>>;

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

    #[derive(Clone, Debug)]
    pub struct MockBackend {
        enqueue_return: Arc<Mutex<Vec<Result<JobId, BackendError>>>>,
        mark_complete_return: Arc<Mutex<Vec<Result<(), BackendError>>>>,
        mark_retryable_return: Arc<Mutex<Vec<Result<(), BackendError>>>>,
        mark_discarded_return: Arc<Mutex<Vec<Result<(), BackendError>>>>,
        mark_cancelled_return: Arc<Mutex<Vec<Result<(), BackendError>>>>,
        mark_snoozed_return: Arc<Mutex<Vec<Result<(), BackendError>>>>,
        senders: Arc<Mutex<HashMap<ExecutorIdentifier, StreamSender>>>,
    }

    impl Drop for MockBackend {
        fn drop(&mut self) {
            let enqueue_returns = self.enqueue_return.lock().unwrap();
            assert!(
                enqueue_returns.is_empty(),
                "Expectation for call to enqueue not fulfilled:\n\
                - expected calls returning {enqueue_returns:?}"
            );
            let mark_complete_return = self.mark_complete_return.lock().unwrap();
            assert!(
                mark_complete_return.is_empty(),
                "Expectation for call to mark_job_complete not fulfilled:\n\
                - expected calls returning {mark_complete_return:?}"
            );
            let mark_retryable_return = self.mark_retryable_return.lock().unwrap();
            assert!(
                mark_retryable_return.is_empty(),
                "Expectation for call to mark_job_retryable not fulfilled:\n\
                - expected calls returning {mark_retryable_return:?}"
            );
            let mark_discarded_return = self.mark_discarded_return.lock().unwrap();
            assert!(
                mark_discarded_return.is_empty(),
                "Expectation for call to mark_job_discarded not fulfilled:\n\
                - expected calls returning {mark_discarded_return:?}"
            );
            let mark_cancelled_return = self.mark_cancelled_return.lock().unwrap();
            assert!(
                mark_cancelled_return.is_empty(),
                "Expectation for call to mark_job_cancelled not fulfilled:\n\
                - expected calls returning {mark_cancelled_return:?}"
            );
            let mark_snoozed_return = self.mark_snoozed_return.lock().unwrap();
            assert!(
                mark_snoozed_return.is_empty(),
                "Expectation for call to mark_job_snoozed not fulfilled:\n\
                - expected calls returning {mark_snoozed_return:?}"
            );
        }
    }

    impl Default for MockBackend {
        fn default() -> Self {
            Self {
                enqueue_return: Arc::new(vec![].into()),
                mark_complete_return: Arc::new(vec![].into()),
                mark_retryable_return: Arc::new(vec![].into()),
                mark_discarded_return: Arc::new(vec![].into()),
                mark_cancelled_return: Arc::new(vec![].into()),
                mark_snoozed_return: Arc::new(vec![].into()),
                senders: Arc::new(HashMap::default().into()),
            }
        }
    }

    #[async_trait]
    impl Backend for MockBackend {
        async fn subscribe_new_events(
            &self,
            executor_name: ExecutorIdentifier,
        ) -> Pin<Box<dyn Stream<Item = Result<Job, BackendError>> + Send>> {
            let (sender, receiver) = mpsc::unbounded_channel();
            self.senders.lock().unwrap().insert(executor_name, sender);
            Box::pin(UnboundedReceiverStream::from(receiver))
        }
        async fn enqueue<'a>(&self, _job: EnqueuableJob<'a>) -> Result<JobId, BackendError> {
            self.enqueue_return
                .lock()
                .unwrap()
                .pop()
                .expect("No matching expectation for enqueue")
        }
        async fn mark_job_complete(&self, _id: JobId) -> Result<(), BackendError> {
            self.mark_complete_return
                .lock()
                .unwrap()
                .pop()
                .expect("No matching expectation for mark_job_complete")
        }
        async fn mark_job_retryable(
            &self,
            _id: JobId,
            _next_scheduled_at: DateTime<Utc>,
            _error: ExecutionError,
        ) -> Result<(), BackendError> {
            self.mark_retryable_return
                .lock()
                .unwrap()
                .pop()
                .expect("No matching expectation for mark_job_retryable")
        }
        async fn mark_job_discarded(
            &self,
            _id: JobId,
            _error: ExecutionError,
        ) -> Result<(), BackendError> {
            self.mark_discarded_return
                .lock()
                .unwrap()
                .pop()
                .expect("No matching expectation for mark_job_discarded")
        }
        async fn mark_job_cancelled(
            &self,
            _id: JobId,
            _error: ExecutionError,
        ) -> Result<(), BackendError> {
            self.mark_cancelled_return
                .lock()
                .unwrap()
                .pop()
                .expect("No matching expectation for mark_job_discarded")
        }
        async fn mark_job_snoozed(
            &self,
            _id: JobId,
            _next_scheduled_at: DateTime<Utc>,
        ) -> Result<(), BackendError> {
            self.mark_snoozed_return
                .lock()
                .unwrap()
                .pop()
                .expect("No matching expectation for mark_job_snoozed")
        }
        async fn prune_jobs(&self, _spec: &PruneSpec) -> Result<(), BackendError> {
            unimplemented!()
        }
        async fn rerun_job(&self, _id: JobId) -> Result<(), BackendError> {
            unimplemented!()
        }
        async fn update_job(&self, _job: Job) -> Result<(), BackendError> {
            unimplemented!()
        }
        async fn query<'a>(&self, _query: Query<'a>) -> Result<Vec<Job>, BackendError> {
            unimplemented!()
        }
    }

    impl MockBackend {
        pub(crate) fn expect_enqueue_returning(&self, result: Result<JobId, BackendError>) {
            self.enqueue_return.lock().unwrap().push(result)
        }
        pub(crate) fn expect_mark_job_complete_returning(&self, result: Result<(), BackendError>) {
            self.mark_complete_return.lock().unwrap().push(result);
        }
        pub(crate) fn expect_mark_job_retryable_returning(&self, result: Result<(), BackendError>) {
            self.mark_retryable_return.lock().unwrap().push(result);
        }
        pub(crate) fn expect_mark_job_discarded_returning(&self, result: Result<(), BackendError>) {
            self.mark_discarded_return.lock().unwrap().push(result);
        }
        pub(crate) fn expect_mark_job_snoozed_returning(&self, result: Result<(), BackendError>) {
            self.mark_snoozed_return.lock().unwrap().push(result);
        }
        pub(crate) fn expect_mark_job_cancelled_returning(&self, result: Result<(), BackendError>) {
            self.mark_cancelled_return.lock().unwrap().push(result);
        }
        pub(crate) fn push_to_stream(
            &self,
            executor_name: ExecutorIdentifier,
            event: Result<Job, BackendError>,
        ) {
            self.senders
                .lock()
                .unwrap()
                .get(&executor_name)
                .map(|sender| sender.send(event));
        }
    }
}
