//! Provides an in memory implementation of [`Backend`].
//!
//! Currently this is provided for testing purposes and not designed for use in a production system.
//!
//! It is not optimized instead is designed to be a correct implementation for use in a test setup.
use std::{
    collections::HashMap,
    ops::Sub,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicI32, Ordering},
        Arc, RwLock,
    },
};

use crate::{
    executor::ExecutorIdentifier,
    job::{
        uniqueness_criteria::{Resolution, UniquenessCriteria},
        JobError, JobId, JobStatus,
    },
    pruner::{PruneBy, PruneSpec},
};
use async_stream::stream;
use async_trait::async_trait;
use futures::Stream;
use tokio::sync::mpsc;

use super::{
    queryable::Queryable, Backend, BackendError, EnqueuableJob, ExecutionError, Job, Query,
};
use chrono::{DateTime, TimeDelta, Utc};

struct ReadyJobStream {
    backend: InMemoryBackend,
    executor_identifier: ExecutorIdentifier,
    receiver: tokio::sync::mpsc::UnboundedReceiver<DateTime<Utc>>,
}

impl ReadyJobStream {
    const DEFAULT_DELAY: std::time::Duration = std::time::Duration::from_secs(30);
    const DELTA: std::time::Duration = std::time::Duration::from_millis(15);

    pub async fn next(&mut self) -> Result<Job, BackendError> {
        loop {
            let delay = match self
                .backend
                .next_available_job_scheduled_at_for_executor(self.executor_identifier.as_str())
                .await?
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
                    .load_job_mark_as_executing_for_executor(self.executor_identifier.as_str())?
                {
                    return Ok(job);
                }
            }
            tokio::select! {
                _ = self.receiver.recv() => { },
                _ = tokio::time::sleep(delay) => { },

            }
        }
    }
}

type Subscriber = mpsc::UnboundedSender<DateTime<Utc>>;

/// An in memory implementation of [`Backend`].
///
/// It is provided as a correct (but not optimized) implementation primarily for use in testing
/// circumstances.
///
/// **This is not designed for use in a production systems.**
#[derive(Clone, Default)]
pub struct InMemoryBackend {
    jobs: Arc<RwLock<Vec<KeyedJob>>>,
    id_counter: Arc<AtomicI32>,
    subscribers: Arc<RwLock<HashMap<&'static str, Vec<Subscriber>>>>,
    paused: Arc<AtomicBool>,
}

#[derive(Clone)]
pub(super) struct KeyedJob {
    job: Job,
    pub(super) key: Option<i64>,
}

impl KeyedJob {
    fn job(&self) -> Job {
        self.job.clone()
    }
}

impl std::ops::Deref for KeyedJob {
    type Target = Job;

    fn deref(&self) -> &Self::Target {
        &self.job
    }
}

impl std::ops::DerefMut for KeyedJob {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.job
    }
}

impl InMemoryBackend {
    /// Creates a new instance of [`InMemoryBackend`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Run the backend in paused mode where streams will not be woken up when jobs are inserted or
    /// updated.
    ///
    /// If you would like to then run jobs later in the test you can call
    /// [`InMemoryBackend::notify_all`].
    pub fn paused(self) -> Self {
        self.paused.store(true, Ordering::Relaxed);
        self
    }

    /// Wake up all the stream subscribers to continue execution.
    ///
    /// Particularly helpful when running the backend in paused mode.
    pub fn notify_all(&self) -> Result<(), BackendError> {
        let scheduled_at = Utc::now();
        self.subscribers
            .read()
            .map_err(|_| BackendError::BadState)?
            .values()
            .for_each(|subscriber| {
                subscriber.iter().for_each(|sender| {
                    let _ = sender.send(scheduled_at);
                })
            });
        Ok(())
    }

    /// A helper function for getting a job from an ID.
    ///
    /// This could be implemented as using [`Query`] as follows:
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    /// # use rexecutor::backend::{Backend, Query, EnqueuableJob, memory::InMemoryBackend};
    /// # use chrono::Utc;
    /// let backend = InMemoryBackend::default();
    ///
    /// let job = EnqueuableJob {
    ///     executor: "executor".to_owned(),
    ///     data: serde_json::Value::String("data".to_owned()),
    ///     metadata: serde_json::Value::String("metadata".to_owned()),
    ///     max_attempts: 5,
    ///     scheduled_at: Utc::now(),
    ///     tags: Default::default(),
    ///     priority: 0,
    ///     uniqueness_criteria: None,
    /// };
    ///
    /// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
    /// let job_id = backend.enqueue(job).await.unwrap();
    ///
    /// assert_eq!(
    ///     backend.get_job(job_id),
    ///     backend.query(Query::IdEquals(job_id)).await.unwrap().into_iter().next()
    /// );
    /// # });
    /// ```
    pub fn get_job(&self, id: JobId) -> Option<Job> {
        let jobs = self.jobs.read().unwrap();
        jobs.get(i32::from(id) as usize).map(KeyedJob::job)
    }

    async fn next_available_job_scheduled_at_for_executor(
        &self,
        as_str: &str,
    ) -> Result<Option<DateTime<Utc>>, BackendError> {
        self.query(Query::And(vec![
            Query::ExecutorEqual(as_str),
            Query::Or(vec![
                Query::StatusEqual(JobStatus::Scheduled),
                Query::StatusEqual(JobStatus::Retryable),
            ]),
        ]))
        .await
        .map(|jobs| jobs.into_iter().map(|job| job.scheduled_at).min())
    }

    fn load_job_mark_as_executing_for_executor(
        &self,
        as_str: &str,
    ) -> Result<Option<Job>, BackendError> {
        let mut jobs = self.jobs.write().unwrap();
        let mut jobs = jobs
            .iter_mut()
            .filter(|job| {
                job.executor == as_str
                    && job.scheduled_at - Utc::now() < TimeDelta::milliseconds(100)
                    && (job.status == JobStatus::Retryable || job.status == JobStatus::Scheduled)
            })
            .collect::<Vec<_>>();
        jobs.sort_by(|a, b| {
            a.scheduled_at
                .cmp(&b.scheduled_at)
                .then(a.priority.cmp(&b.priority))
        });
        Ok(jobs.first_mut().map(|job| {
            job.mark_job_executing();
            job.job.to_owned()
        }))
    }

    fn notify_subscribers(
        &self,
        executor: &str,
        scheduled_at: DateTime<Utc>,
    ) -> Result<(), BackendError> {
        if !self.paused.load(Ordering::Relaxed) {
            let _ = self
                .subscribers
                .read()
                .map_err(|_| BackendError::BadState)?
                .get(executor)
                .map(|subscriber| {
                    subscriber.iter().for_each(|sender| {
                        let _ = sender.send(scheduled_at);
                    })
                });
        }
        Ok(())
    }

    fn matching_jobs(&self, queryable: &impl Queryable) -> Result<Vec<KeyedJob>, BackendError> {
        Ok(self
            .jobs
            .read()
            .map_err(|_| BackendError::BadState)?
            .iter()
            .filter(|job| queryable.matches(job))
            .cloned()
            .collect())
    }

    fn insert_new_job(&self, job: EnqueuableJob<'_>) -> Result<JobId, BackendError> {
        let executor = job.executor.clone();
        let scheduled_at = job.scheduled_at;
        let id = self.id_counter.fetch_add(1, Ordering::SeqCst);

        self.jobs
            .write()
            .map_err(|_| BackendError::BadState)?
            .push(job.into_job(id));

        self.notify_subscribers(executor.as_str(), scheduled_at)?;

        Ok(id.into())
    }

    async fn insert_unique_job(&self, job: EnqueuableJob<'_>) -> Result<JobId, BackendError> {
        let matching_jobs = self.matching_jobs(&job)?;
        if matching_jobs.is_empty() {
            self.insert_new_job(job)
        } else {
            // TODO: what is the correct behaviour when there are multiple matching jobs
            let mut matching_job = matching_jobs.into_iter().next().unwrap();
            matching_job.replace(job);
            let id = matching_job.id;
            self.update_job(matching_job.job).await?;
            Ok(id.into())
        }
    }
}

impl<'a> EnqueuableJob<'a> {
    pub(super) fn into_job(self, id: i32) -> KeyedJob {
        KeyedJob {
            key: self.uniqueness_criteria.and_then(|uq| uq.key),
            job: Job {
                id,
                status: JobStatus::Scheduled,
                executor: self.executor,
                data: self.data,
                metadata: self.metadata,
                attempt: 0,
                max_attempts: self.max_attempts as i32,
                priority: self.priority as i32,
                tags: self.tags,
                errors: vec![],
                inserted_at: Utc::now(),
                scheduled_at: self.scheduled_at,
                attempted_at: None,
                completed_at: None,
                cancelled_at: None,
                discarded_at: None,
            },
        }
    }
}

impl ExecutionError {
    fn into_job_error(self, attempt: u16) -> JobError {
        JobError {
            attempt,
            error_type: self.error_type,
            details: self.message,
            recorded_at: Utc::now(),
        }
    }
}

impl Job {
    fn mark_job_executing(&mut self) {
        self.attempted_at = Some(Utc::now());
        self.attempt += 1;
        self.status = JobStatus::Executing;
    }

    fn mark_job_complete(&mut self) {
        self.completed_at = Some(Utc::now());
        self.status = JobStatus::Complete;
    }

    fn mark_job_snoozed(&mut self, scheduled_at: DateTime<Utc>) {
        self.scheduled_at = scheduled_at;
        self.status = match self.attempt {
            1 => JobStatus::Scheduled,
            _ => JobStatus::Retryable,
        };
        self.attempt -= 1;
    }

    fn mark_job_retryable(&mut self, scheduled_at: DateTime<Utc>, error: ExecutionError) {
        self.scheduled_at = scheduled_at;
        self.errors.push(error.into_job_error(self.attempt as u16));
        self.status = JobStatus::Retryable;
    }

    fn mark_job_discarded(&mut self, error: ExecutionError) {
        self.discarded_at = Some(Utc::now());
        self.errors.push(error.into_job_error(self.attempt as u16));
        self.status = JobStatus::Discarded;
    }

    fn mark_job_cancelled(&mut self, error: ExecutionError) {
        self.cancelled_at = Some(Utc::now());
        self.errors.push(error.into_job_error(self.attempt as u16));
        self.status = JobStatus::Cancelled;
    }

    fn mark_job_rerunable(&mut self) {
        self.status = match self.attempt {
            1 => JobStatus::Scheduled,
            _ => JobStatus::Retryable,
        };
        self.max_attempts += 1;
        self.scheduled_at = Utc::now();
        self.completed_at = None;
        self.discarded_at = None;
        self.cancelled_at = None;
    }
    fn replace(&mut self, job: EnqueuableJob<'_>) {
        if let Some(UniquenessCriteria {
            on_conflict: Resolution::Replace(ref replace),
            ..
        }) = job.uniqueness_criteria
        {
            if replace.for_statuses.contains(&self.status) {
                if replace.scheduled_at {
                    self.scheduled_at = job.scheduled_at;
                }
                if replace.data {
                    self.data = job.data;
                }
                if replace.metadata {
                    self.metadata = job.metadata;
                }
                if replace.priority {
                    self.priority = job.priority.into();
                }
                if replace.max_attempts {
                    self.max_attempts = job.max_attempts.into();
                }
            }
        }
    }
}

#[async_trait]
impl Backend for InMemoryBackend {
    async fn subscribe_ready_jobs(
        &self,
        executor_identifier: ExecutorIdentifier,
    ) -> Pin<Box<dyn Stream<Item = Result<Job, BackendError>> + Send>> {
        let (sender, receiver) = mpsc::unbounded_channel();
        self.subscribers
            .write()
            .expect("Should we make this a fallible function")
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
        match job.uniqueness_criteria {
            None => self.insert_new_job(job),
            Some(_) => self.insert_unique_job(job).await,
        }
    }
    async fn mark_job_complete(&self, id: JobId) -> Result<(), BackendError> {
        let mut jobs = self.jobs.write().map_err(|_| BackendError::BadState)?;
        match jobs.iter_mut().find(|job| job.id == i32::from(id)) {
            None => Err(BackendError::JobNotFound(id)),
            Some(job) => {
                job.mark_job_complete();
                Ok(())
            }
        }
    }
    async fn mark_job_retryable(
        &self,
        id: JobId,
        next_scheduled_at: DateTime<Utc>,
        error: ExecutionError,
    ) -> Result<(), BackendError> {
        let mut jobs = self.jobs.write().map_err(|_| BackendError::BadState)?;
        match jobs.iter_mut().find(|job| job.id == i32::from(id)) {
            None => Err(BackendError::JobNotFound(id)),
            Some(job) => {
                job.mark_job_retryable(next_scheduled_at, error);
                self.notify_subscribers(job.executor.as_str(), job.scheduled_at)?;
                Ok(())
            }
        }
    }
    async fn mark_job_discarded(
        &self,
        id: JobId,
        error: ExecutionError,
    ) -> Result<(), BackendError> {
        let mut jobs = self.jobs.write().map_err(|_| BackendError::BadState)?;
        match jobs.iter_mut().find(|job| job.id == i32::from(id)) {
            None => Err(BackendError::JobNotFound(id)),
            Some(job) => {
                job.mark_job_discarded(error);
                Ok(())
            }
        }
    }
    async fn mark_job_cancelled(
        &self,
        id: JobId,
        error: ExecutionError,
    ) -> Result<(), BackendError> {
        let mut jobs = self.jobs.write().map_err(|_| BackendError::BadState)?;
        match jobs.iter_mut().find(|job| job.id == i32::from(id)) {
            None => Err(BackendError::JobNotFound(id)),
            Some(job) => {
                job.mark_job_cancelled(error);
                Ok(())
            }
        }
    }
    async fn mark_job_snoozed(
        &self,
        id: JobId,
        next_scheduled_at: DateTime<Utc>,
    ) -> Result<(), BackendError> {
        let mut jobs = self.jobs.write().map_err(|_| BackendError::BadState)?;
        match jobs.iter_mut().find(|job| job.id == i32::from(id)) {
            None => Err(BackendError::JobNotFound(id)),
            Some(job) => {
                job.mark_job_snoozed(next_scheduled_at);
                self.notify_subscribers(job.executor.as_str(), job.scheduled_at)?;
                Ok(())
            }
        }
    }
    async fn prune_jobs(&self, prune_spec: &PruneSpec) -> Result<(), BackendError> {
        let now = Utc::now();
        let mut jobs = self.jobs.write().map_err(|_| BackendError::BadState)?;
        match prune_spec.prune_by {
            PruneBy::MaxAge(delta) => {
                let min_scheduled_at = now - delta;
                let max_scheduled_at = now + delta;
                jobs.retain(|job| {
                    !prune_spec.matches(job)
                        || (job.scheduled_at < max_scheduled_at
                            && job.scheduled_at > min_scheduled_at)
                });
            }
            PruneBy::MaxLength(length) => {
                let mut count = 0;
                jobs.retain(|job| {
                    if prune_spec.matches(job) {
                        count += 1;
                        count > length
                    } else {
                        true
                    }
                });
            }
        };
        Ok(())
    }
    async fn rerun_job(&self, id: JobId) -> Result<(), BackendError> {
        let mut jobs = self.jobs.write().map_err(|_| BackendError::BadState)?;
        match jobs.iter_mut().find(|job| job.id == i32::from(id)) {
            None => Err(BackendError::JobNotFound(id)),
            Some(job) => {
                job.mark_job_rerunable();
                self.notify_subscribers(job.executor.as_str(), job.scheduled_at)?;
                Ok(())
            }
        }
    }
    async fn update_job(&self, job: Job) -> Result<(), BackendError> {
        let mut jobs = self.jobs.write().map_err(|_| BackendError::BadState)?;
        match jobs.iter_mut().find(|j| j.id == job.id) {
            None => Err(BackendError::JobNotFound(job.id.into())),
            Some(j) => {
                j.job = job;
                self.notify_subscribers(j.executor.as_str(), j.scheduled_at)?;
                Ok(())
            }
        }
    }
    async fn query<'a>(&self, query: Query<'a>) -> Result<Vec<Job>, BackendError> {
        Ok(self
            .matching_jobs(&query)?
            .into_iter()
            .map(|job| job.job)
            .collect())
    }
}

#[cfg(test)]
pub(super) mod test {
    use std::{ops::Add, time::Duration};

    use crate::{
        job::{uniqueness_criteria::Replace, ErrorType},
        pruner::Spec,
    };
    use futures::StreamExt;

    use super::*;
    use assert_matches::assert_matches;

    impl KeyedJob {
        pub fn raw_job() -> Self {
            Self {
                job: Job::raw_job(),
                key: None,
            }
        }

        pub(crate) fn with_tags(self, tags: Vec<String>) -> Self {
            Self {
                job: self.job.with_tags(tags),
                ..self
            }
        }

        pub(crate) fn with_status(self, status: JobStatus) -> Self {
            Self {
                job: self.job.with_status(status),
                ..self
            }
        }

        pub(crate) fn with_scheduled_at(self, scheduled_at: DateTime<Utc>) -> Self {
            Self {
                job: self.job.with_scheduled_at(scheduled_at),
                ..self
            }
        }
    }

    impl InMemoryBackend {
        async fn set_job_attempt(&self, id: JobId, attempt: i32) {
            let mut job = self.get_job(id).unwrap();
            job.attempt = attempt;
            self.update_job(job).await.unwrap();
        }
        async fn set_job_status(&self, id: JobId, job_status: JobStatus) {
            let mut job = self.get_job(id).unwrap();
            job.status = job_status;
            self.update_job(job).await.unwrap();
        }
    }

    #[tokio::test]
    async fn subscribe_ready_jobs() {
        let executor = "executor";
        let backend = InMemoryBackend::new();
        let mut stream = backend.subscribe_ready_jobs(executor.into()).await;
        backend
            .enqueue(EnqueuableJob::mock_job().with_executor("another_executor"))
            .await
            .unwrap();
        let job_id = backend
            .enqueue(EnqueuableJob::mock_job().with_executor(executor))
            .await
            .unwrap();

        let job = stream.next().await.unwrap().unwrap();
        assert_eq!(job.id, i32::from(job_id));
        assert_eq!(job.executor, executor);
    }

    #[tokio::test]
    async fn subscribe_ready_jobs_enqueuing_wakes_subscriber() {
        let executor = "executor";
        let backend = InMemoryBackend::new();
        let mut stream = backend.subscribe_ready_jobs(executor.into()).await;
        let handle = tokio::spawn(async move {
            match tokio::time::timeout(Duration::from_secs(2), stream.next()).await {
                Ok(Some(Ok(job))) => assert_eq!(job.executor, executor),
                Err(_) => panic!("Didn't get woken by enqueue of new job"),
                _ => panic!("Bad things happened"),
            }
        });
        tokio::task::yield_now().await;
        backend
            .enqueue(EnqueuableJob::mock_job().with_executor("another_executor"))
            .await
            .unwrap();
        backend
            .enqueue(EnqueuableJob::mock_job().with_executor(executor))
            .await
            .unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn subscribe_ready_jobs_enqueuing_does_not_wakes_subscriber_when_paused() {
        let executor = "executor";
        let backend = InMemoryBackend::new().paused();
        let mut stream = backend.subscribe_ready_jobs(executor.into()).await;
        let handle = tokio::spawn(async move {
            match tokio::time::timeout(Duration::from_secs(1), stream.next()).await {
                Ok(Some(Ok(_job))) => panic!("Should not get woken up"),
                Err(_) => {}
                _ => panic!("Bad things happened"),
            }
        });
        tokio::task::yield_now().await;
        backend
            .enqueue(EnqueuableJob::mock_job().with_executor("another_executor"))
            .await
            .unwrap();
        backend
            .enqueue(EnqueuableJob::mock_job().with_executor(executor))
            .await
            .unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn calling_notify_all_continues_execution() {
        let executor = "executor";
        let backend = InMemoryBackend::new().paused();
        let mut stream = backend.subscribe_ready_jobs(executor.into()).await;
        let handle = tokio::spawn(async move {
            match tokio::time::timeout(Duration::from_secs(1), stream.next()).await {
                Ok(Some(Ok(job))) => assert_eq!(job.executor, executor),
                Err(_) => panic!("Didn't get woken by enqueue of new job"),
                _ => panic!("Bad things happened"),
            }
        });
        tokio::task::yield_now().await;
        backend
            .enqueue(EnqueuableJob::mock_job().with_executor("another_executor"))
            .await
            .unwrap();
        backend
            .enqueue(EnqueuableJob::mock_job().with_executor(executor))
            .await
            .unwrap();
        backend.notify_all().unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn subscribe_ready_jobs_streams_jobs_by_priority() {
        let executor = "executor";
        let scheduled_at1 = Utc::now();
        let scheduled_at2 = Utc::now() + TimeDelta::milliseconds(10);
        let backend = InMemoryBackend::new();
        let mut stream = backend.subscribe_ready_jobs(executor.into()).await;
        let job_id1 = backend
            .enqueue(
                EnqueuableJob::mock_job()
                    .with_executor(executor)
                    .with_scheduled_at(scheduled_at2),
            )
            .await
            .unwrap();
        let job_id2 = backend
            .enqueue(
                EnqueuableJob::mock_job()
                    .with_executor(executor)
                    .with_priority(2)
                    .with_scheduled_at(scheduled_at1),
            )
            .await
            .unwrap();
        let job_id3 = backend
            .enqueue(
                EnqueuableJob::mock_job()
                    .with_executor(executor)
                    .with_scheduled_at(scheduled_at1),
            )
            .await
            .unwrap();

        let job_ids: [JobId; 3] = [
            stream.next().await.unwrap().unwrap().id.into(),
            stream.next().await.unwrap().unwrap().id.into(),
            stream.next().await.unwrap().unwrap().id.into(),
        ];
        assert_eq!(job_ids, [job_id3, job_id2, job_id1]);
    }

    #[tokio::test]
    async fn subscribe_ready_jobs_streams_only_one_steam_receives_job() {
        let executor = "executor";
        let backend = InMemoryBackend::new();
        let mut stream1 = backend.subscribe_ready_jobs(executor.into()).await;
        let mut stream2 = backend.subscribe_ready_jobs(executor.into()).await;
        let job_id1 = backend
            .enqueue(EnqueuableJob::mock_job().with_executor(executor))
            .await
            .unwrap();
        let job_id2 = backend
            .enqueue(EnqueuableJob::mock_job().with_executor(executor))
            .await
            .unwrap();

        let job_ids: std::collections::HashSet<_> =
            futures::future::join_all([stream1.next(), stream2.next()])
                .await
                .into_iter()
                .map(|res| res.unwrap().unwrap().id)
                .collect();
        let expected = [i32::from(job_id1), i32::from(job_id2)]
            .into_iter()
            .collect();
        assert_eq!(job_ids, expected);
    }

    #[tokio::test]
    async fn enqueue() {
        let backend = InMemoryBackend::new();

        let id1 = backend.enqueue(EnqueuableJob::mock_job()).await.unwrap();
        let id2 = backend.enqueue(EnqueuableJob::mock_job()).await.unwrap();

        assert_eq!(id1, 0.into());
        assert_eq!(id2, 1.into());
        assert!(backend.get_job(id1).is_some());
        assert!(backend.get_job(id2).is_some());
    }

    #[tokio::test]
    async fn enqueue_unique() {
        let backend = InMemoryBackend::new();

        let id1 = backend.enqueue(EnqueuableJob::mock_job()).await.unwrap();
        let id2 = backend
            .enqueue(EnqueuableJob::mock_job().with_uniqueness_criteria(Some(
                UniquenessCriteria::by_executor().and_within(TimeDelta::minutes(2)),
            )))
            .await
            .unwrap();

        assert_eq!(id1, 0.into());
        assert_eq!(id2, id1);
        assert!(backend.get_job(id2).is_some());
    }

    #[tokio::test]
    async fn enqueue_unique_no_matching() {
        let backend = InMemoryBackend::new();

        let id1 = backend.enqueue(EnqueuableJob::mock_job()).await.unwrap();
        let id2 = backend
            .enqueue(
                EnqueuableJob::mock_job().with_uniqueness_criteria(Some(
                    UniquenessCriteria::by_statuses(&[JobStatus::Retryable])
                        .and_within(TimeDelta::minutes(2)),
                )),
            )
            .await
            .unwrap();

        assert_eq!(id1, 0.into());
        assert_eq!(id2, 1.into());
        assert!(backend.get_job(id1).is_some());
        assert!(backend.get_job(id2).is_some());
    }

    #[tokio::test]
    async fn enqueue_unique_replace() {
        let backend = InMemoryBackend::new();

        let id1 = backend
            .enqueue(EnqueuableJob::mock_job().with_priority(2))
            .await
            .unwrap();
        let id2 = backend
            .enqueue(
                EnqueuableJob::mock_job().with_uniqueness_criteria(Some(
                    UniquenessCriteria::by_executor()
                        .and_within(TimeDelta::minutes(2))
                        .on_conflict(
                            Replace::priority()
                                .and_data()
                                .and_metadata()
                                .and_scheduled_at()
                                .and_max_attempts(),
                        ),
                )),
            )
            .await
            .unwrap();

        assert_eq!(id1, 0.into());
        assert_eq!(id2, id1);
        let job = backend.get_job(id1).expect("Job should be enqueued");
        // Priority has been updated
        assert_eq!(job.priority, 0);
    }

    #[tokio::test]
    async fn update_job() {
        let backend = InMemoryBackend::new();
        let id = backend.enqueue(EnqueuableJob::mock_job()).await.unwrap();
        let mut job = backend.get_job(id).unwrap();
        job.attempt = 3;

        assert!(backend.update_job(job).await.is_ok());

        let job = backend.get_job(id).unwrap();
        assert_eq!(job.attempt, 3);
    }

    #[tokio::test]
    async fn update_job_not_found() {
        let job = Job::raw_job();
        assert_matches!(
            InMemoryBackend::new().update_job(job).await,
            Err(BackendError::JobNotFound(_))
        );
    }

    #[tokio::test]
    async fn mark_job_complete() {
        let backend = InMemoryBackend::new();
        let id = backend.enqueue(EnqueuableJob::mock_job()).await.unwrap();

        assert!(backend.mark_job_complete(id).await.is_ok());
        assert_eq!(backend.get_job(id).unwrap().status, JobStatus::Complete);
    }

    #[tokio::test]
    async fn mark_job_complete_not_found() {
        assert_matches!(
            InMemoryBackend::new().mark_job_complete(42.into()).await,
            Err(BackendError::JobNotFound(_))
        );
    }

    #[tokio::test]
    async fn rerun_job_first_attempt() {
        let backend = InMemoryBackend::new();
        let job = EnqueuableJob::mock_job();
        let original_max_attempts = job.max_attempts as i32;
        let id = backend.enqueue(job).await.unwrap();
        backend.set_job_attempt(id, 1).await;

        assert!(backend.rerun_job(id).await.is_ok());

        let job = backend.get_job(id).unwrap();
        assert_eq!(job.status, JobStatus::Scheduled);
        assert_eq!(job.attempt, 1);
        assert_eq!(job.max_attempts, original_max_attempts + 1);
        assert!(job.completed_at.is_none());
        assert!(job.cancelled_at.is_none());
        assert!(job.discarded_at.is_none());
    }

    #[tokio::test]
    async fn rerun_job_first_subsequent_attempt() {
        let backend = InMemoryBackend::new();
        let job = EnqueuableJob::mock_job();
        let original_max_attempts = job.max_attempts as i32;
        let id = backend.enqueue(job).await.unwrap();
        backend.set_job_attempt(id, 2).await;

        assert!(backend.rerun_job(id).await.is_ok());

        let job = backend.get_job(id).unwrap();
        assert_eq!(job.status, JobStatus::Retryable);
        assert_eq!(job.attempt, 2);
        assert_eq!(job.max_attempts, original_max_attempts + 1);
        assert!(job.completed_at.is_none());
        assert!(job.cancelled_at.is_none());
        assert!(job.discarded_at.is_none());
    }

    #[tokio::test]
    async fn rerun_job_not_found() {
        assert_matches!(
            InMemoryBackend::new().rerun_job(42.into()).await,
            Err(BackendError::JobNotFound(_))
        );
    }

    #[tokio::test]
    async fn mark_job_snoozed_first_attempt() {
        let scheduled_at = Utc::now().add(TimeDelta::days(1));
        let backend = InMemoryBackend::new();
        let id = backend.enqueue(EnqueuableJob::mock_job()).await.unwrap();
        backend.set_job_attempt(id, 1).await;

        assert!(backend.mark_job_snoozed(id, scheduled_at).await.is_ok());

        let job = backend.get_job(id).unwrap();
        assert_eq!(job.status, JobStatus::Scheduled);
        assert_eq!(job.attempt, 0);
        assert_eq!(job.scheduled_at, scheduled_at);
    }

    #[tokio::test]
    async fn mark_job_snoozed_other_attempt() {
        let scheduled_at = Utc::now().add(TimeDelta::days(1));
        let backend = InMemoryBackend::new();
        let id = backend.enqueue(EnqueuableJob::mock_job()).await.unwrap();
        backend.set_job_attempt(id, 2).await;

        assert!(backend.mark_job_snoozed(id, scheduled_at).await.is_ok());

        let job = backend.get_job(id).unwrap();
        assert_eq!(job.status, JobStatus::Retryable);
        assert_eq!(job.attempt, 1);
        assert_eq!(job.scheduled_at, scheduled_at);
    }

    #[tokio::test]
    async fn mark_job_snoozed_not_found() {
        let scheduled_at = Utc::now().add(TimeDelta::days(1));
        assert_matches!(
            InMemoryBackend::new()
                .mark_job_snoozed(42.into(), scheduled_at)
                .await,
            Err(BackendError::JobNotFound(_))
        );
    }

    #[tokio::test]
    async fn mark_job_discarded() {
        let backend = InMemoryBackend::new();
        let id = backend.enqueue(EnqueuableJob::mock_job()).await.unwrap();
        let error = ExecutionError {
            error_type: ErrorType::Other("custom".to_owned()),
            message: "Error Message".to_owned(),
        };

        assert!(backend.mark_job_discarded(id, error.clone()).await.is_ok());

        let job = backend.get_job(id).unwrap();
        assert_eq!(job.status, JobStatus::Discarded);
        assert_eq!(job.errors.len(), 1);

        let job_error = job.errors.first().unwrap();
        assert_eq!(job_error.attempt as i32, job.attempt);
        assert_eq!(job_error.error_type, error.error_type);
        assert_eq!(job_error.details, error.message);
    }

    #[tokio::test]
    async fn mark_job_discarded_not_found() {
        let error = ExecutionError {
            error_type: ErrorType::Other("custom".to_owned()),
            message: "Error Message".to_owned(),
        };
        assert_matches!(
            InMemoryBackend::new()
                .mark_job_discarded(42.into(), error)
                .await,
            Err(BackendError::JobNotFound(_))
        );
    }

    #[tokio::test]
    async fn mark_job_retryable() {
        let scheduled_at = Utc::now().add(TimeDelta::days(1));
        let backend = InMemoryBackend::new();
        let id = backend.enqueue(EnqueuableJob::mock_job()).await.unwrap();
        let error = ExecutionError {
            error_type: ErrorType::Other("custom".to_owned()),
            message: "Error Message".to_owned(),
        };

        assert!(backend
            .mark_job_retryable(id, scheduled_at, error.clone())
            .await
            .is_ok());

        let job = backend.get_job(id).unwrap();
        assert_eq!(job.status, JobStatus::Retryable);
        assert_eq!(job.errors.len(), 1);

        let job_error = job.errors.first().unwrap();
        assert_eq!(job_error.attempt as i32, job.attempt);
        assert_eq!(job_error.error_type, error.error_type);
        assert_eq!(job_error.details, error.message);
    }

    #[tokio::test]
    async fn mark_job_retryable_not_found() {
        let scheduled_at = Utc::now().add(TimeDelta::days(1));
        let error = ExecutionError {
            error_type: ErrorType::Other("custom".to_owned()),
            message: "Error Message".to_owned(),
        };
        assert_matches!(
            InMemoryBackend::new()
                .mark_job_retryable(42.into(), scheduled_at, error)
                .await,
            Err(BackendError::JobNotFound(_))
        );
    }

    #[tokio::test]
    async fn mark_job_cancelled() {
        let backend = InMemoryBackend::new();
        let id = backend.enqueue(EnqueuableJob::mock_job()).await.unwrap();
        let error = ExecutionError {
            error_type: ErrorType::Other("custom".to_owned()),
            message: "Error Message".to_owned(),
        };

        assert!(backend.mark_job_cancelled(id, error.clone()).await.is_ok());

        let job = backend.get_job(id).unwrap();
        assert_eq!(job.status, JobStatus::Cancelled);
        assert_eq!(job.errors.len(), 1);

        let job_error = job.errors.first().unwrap();
        assert_eq!(job_error.attempt as i32, job.attempt);
        assert_eq!(job_error.error_type, error.error_type);
        assert_eq!(job_error.details, error.message);
    }

    #[tokio::test]
    async fn mark_job_cancelled_not_found() {
        let error = ExecutionError {
            error_type: ErrorType::Other("custom".to_owned()),
            message: "Error Message".to_owned(),
        };
        assert_matches!(
            InMemoryBackend::new()
                .mark_job_cancelled(42.into(), error)
                .await,
            Err(BackendError::JobNotFound(_))
        );
    }

    #[tokio::test]
    async fn query() {
        let backend = InMemoryBackend::new();
        let id = backend.enqueue(EnqueuableJob::mock_job()).await.unwrap();
        let _ = backend
            .enqueue(EnqueuableJob::mock_job().with_executor("other_executor"))
            .await
            .unwrap();
        let _ = backend.enqueue(EnqueuableJob::mock_job()).await.unwrap();

        assert_eq!(
            backend
                .query(Query::IdEquals(42.into()))
                .await
                .unwrap()
                .len(),
            0
        );
        assert_eq!(backend.query(Query::IdEquals(id)).await.unwrap().len(), 1);
        assert_eq!(
            backend
                .query(Query::ExecutorEqual(EnqueuableJob::DEFAULT_EXECUTOR))
                .await
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn prune_jobs() {
        let now = Utc::now();
        let backend = InMemoryBackend::new();
        for i in 0..100 {
            let id = backend
                .enqueue(EnqueuableJob::mock_job().with_scheduled_at(now - TimeDelta::hours(i)))
                .await
                .unwrap();
            backend.set_job_status(id, JobStatus::Complete).await;
        }
        for _ in 0..100 {
            let id = backend
                .enqueue(EnqueuableJob::mock_job().with_executor("other_executor"))
                .await
                .unwrap();
            backend.set_job_status(id, JobStatus::Complete).await;
        }

        backend
            .prune_jobs(&PruneSpec {
                status: JobStatus::Cancelled,
                prune_by: PruneBy::MaxLength(10),
                executors: Spec::Except(vec![]),
            })
            .await
            .unwrap();

        // No jobs have been pruned
        let all_jobs = Query::Not(Box::new(Query::ExecutorEqual("")));
        assert_eq!(backend.query(all_jobs.clone()).await.unwrap().len(), 200);

        backend
            .prune_jobs(&PruneSpec {
                status: JobStatus::Complete,
                prune_by: PruneBy::MaxLength(50),
                executors: Spec::Only(vec!["other_executor"]),
            })
            .await
            .unwrap();

        // 50 jobs have been pruned for `"other_executor"`
        let all_jobs = Query::Not(Box::new(Query::ExecutorEqual("")));
        assert_eq!(backend.query(all_jobs.clone()).await.unwrap().len(), 150);

        backend
            .prune_jobs(&PruneSpec {
                status: JobStatus::Complete,
                prune_by: PruneBy::MaxAge(TimeDelta::hours(50)),
                executors: Spec::Only(vec![EnqueuableJob::DEFAULT_EXECUTOR]),
            })
            .await
            .unwrap();

        // 50 more jobs have been pruned for `EnqueuableJob::DEFAULT_EXECUTOR`
        let all_jobs = Query::Not(Box::new(Query::ExecutorEqual("")));
        assert_eq!(backend.query(all_jobs.clone()).await.unwrap().len(), 100);
    }

    #[tokio::test]
    async fn badstate_errors_subscribers() {
        let backend = InMemoryBackend::new();
        tokio::task::spawn({
            let backend = backend.clone();
            async move {
                let _guard = backend.subscribers.write();
                panic!()
            }
        })
        .await
        .unwrap_err();

        assert_matches!(
            backend.enqueue(EnqueuableJob::mock_job()).await,
            Err(BackendError::BadState)
        );
    }

    #[tokio::test]
    async fn badstate_errors() {
        let backend = InMemoryBackend::new();
        let id = backend.enqueue(EnqueuableJob::mock_job()).await.unwrap();
        let scheduled_at = Utc::now().add(TimeDelta::days(1));
        let error = ExecutionError {
            error_type: ErrorType::Other("custom".to_owned()),
            message: "Error Message".to_owned(),
        };
        let pruner_spec = PruneSpec {
            status: JobStatus::Executing,
            prune_by: crate::pruner::PruneBy::MaxLength(5),
            executors: Spec::Only(vec![Job::DEFAULT_EXECUTOR]),
        };

        tokio::task::spawn({
            let backend = backend.clone();
            async move {
                let _guard = backend.jobs.write();
                panic!()
            }
        })
        .await
        .unwrap_err();

        assert_matches!(
            backend.enqueue(EnqueuableJob::mock_job()).await,
            Err(BackendError::BadState)
        );
        assert_matches!(
            backend.mark_job_complete(id).await,
            Err(BackendError::BadState)
        );
        assert_matches!(backend.rerun_job(id).await, Err(BackendError::BadState));
        assert_matches!(
            backend
                .mark_job_retryable(id, scheduled_at, error.clone())
                .await,
            Err(BackendError::BadState)
        );
        assert_matches!(
            backend.mark_job_discarded(id, error.clone()).await,
            Err(BackendError::BadState)
        );
        assert_matches!(
            backend.mark_job_cancelled(id, error.clone()).await,
            Err(BackendError::BadState)
        );
        assert_matches!(
            backend.mark_job_snoozed(id, scheduled_at).await,
            Err(BackendError::BadState)
        );
        assert_matches!(
            backend.prune_jobs(&pruner_spec).await,
            Err(BackendError::BadState)
        );
        assert_matches!(
            backend.update_job(Job::raw_job()).await,
            Err(BackendError::BadState)
        );
        assert_matches!(
            backend.query(Query::IdEquals(id)).await,
            Err(BackendError::BadState)
        );
    }
}
