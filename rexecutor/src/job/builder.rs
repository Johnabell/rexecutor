//! The builder for enqueuing jobs.
//!
//! Generally this will not be constructed directly and will instead be constructed via
//! [`Executor::builder`].
//!
//! # Overriding [`Executor`] default values
//!
//! When defining an [`Executor`] you specify the maximum number of attempts via
//! [`Executor::MAX_ATTEMPTS`]. However, when inserting a job it is possible to override this value
//! by calling [`JobBuilder::with_max_attempts`] (if not called the max attempts will be equal to
//! [`Executor::MAX_ATTEMPTS`].
//!
//! Similarly, the executor can define a job uniqueness criteria via
//! [`Executor::UNIQUENESS_CRITERIA`]. However, using [`JobBuilder::unique`] it is possible to
//! override this value for a specific job.
//!
//! # Global backend
//!
//! Jobs can either be enqueued onto a specific backend via [`JobBuilder::enqueue_to_backend`] or
//! enqueued to a globally set backend via [`JobBuilder::enqueue`]. To make use this API and the
//! global backend, [`crate::Rexecutor::set_global_backend`] should be called. If this hasn't been
//! called, then a [`RexecuterError::GlobalBackend`] will be returned.
//!
//! # Example
//!
//! To enqueue a job to the global backend for an [`Executor`] called `SimpleExecutor` the
//! following code can be executed:
//!
//! ```
//! # use rexecutor::prelude::*;
//! # use chrono::TimeDelta;
//! # pub(crate) struct SimpleExecutor;
//! #
//! # #[async_trait::async_trait]
//! # impl Executor for SimpleExecutor {
//! #     type Data = String;
//! #     type Metadata = String;
//! #     const NAME: &'static str = "simple_executor";
//! #     const MAX_ATTEMPTS: u16 = 2;
//! #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
//! #         ExecutionResult::Done
//! #     }
//! # }
//! # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
//! let result = SimpleExecutor::builder()
//!     .with_max_attempts(2)
//!     .with_tags(vec!["initial_job", "delayed"])
//!     .with_data("First job".into())
//!     .schedule_in(TimeDelta::hours(2))
//!     .enqueue()
//!     .await;
//! # });
//! ```
use crate::{
    backend::{Backend, EnqueuableJob},
    executor::Executor,
    job::uniqueness_criteria::UniquenessCriteria,
    RexecuterError, GLOBAL_BACKEND,
};
use chrono::{DateTime, Duration, Utc};

use super::JobId;

/// A builder for enqueuing jobs.
///
/// Generally this will not be constructed directly and will instead be constructed via
/// [`Executor::builder`].
///
/// # Overriding [`Executor`] default values
///
/// When defining an [`Executor`] you specify the maximum number of attempts via
/// [`Executor::MAX_ATTEMPTS`]. However, when inserting a job it is possible to override this value
/// by calling [`JobBuilder::with_max_attempts`] (if not called the max attempts will be equal to
/// [`Executor::MAX_ATTEMPTS`].
///
/// Similarly, the executor can define a job uniqueness criteria via
/// [`Executor::UNIQUENESS_CRITERIA`]. However, using [`JobBuilder::unique`] it is possible to
/// override this value for a specific job.
///
/// # Global backend
///
/// Jobs can either be enqueued onto a specific backend via [`JobBuilder::enqueue_to_backend`] or
/// enqueued to a globally set backend via [`JobBuilder::enqueue`]. To make use this API and the
/// global backend, [`crate::Rexecutor::set_global_backend`] should be called. If this hasn't been
/// called, then a [`RexecuterError::GlobalBackend`] will be returned.
///
/// # Example
///
/// To enqueue a job to the global backend for an [`Executor`] called `SimpleExecutor` the
/// following code can be executed:
///
/// ```
/// # use rexecutor::prelude::*;
/// # use chrono::Utc;
/// # pub(crate) struct SimpleExecutor;
/// #
/// # #[async_trait::async_trait]
/// # impl Executor for SimpleExecutor {
/// #     type Data = String;
/// #     type Metadata = String;
/// #     const NAME: &'static str = "simple_executor";
/// #     const MAX_ATTEMPTS: u16 = 2;
/// #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
/// #         ExecutionResult::Done
/// #     }
/// # }
/// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
/// let result = SimpleExecutor::builder()
///     .with_max_attempts(2)
///     .with_priority(2)
///     .add_tag("initial_job")
///     .with_data("First job".into())
///     .with_metadata("550e8400-e29b-41d4-a716-446655440000".into())
///     .schedule_at(Utc::now())
///     .enqueue()
///     .await;
/// # });
/// ```
// TODO add api to add as a cron job
pub struct JobBuilder<'a, E>
where
    E: Executor + 'static,
{
    data: Option<E::Data>,
    metadata: Option<E::Metadata>,
    max_attempts: Option<u16>,
    tags: Vec<String>,
    scheduled_at: DateTime<Utc>,
    priority: u16,
    uniqueness_criteria: Option<UniquenessCriteria<'a>>,
}

impl<'a, E> Default for JobBuilder<'a, E>
where
    E: Executor,
{
    fn default() -> Self {
        Self {
            data: Default::default(),
            metadata: Default::default(),
            max_attempts: None,
            tags: Default::default(),
            scheduled_at: Utc::now(),
            priority: 0,
            uniqueness_criteria: None,
        }
    }
}

impl<'a, E> JobBuilder<'a, E>
where
    E: Executor,
{
    /// Adds the job's data.
    ///
    /// For jobs with data this will need to be called for every job inserted.
    pub fn with_data(self, data: E::Data) -> Self {
        Self {
            data: Some(data),
            ..self
        }
    }

    /// Adds metadata to the job.
    pub fn with_metadata(self, metadata: E::Metadata) -> Self {
        Self {
            metadata: Some(metadata),
            ..self
        }
    }

    /// Sets the max attempts.
    ///
    /// If this is not called, `max_attempts` is set to [`Executor::MAX_ATTEMPTS`].
    pub fn with_max_attempts(self, max_attempts: u16) -> Self {
        Self {
            max_attempts: Some(max_attempts),
            ..self
        }
    }

    /// Sets when the jobs should be scheduled for.
    ///
    /// If not called the job is enqueue to run immediately, i.e. scheduled_at is set to
    /// [`Utc::now()`].
    pub fn schedule_at(self, schedule_at: DateTime<Utc>) -> Self {
        Self {
            scheduled_at: schedule_at,
            ..self
        }
    }

    /// Sets when the jobs should be scheduled by specifying the duration between now and the
    /// intended schedule time.
    ///
    /// If not called the job is enqueue to run immediately, i.e. scheduled_at is set to
    /// [`Utc::now()`].
    pub fn schedule_in(self, schedule_in: Duration) -> Self {
        Self {
            scheduled_at: Utc::now() + schedule_in,
            ..self
        }
    }

    /// Adds a single tag to the job.
    pub fn add_tag(self, tag: impl Into<String>) -> Self {
        let mut tags = self.tags;
        tags.push(tag.into());
        Self { tags, ..self }
    }

    /// Specify the complete list of tags to set on the job.
    ///
    /// Unlike [`JobBuilder::add_tag`], this function will override and previously specified tags.
    pub fn with_tags(self, tags: Vec<impl Into<String>>) -> Self {
        let tags = tags.into_iter().map(Into::into).collect();
        Self { tags, ..self }
    }

    /// Specify the [`UniquenessCriteria`] for inserting this job.
    ///
    /// [`UniquenessCriteria`] can be used to ensure the no duplicate jobs are inserted.
    ///
    /// If this is not specified here, the uniqueness criteria is set to
    /// [`Executor::UNIQUENESS_CRITERIA`].
    pub fn unique(self, criteria: UniquenessCriteria<'a>) -> Self {
        Self {
            uniqueness_criteria: Some(criteria),
            ..self
        }
    }

    /// Specify the job priority.
    ///
    /// Job priorities can be used to ensure certain jobs run before others when scheduled at the
    /// same time.
    ///
    /// Job priorities are handled on a per [`Executor`] basis.
    ///
    /// The lower the number the higher the priority, i.e. a priority of `0` is the maximum
    /// priority.
    ///
    /// If not specified, defaults to `0` (maximum priority).
    pub fn with_priority(self, priority: u16) -> Self {
        Self { priority, ..self }
    }

    /// Enqueue this job to the global backend.
    ///
    /// To make use this API and the global backend, [`crate::Rexecutor::set_global_backend`]
    /// should be called. If this hasn't been called, then a [`RexecuterError::GlobalBackend`]
    /// will be returned.
    pub async fn enqueue(self) -> Result<JobId, RexecuterError>
    where
        E::Data: 'static + Send,
    {
        let backend = GLOBAL_BACKEND.get().ok_or(RexecuterError::GlobalBackend)?;

        self.enqueue_to_backend(backend.as_ref()).await
    }

    /// Enqueue this job to the provided backend.
    pub async fn enqueue_to_backend<B: Backend + ?Sized>(
        self,
        backend: &B,
    ) -> Result<JobId, RexecuterError>
    where
        E::Data: 'static + Send,
    {
        let job_id = backend
            .enqueue(EnqueuableJob {
                data: serde_json::to_value(self.data)?,
                metadata: serde_json::to_value(self.metadata)?,
                executor: E::NAME.to_owned(),
                max_attempts: self.max_attempts.unwrap_or(E::MAX_ATTEMPTS),
                scheduled_at: self.scheduled_at,
                tags: self.tags,
                priority: self.priority,
                uniqueness_criteria: self.uniqueness_criteria.or(E::UNIQUENESS_CRITERIA),
            })
            .await?;

        Ok(job_id)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        backend::{BackendError, MockBackend},
        executor::test::SimpleExecutor,
        Rexecutor,
    };

    use super::*;

    #[tokio::test]
    async fn enqueue() {
        let expected_job_id = JobId(0);

        let mut backend = MockBackend::default();
        backend
            .expect_enqueue()
            .returning(move |_| Ok(expected_job_id));

        let job_id = SimpleExecutor::builder()
            .with_max_attempts(2)
            .with_tags(vec!["initial_job"])
            .with_data("First job".into())
            .schedule_in(Duration::hours(2))
            .enqueue_to_backend(&backend)
            .await
            .unwrap();

        assert_eq!(job_id, expected_job_id);
    }

    #[tokio::test]
    async fn enqueue_error() {
        let mut backend = MockBackend::default();
        backend
            .expect_enqueue()
            .returning(move |_| Err(BackendError::BadStateError));

        SimpleExecutor::builder()
            .with_max_attempts(2)
            .with_tags(vec!["initial_job"])
            .with_data("First job".into())
            .schedule_in(Duration::hours(2))
            .enqueue_to_backend(&backend)
            .await
            .expect_err("Should error");
    }

    #[tokio::test]
    async fn enqueue_to_global() {
        // This might need updating, since the global backend truely is global. Might be nice to
        // have a testing strategy for people using the global backend.
        let expected_job_id = JobId(0);

        let mut backend = MockBackend::default();
        backend
            .expect_enqueue()
            .returning(move |_| Ok(expected_job_id));

        Rexecutor::new(Arc::new(backend))
            .set_global_backend()
            .unwrap();

        let job_id = SimpleExecutor::builder()
            .with_max_attempts(2)
            .with_tags(vec!["initial_job"])
            .with_data("First job".into())
            .schedule_in(Duration::hours(2))
            .enqueue()
            .await
            .unwrap();

        assert_eq!(job_id, expected_job_id);
    }
}
