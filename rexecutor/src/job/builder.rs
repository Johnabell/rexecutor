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
//! called, then a [`RexecutorError::GlobalBackend`] will be returned.
//!
//! # Example
//!
//! To enqueue a job to the global backend for an [`Executor`] called `ExampleExecutor` the
//! following code can be executed:
//!
//! ```
//! # use rexecutor::prelude::*;
//! # use chrono::{Utc, TimeDelta};
//! # use rexecutor::backend::memory::InMemoryBackend;
//! # use rexecutor::assert_enqueued;
//! # pub(crate) struct ExampleExecutor;
//! #
//! # #[async_trait::async_trait]
//! # impl Executor for ExampleExecutor {
//! #     type Data = String;
//! #     type Metadata = String;
//! #     const NAME: &'static str = "simple_executor";
//! #     const MAX_ATTEMPTS: u16 = 2;
//! #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
//! #         ExecutionResult::Done
//! #     }
//! # }
//! # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
//! let backend = InMemoryBackend::new().paused();
//! Rexecutor::new(backend).set_global_backend().unwrap();
//!
//! ExampleExecutor::builder()
//!     .with_max_attempts(2)
//!     .with_tags(vec!["initial_job", "delayed"])
//!     .with_data("First job".into())
//!     .schedule_in(TimeDelta::hours(2))
//!     .enqueue()
//!     .await
//!     .unwrap();
//!
//! assert_enqueued!(
//!     with_data: "First job".to_owned(),
//!     tagged_with: ["initial_job", "delayed"],
//!     scheduled_after: Utc::now() + TimeDelta::minutes(110),
//!     scheduled_before: Utc::now() + TimeDelta::minutes(130),
//!     for_executor: ExampleExecutor
//! );
//! # });
//! ```
use std::marker::PhantomData;

use crate::{
    backend::{Backend, EnqueuableJob},
    executor::Executor,
    global_backend::GlobalBackend,
    job::uniqueness_criteria::UniquenessCriteria,
    RexecutorError,
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
/// called, then a [`RexecutorError::GlobalBackend`] will be returned.
///
/// # Example
///
/// To enqueue a job to the global backend for an [`Executor`] called `ExampleExecutor` the
/// following code can be executed:
///
/// ```
/// # use rexecutor::prelude::*;
/// # use chrono::{Utc, TimeDelta};
/// # use rexecutor::backend::memory::InMemoryBackend;
/// # use rexecutor::assert_enqueued;
/// # pub(crate) struct ExampleExecutor;
/// #
/// # #[async_trait::async_trait]
/// # impl Executor for ExampleExecutor {
/// #     type Data = String;
/// #     type Metadata = String;
/// #     const NAME: &'static str = "simple_executor";
/// #     const MAX_ATTEMPTS: u16 = 2;
/// #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
/// #         ExecutionResult::Done
/// #     }
/// # }
/// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
/// let backend = InMemoryBackend::new().paused();
/// Rexecutor::new(backend).set_global_backend().unwrap();
///
/// ExampleExecutor::builder()
///     .with_max_attempts(2)
///     .with_priority(2)
///     .add_tag("initial_job")
///     .with_data("First job".into())
///     .with_metadata("550e8400-e29b-41d4-a716-446655440000".into())
///     .schedule_at(Utc::now())
///     .enqueue()
///     .await
///     .unwrap();
///
/// assert_enqueued!(
///     with_data: "First job".to_owned(),
///     with_metadata: "550e8400-e29b-41d4-a716-446655440000".to_owned(),
///     tagged_with: ["initial_job"],
///     scheduled_after: Utc::now() - TimeDelta::minutes(1),
///     scheduled_before: Utc::now(),
///     for_executor: ExampleExecutor
/// );
/// # });
/// ```
// TODO add api to add as a cron job
#[allow(private_bounds)]
pub struct JobBuilder<'a, E, State: BuilderState = NoData>
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
    _state: PhantomData<State>,
}

/// Internal trait for states of the job builder.
trait BuilderState {}

/// Builder in a state before the data has been specified.
pub struct NoData;
/// Builder in state where the data has been specified.
pub struct WithData;

impl BuilderState for NoData {}
impl BuilderState for WithData {}

/// Marker trait to specify that it is possible to enqueue a job without data.
///
/// Currently this is implemented for unit and [`Option<T>`].
///
/// Consider making this a public trait.
trait DataOptional {}

impl DataOptional for () {}
impl<T> DataOptional for Option<T> {}

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
            _state: PhantomData,
        }
    }
}

#[allow(private_bounds)]
impl<'a, E, State> JobBuilder<'a, E, State>
where
    E: Executor,
    State: BuilderState,
{
    /// Adds the job's data.
    ///
    /// For jobs with data this will need to be called for every job inserted.
    pub fn with_data(self, data: E::Data) -> JobBuilder<'a, E, WithData> {
        JobBuilder {
            data: Some(data),
            metadata: self.metadata,
            max_attempts: self.max_attempts,
            tags: self.tags,
            scheduled_at: self.scheduled_at,
            priority: self.priority,
            uniqueness_criteria: self.uniqueness_criteria,
            _state: PhantomData,
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

    async fn _enqueue(self) -> Result<JobId, RexecutorError>
    where
        E::Data: 'static + Send,
    {
        self._enqueue_to_backend(GlobalBackend::as_ref()?).await
    }

    /// Enqueue this job to the provided backend.
    async fn _enqueue_to_backend<B: Backend + ?Sized>(
        self,
        backend: &B,
    ) -> Result<JobId, RexecutorError>
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

#[allow(private_bounds)]
impl<'a, E> JobBuilder<'a, E, NoData>
where
    E: Executor,
    E::Data: DataOptional,
{
    /// Enqueue this job to the global backend.
    ///
    /// To make use this API and the global backend, [`crate::Rexecutor::set_global_backend`]
    /// should be called. If this hasn't been called, then a [`RexecutorError::GlobalBackend`]
    /// will be returned.
    ///
    /// # Example
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    /// # use chrono::{Utc, TimeDelta};
    /// # use rexecutor::backend::memory::InMemoryBackend;
    /// # use rexecutor::RexecutorError;
    /// # use rexecutor::assert_enqueued;
    /// # pub(crate) struct ExampleExecutor;
    /// #
    /// # #[async_trait::async_trait]
    /// # impl Executor for ExampleExecutor {
    /// #     type Data = ();
    /// #     type Metadata = String;
    /// #     const NAME: &'static str = "simple_executor";
    /// #     const MAX_ATTEMPTS: u16 = 2;
    /// #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
    /// #         ExecutionResult::Done
    /// #     }
    /// # }
    /// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
    /// // If we have not set the global backend, we get an error.
    /// let result = ExampleExecutor::builder()
    ///     .with_max_attempts(2)
    ///     .with_priority(2)
    ///     .add_tag("initial_job")
    ///     .with_metadata("550e8400-e29b-41d4-a716-446655440000".into())
    ///     .schedule_at(Utc::now())
    ///     .enqueue()
    ///     .await;
    ///
    /// assert!(matches!(result, Err(RexecutorError::GlobalBackend)));
    ///
    /// // When we set the global backend, we can enqueue to it.
    /// let backend = InMemoryBackend::new().paused();
    /// Rexecutor::new(backend).set_global_backend().unwrap();
    ///
    /// ExampleExecutor::builder()
    ///     .with_max_attempts(2)
    ///     .with_priority(2)
    ///     .add_tag("initial_job")
    ///     .with_metadata("550e8400-e29b-41d4-a716-446655440000".into())
    ///     .schedule_at(Utc::now())
    ///     .enqueue()
    ///     .await
    ///     .unwrap();
    ///
    /// assert_enqueued!(
    ///     with_metadata: "550e8400-e29b-41d4-a716-446655440000".to_owned(),
    ///     tagged_with: ["initial_job"],
    ///     scheduled_after: Utc::now() - TimeDelta::minutes(1),
    ///     scheduled_before: Utc::now(),
    ///     for_executor: ExampleExecutor
    /// );
    /// # });
    /// ```
    pub async fn enqueue(self) -> Result<JobId, RexecutorError>
    where
        E::Data: 'static + Send,
    {
        self._enqueue().await
    }

    /// Enqueue this job to the provided backend.
    pub async fn enqueue_to_backend<B: Backend + ?Sized>(
        self,
        backend: &B,
    ) -> Result<JobId, RexecutorError>
    where
        E::Data: 'static + Send,
    {
        self._enqueue_to_backend(backend).await
    }
}
impl<'a, E> JobBuilder<'a, E, WithData>
where
    E: Executor,
{
    /// Enqueue this job to the global backend.
    ///
    /// To make use this API and the global backend, [`crate::Rexecutor::set_global_backend`]
    /// should be called. If this hasn't been called, then a [`RexecutorError::GlobalBackend`]
    /// will be returned.
    ///
    /// # Example
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    /// # use chrono::{Utc, TimeDelta};
    /// # use rexecutor::backend::memory::InMemoryBackend;
    /// # use rexecutor::RexecutorError;
    /// # use rexecutor::assert_enqueued;
    /// # pub(crate) struct ExampleExecutor;
    /// #
    /// # #[async_trait::async_trait]
    /// # impl Executor for ExampleExecutor {
    /// #     type Data = String;
    /// #     type Metadata = String;
    /// #     const NAME: &'static str = "simple_executor";
    /// #     const MAX_ATTEMPTS: u16 = 2;
    /// #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
    /// #         ExecutionResult::Done
    /// #     }
    /// # }
    /// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
    /// // If we have not set the global backend, we get an error.
    /// let result = ExampleExecutor::builder()
    ///     .with_max_attempts(2)
    ///     .with_priority(2)
    ///     .add_tag("initial_job")
    ///     .with_data("First job".into())
    ///     .with_metadata("550e8400-e29b-41d4-a716-446655440000".into())
    ///     .schedule_at(Utc::now())
    ///     .enqueue()
    ///     .await;
    ///
    /// assert!(matches!(result, Err(RexecutorError::GlobalBackend)));
    ///
    /// // When we set the global backend, we can enqueue to it.
    /// let backend = InMemoryBackend::new().paused();
    /// Rexecutor::new(backend).set_global_backend().unwrap();
    ///
    /// ExampleExecutor::builder()
    ///     .with_max_attempts(2)
    ///     .with_priority(2)
    ///     .add_tag("initial_job")
    ///     .with_data("First job".into())
    ///     .with_metadata("550e8400-e29b-41d4-a716-446655440000".into())
    ///     .schedule_at(Utc::now())
    ///     .enqueue()
    ///     .await
    ///     .unwrap();
    ///
    /// assert_enqueued!(
    ///     with_data: "First job".to_owned(),
    ///     with_metadata: "550e8400-e29b-41d4-a716-446655440000".to_owned(),
    ///     tagged_with: ["initial_job"],
    ///     scheduled_after: Utc::now() - TimeDelta::minutes(1),
    ///     scheduled_before: Utc::now(),
    ///     for_executor: ExampleExecutor
    /// );
    /// # });
    /// ```
    pub async fn enqueue(self) -> Result<JobId, RexecutorError>
    where
        E::Data: 'static + Send,
    {
        self._enqueue().await
    }

    /// Enqueue this job to the provided backend.
    pub async fn enqueue_to_backend<B: Backend + ?Sized>(
        self,
        backend: &B,
    ) -> Result<JobId, RexecutorError>
    where
        E::Data: 'static + Send,
    {
        self._enqueue_to_backend(backend).await
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
            .returning(move |_| Err(BackendError::BadState));

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
