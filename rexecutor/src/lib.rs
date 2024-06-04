//! A robust job processing library for rust.
//!
//! # Setting up `Rexecutor`
//!
//! To create an instance of [`Rexecutor`] you will need to have an implementation of [`Backend`].
//! The only one provided in this crate is [`backend::memory::InMemoryBackend`] which is primarily
//! provided for testing purposes. Instead a backend should be used from one of the implementations
//! provided by other crates.
//!
//! # Creating executors
//!
//! Jobs are defined by creating a struct/enum and implementing `Executor` for it.
//!
//! ## Example defining an executor
//!
//! You can define and enqueue a job as follows:
//!
//! ```
//! use rexecutor::prelude::*;
//! use chrono::{Utc, TimeDelta};
//! use rexecutor::backend::memory::InMemoryBackend;
//! use rexecutor::assert_enqueued;
//! let backend = InMemoryBackend::new().paused();
//! Rexecutor::new(backend).set_global_backend().unwrap();
//! struct EmailJob;
//!
//! #[async_trait::async_trait]
//! impl Executor for EmailJob {
//!     type Data = String;
//!     type Metadata = String;
//!     const NAME: &'static str = "email_job";
//!     const MAX_ATTEMPTS: u16 = 2;
//!     async fn execute(job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
//!         println!("{} running, with args: {}", Self::NAME, job.data);
//!         /// Do something important with an email
//!         ExecutionResult::Done
//!     }
//! }
//!
//! # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
//! let _ = EmailJob::builder()
//!     .with_data("bob.shuruncle@example.com".to_owned())
//!     .schedule_in(TimeDelta::hours(3))
//!     .enqueue()
//!     .await;
//!
//! assert_enqueued!(
//!     with_data: "bob.shuruncle@example.com".to_owned(),
//!     scheduled_after: Utc::now() + TimeDelta::minutes(170),
//!     scheduled_before: Utc::now() + TimeDelta::minutes(190),
//!     for_executor: EmailJob
//! );
//! # });
//! ```
//!
//! ## Unique jobs
//!
//! It is possible to ensure uniqueness of jobs based on certain criteria. This can be defined as
//! part of the implementation of `Executor` via [`Executor::UNIQUENESS_CRITERIA`] or when
//! inserting the job via [`job::builder::JobBuilder::unique`].
//!
//! For example to ensure that only one unique job is ran every five minutes it is possible to use
//! the following uniqueness criteria.
//!
//! ```
//! # use rexecutor::prelude::*;
//! # use chrono::{Utc, TimeDelta};
//! # use rexecutor::backend::memory::InMemoryBackend;
//! # use rexecutor::assert_enqueued;
//! # let backend = InMemoryBackend::new().paused();
//! # Rexecutor::new(backend).set_global_backend().unwrap();
//! struct UniqueJob;
//!
//! #[async_trait::async_trait]
//! impl Executor for UniqueJob {
//!     type Data = ();
//!     type Metadata = ();
//!     const NAME: &'static str = "unique_job";
//!     const MAX_ATTEMPTS: u16 = 1;
//!     const UNIQUENESS_CRITERIA: Option<UniquenessCriteria<'static>> = Some(
//!         UniquenessCriteria::by_executor()
//!             .and_within(TimeDelta::seconds(300))
//!             .on_conflict(Replace::priority().for_statuses(&JobStatus::ALL)),
//!     );
//!     async fn execute(job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
//!         println!("{} running, with args: {:?}", Self::NAME, job.data);
//!         // Do something important
//!         ExecutionResult::Done
//!     }
//! }
//!
//! # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
//! let _ = UniqueJob::builder().enqueue().await;
//! let _ = UniqueJob::builder().enqueue().await;
//!
//! // Only one of jobs was enqueued
//! assert_enqueued!(
//!     1 job,
//!     scheduled_before: Utc::now(),
//!     for_executor: UniqueJob
//! );
//! # });
//! ```
//!
//! Additionally it is possible to specify what action should be taken when there is a conflicting
//! job. In the example above the priority is override. For more details of how to use uniqueness
//! see [`job::uniqueness_criteria::UniquenessCriteria`].
//!
//!
//! # Overriding [`Executor`] default values
//!
//! When defining an [`Executor`] you specify the maximum number of attempts via
//! [`Executor::MAX_ATTEMPTS`]. However, when inserting a job it is possible to override this value
//! by calling [`job::builder::JobBuilder::with_max_attempts`] (if not called the max attempts will be equal to
//! [`Executor::MAX_ATTEMPTS`].
//!
//! Similarly, the executor can define a job uniqueness criteria via
//! [`Executor::UNIQUENESS_CRITERIA`]. However, using [`job::builder::JobBuilder::unique`] it is possible to
//! override this value for a specific job.
//!
//! # Setting up executors to run
//!
//! For each executor you would like to run [`Rexecutor::with_executor`] should be called. Being
//! explicit about this opens the possibility of having specific nodes in a cluster running as
//! worker nodes for certain enqueued jobs while other node not responsible for their execution.
//!
//! ## Example setting up executors
//!
//! ```
//! # use rexecutor::prelude::*;
//! # use std::str::FromStr;
//! # use chrono::TimeDelta;
//! # use rexecutor::backend::memory::InMemoryBackend;
//! # pub(crate) struct RefreshWorker;
//! # pub(crate) struct EmailScheduler;
//! # pub(crate) struct RegistrationWorker;
//! #
//! # #[async_trait::async_trait]
//! # impl Executor for RefreshWorker {
//! #     type Data = String;
//! #     type Metadata = String;
//! #     const NAME: &'static str = "refresh_worker";
//! #     const MAX_ATTEMPTS: u16 = 2;
//! #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
//! #         ExecutionResult::Done
//! #     }
//! # }
//! # #[async_trait::async_trait]
//! # impl Executor for EmailScheduler {
//! #     type Data = String;
//! #     type Metadata = String;
//! #     const NAME: &'static str = "email_scheduler";
//! #     const MAX_ATTEMPTS: u16 = 2;
//! #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
//! #         ExecutionResult::Done
//! #     }
//! # }
//! # #[async_trait::async_trait]
//! # impl Executor for RegistrationWorker {
//! #     type Data = String;
//! #     type Metadata = String;
//! #     const NAME: &'static str = "registration_worker";
//! #     const MAX_ATTEMPTS: u16 = 2;
//! #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
//! #         ExecutionResult::Done
//! #     }
//! # }
//! # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
//! let backend = InMemoryBackend::new();
//! Rexecutor::new(backend)
//!     .with_executor::<RefreshWorker>()
//!     .with_executor::<EmailScheduler>()
//!     .with_executor::<RegistrationWorker>();
//! # });
//! ```
//!
//! # Enqueuing jobs
//!
//! Generally jobs will be enqueued using the [`job::builder::JobBuilder`] returned by [`Executor::builder`].
//!
//! When enqueuing jobs the data and metadata of the job can be specified. Additionally, the
//! default value of the [`Executor`] can be overriden.
//!
//! ## Overriding [`Executor`] default values
//!
//! When defining an [`Executor`] you specify the maximum number of attempts via
//! [`Executor::MAX_ATTEMPTS`]. However, when inserting a job it is possible to override this value
//! by calling [`job::builder::JobBuilder::with_max_attempts`] (if not called the max attempts will be equal to
//! [`Executor::MAX_ATTEMPTS`].
//!
//! Similarly, the executor can define a job uniqueness criteria via
//! [`Executor::UNIQUENESS_CRITERIA`]. However, using [`job::builder::JobBuilder::unique`] it is possible to
//! override this value for a specific job.
//!
//! ## Example enqueuing a job
//!
//! ```
//! # use rexecutor::prelude::*;
//! # use std::sync::Arc;
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
//! let backend = Arc::new(InMemoryBackend::new().paused());
//! Rexecutor::new(backend.clone()).set_global_backend().unwrap();
//!
//! ExampleExecutor::builder()
//!     .with_max_attempts(2)
//!     .with_tags(vec!["initial_job", "delayed"])
//!     .with_data("First job".into())
//!     .schedule_in(TimeDelta::hours(2))
//!     .enqueue_to_backend(&backend)
//!     .await
//!     .unwrap();
//!
//! assert_enqueued!(
//!     to: backend,
//!     with_data: "First job".to_owned(),
//!     tagged_with: ["initial_job", "delayed"],
//!     scheduled_after: Utc::now() + TimeDelta::minutes(110),
//!     scheduled_before: Utc::now() + TimeDelta::minutes(130),
//!     for_executor: ExampleExecutor
//! );
//! # });
//! ```
//!
//! # Compile time scheduling of cron jobs
//!
//! It can be useful to have jobs that run on a given schedule. Jobs like this can be setup using
//! either [`Rexecutor::with_cron_executor`] or [`Rexecutor::with_cron_executor_for_timezone`]. The
//! later is use to specify the specific timezone that the jobs should be scheduled to run in.
//!
//! ## Example setting up a UTC cron job
//!
//! To setup a cron jobs to run every day at midnight you can use the following code.
//!
//! ```
//! # use rexecutor::prelude::*;
//! # use rexecutor::backend::{Backend, memory::InMemoryBackend};
//! struct CronJob;
//! #[async_trait::async_trait]
//! impl Executor for CronJob {
//!     type Data = String;
//!     type Metadata = ();
//!     const NAME: &'static str = "cron_job";
//!     const MAX_ATTEMPTS: u16 = 1;
//!     async fn execute(job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
//!         /// Do something important
//!         ExecutionResult::Done
//!     }
//! }
//! # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
//! let schedule = cron::Schedule::try_from("0 0 0 * * *").unwrap();
//!
//! let backend = InMemoryBackend::new();
//! Rexecutor::new(backend).with_cron_executor::<CronJob>(schedule, "important data".to_owned());
//! # });
//! ```
//!
//! # Pruning jobs
//!
//! After jobs have completed, been cancelled, or discarded it is useful to be able to clean up.
//! To setup the job pruner [`Rexecutor::with_job_pruner`] should be called passing in the given
//! [`PrunerConfig`].
//!
//! Given the different ways in which jobs can finish it is often useful to be able to have fine
//! grained control over how old jobs should be cleaned up. [`PrunerConfig`] enables such control.
//!
//! When constructing [`PrunerConfig`] a [`cron::Schedule`] is provided to specify when the pruner
//! should run.
//!
//! Depending on the load/throughput of the system the pruner can be scheduled to run anywhere
//! from once a year through to multiple times per hour.
//!
//! ## Example configuring the job pruner
//!
//! ```
//! # use rexecutor::prelude::*;
//! # use std::str::FromStr;
//! # use chrono::TimeDelta;
//! # use rexecutor::backend::memory::InMemoryBackend;
//! # pub(crate) struct RefreshWorker;
//! # pub(crate) struct EmailScheduler;
//! # pub(crate) struct RegistrationWorker;
//! #
//! # #[async_trait::async_trait]
//! # impl Executor for RefreshWorker {
//! #     type Data = String;
//! #     type Metadata = String;
//! #     const NAME: &'static str = "refresh_worker";
//! #     const MAX_ATTEMPTS: u16 = 2;
//! #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
//! #         ExecutionResult::Done
//! #     }
//! # }
//! # #[async_trait::async_trait]
//! # impl Executor for EmailScheduler {
//! #     type Data = String;
//! #     type Metadata = String;
//! #     const NAME: &'static str = "email_scheduler";
//! #     const MAX_ATTEMPTS: u16 = 2;
//! #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
//! #         ExecutionResult::Done
//! #     }
//! # }
//! # #[async_trait::async_trait]
//! # impl Executor for RegistrationWorker {
//! #     type Data = String;
//! #     type Metadata = String;
//! #     const NAME: &'static str = "registration_worker";
//! #     const MAX_ATTEMPTS: u16 = 2;
//! #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
//! #         ExecutionResult::Done
//! #     }
//! # }
//! let config = PrunerConfig::new(cron::Schedule::from_str("0 0 * * * *").unwrap())
//!     .with_max_concurrency(Some(2))
//!     .with_pruner(
//!         Pruner::max_age(TimeDelta::days(31), JobStatus::Complete)
//!             .only::<RefreshWorker>()
//!             .and::<EmailScheduler>(),
//!     )
//!     .with_pruner(
//!         Pruner::max_length(200, JobStatus::Discarded)
//!             .except::<RefreshWorker>()
//!             .and::<EmailScheduler>(),
//!     );
//!
//! # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
//! let backend = InMemoryBackend::new();
//! Rexecutor::new(backend)
//!     .with_executor::<RefreshWorker>()
//!     .with_executor::<EmailScheduler>()
//!     .with_executor::<RegistrationWorker>()
//!     .with_job_pruner(config);
//! # });
//! ```
//!
//! # Shutting rexecutor down
//!
//! To avoid jobs getting killed mid way through their executions it is important to make use of
//! graceful shutdown. This can either explicitly be called using [`Rexecutor::graceful_shutdown`],
//! or via use of the [`DropGuard`] obtained via [`Rexecutor::drop_guard`].
//!
//! Using [`Rexecutor::graceful_shutdown`] or [`Rexecutor::drop_guard`] will ensure that all
//! currently executing jobs will be allowed time to complete before shutting rexecutor down.
//!
//! ## Example using the `DropGuard`
//!
//! ```
//! # use rexecutor::prelude::*;
//! # use std::str::FromStr;
//! # use chrono::TimeDelta;
//! # use rexecutor::backend::memory::InMemoryBackend;
//! # pub(crate) struct RefreshWorker;
//! # pub(crate) struct EmailScheduler;
//! # pub(crate) struct RegistrationWorker;
//! #
//! # #[async_trait::async_trait]
//! # impl Executor for RefreshWorker {
//! #     type Data = String;
//! #     type Metadata = String;
//! #     const NAME: &'static str = "refresh_worker";
//! #     const MAX_ATTEMPTS: u16 = 2;
//! #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
//! #         ExecutionResult::Done
//! #     }
//! # }
//! # #[async_trait::async_trait]
//! # impl Executor for EmailScheduler {
//! #     type Data = String;
//! #     type Metadata = String;
//! #     const NAME: &'static str = "email_scheduler";
//! #     const MAX_ATTEMPTS: u16 = 2;
//! #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
//! #         ExecutionResult::Done
//! #     }
//! # }
//! # #[async_trait::async_trait]
//! # impl Executor for RegistrationWorker {
//! #     type Data = String;
//! #     type Metadata = String;
//! #     const NAME: &'static str = "registration_worker";
//! #     const MAX_ATTEMPTS: u16 = 2;
//! #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
//! #         ExecutionResult::Done
//! #     }
//! # }
//! # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
//! let backend = InMemoryBackend::new();
//! // Note this must be given a name to ensure it is dropped at the end of the scope.
//! // See https://doc.rust-lang.org/book/ch18-03-pattern-syntax.html#ignoring-an-unused-variable-by-starting-its-name-with-_
//! let _guard = Rexecutor::new(backend)
//!     .with_executor::<RefreshWorker>()
//!     .with_executor::<EmailScheduler>()
//!     .with_executor::<RegistrationWorker>()
//!     .drop_guard();
//! # });
//! ```
//!
//! # Global backend
//!
//! Rexecutor can be ran either with use of a global backend. This enables the use of the
//! convenience [`job::builder::JobBuilder::enqueue`] method which does not require a reference to
//! the backend to be passed down to the code that needs to enqueue a job.
//!
//! The global backend can be set using [`Rexecutor::set_global_backend`] this should only be
//! called once otherwise it will return an error.
//!
//! In fact for a single possible [`Rexecutor`] instance it is impossible to call this twice
//!
//! ```compile_fail
//! # use rexecutor::prelude::*;
//! let backend = rexecutor::backend::memory::InMemoryBackend::new();
//! Rexecutor::new(backend).set_global_backend().set_global_backend();
//! ```
//!
//! Note, using a global backend has many of the same drawbacks of any global variable in
//! particular it can make unit testing more difficult.
#![deny(missing_docs)]
use std::{hash::Hash, marker::PhantomData};

pub mod backend;
pub mod backoff;
mod cron_runner;
pub mod executor;
#[doc(hidden)]
pub mod global_backend;
pub mod job;
pub mod prelude;
pub mod pruner;
pub mod testing;

use backend::{Backend, BackendError};
use chrono::{TimeZone, Utc};
use cron_runner::CronRunner;
use executor::Executor;
use global_backend::GlobalBackend;
use job::runner::JobRunner;
use pruner::{runner::PrunerRunner, PrunerConfig};
use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

trait InternalRexecutorState {}

/// The state of [`Rexecutor`] after the global backend has been set, this makes it possible to
/// avoid calling [`Rexecutor::set_global_backend`] multiple times from the same instance.
#[doc(hidden)]
pub struct GlobalSet;

/// The state of [`Rexecutor`] before the global backend has been set. Using different states,
/// makes it possible to avoid calling [`Rexecutor::set_global_backend`] multiple times from the
/// same instance.
#[doc(hidden)]
pub struct GlobalUnset;
impl InternalRexecutorState for GlobalUnset {}
impl InternalRexecutorState for GlobalSet {}

/// The entry point for setting up rexecutor.
///
/// To create an instance of [`Rexecutor`] you will need to have an implementation of [`Backend`].
/// The only one provided in this crate is [`backend::memory::InMemoryBackend`] which is primarily
/// provided for testing purposes. Instead a backend should be used from one of the implementations
/// provided by other crates.
///
/// # Setting up executors
///
/// For each executor you would like to run [`Rexecutor::with_executor`] should be called. Being
/// explicit about this opens the possibility of having specific nodes in a cluster running as
/// worker nodes for certain enqueued jobs while other node not responsible for their execution.
///
/// ## Example setting up executors
///
/// ```
/// # use rexecutor::prelude::*;
/// # use std::str::FromStr;
/// # use chrono::TimeDelta;
/// # use rexecutor::backend::memory::InMemoryBackend;
/// # pub(crate) struct RefreshWorker;
/// # pub(crate) struct EmailScheduler;
/// # pub(crate) struct RegistrationWorker;
/// #
/// # #[async_trait::async_trait]
/// # impl Executor for RefreshWorker {
/// #     type Data = String;
/// #     type Metadata = String;
/// #     const NAME: &'static str = "refresh_worker";
/// #     const MAX_ATTEMPTS: u16 = 2;
/// #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
/// #         ExecutionResult::Done
/// #     }
/// # }
/// # #[async_trait::async_trait]
/// # impl Executor for EmailScheduler {
/// #     type Data = String;
/// #     type Metadata = String;
/// #     const NAME: &'static str = "email_scheduler";
/// #     const MAX_ATTEMPTS: u16 = 2;
/// #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
/// #         ExecutionResult::Done
/// #     }
/// # }
/// # #[async_trait::async_trait]
/// # impl Executor for RegistrationWorker {
/// #     type Data = String;
/// #     type Metadata = String;
/// #     const NAME: &'static str = "registration_worker";
/// #     const MAX_ATTEMPTS: u16 = 2;
/// #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
/// #         ExecutionResult::Done
/// #     }
/// # }
/// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
/// let backend = InMemoryBackend::new();
/// Rexecutor::new(backend)
///     .with_executor::<RefreshWorker>()
///     .with_executor::<EmailScheduler>()
///     .with_executor::<RegistrationWorker>();
/// # });
/// ```
///
/// # Compile time scheduling of cron jobs
///
/// It can be useful to have jobs that run on a given schedule. Jobs like this can be setup using
/// either [`Rexecutor::with_cron_executor`] or [`Rexecutor::with_cron_executor_for_timezone`]. The
/// later is use to specify the specific timezone that the jobs should be scheduled to run in.
///
/// ## Example setting up a UTC cron job
///
/// To setup a cron jobs to run every day at midnight you can use the following code.
///
/// ```
/// # use rexecutor::prelude::*;
/// # use rexecutor::backend::{Backend, memory::InMemoryBackend};
/// struct CronJob;
/// #[async_trait::async_trait]
/// impl Executor for CronJob {
///     type Data = String;
///     type Metadata = ();
///     const NAME: &'static str = "cron_job";
///     const MAX_ATTEMPTS: u16 = 1;
///     async fn execute(job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
///         /// Do something important
///         ExecutionResult::Done
///     }
/// }
/// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
/// let schedule = cron::Schedule::try_from("0 0 0 * * *").unwrap();
///
/// let backend = InMemoryBackend::new();
/// Rexecutor::new(backend).with_cron_executor::<CronJob>(schedule, "important data".to_owned());
/// # });
/// ```
///
/// # Pruning jobs
///
/// After jobs have completed, been cancelled, or discarded it is useful to be able to clean up.
/// To setup the job pruner [`Rexecutor::with_job_pruner`] should be called passing in the given
/// [`PrunerConfig`].
///
/// Given the different ways in which jobs can finish it is often useful to be able to have fine
/// grained control over how old jobs should be cleaned up. [`PrunerConfig`] enables such control.
///
/// When constructing [`PrunerConfig`] a [`cron::Schedule`] is provided to specify when the pruner
/// should run.
///
/// Depending on the load/throughput of the system the pruner can be scheduled to run anywhere
/// from once a year through to multiple times per hour.
///
/// ## Example configuring the job pruner
///
/// ```
/// # use rexecutor::prelude::*;
/// # use std::str::FromStr;
/// # use chrono::TimeDelta;
/// # use rexecutor::backend::memory::InMemoryBackend;
/// # pub(crate) struct RefreshWorker;
/// # pub(crate) struct EmailScheduler;
/// # pub(crate) struct RegistrationWorker;
/// #
/// # #[async_trait::async_trait]
/// # impl Executor for RefreshWorker {
/// #     type Data = String;
/// #     type Metadata = String;
/// #     const NAME: &'static str = "refresh_worker";
/// #     const MAX_ATTEMPTS: u16 = 2;
/// #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
/// #         ExecutionResult::Done
/// #     }
/// # }
/// # #[async_trait::async_trait]
/// # impl Executor for EmailScheduler {
/// #     type Data = String;
/// #     type Metadata = String;
/// #     const NAME: &'static str = "email_scheduler";
/// #     const MAX_ATTEMPTS: u16 = 2;
/// #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
/// #         ExecutionResult::Done
/// #     }
/// # }
/// # #[async_trait::async_trait]
/// # impl Executor for RegistrationWorker {
/// #     type Data = String;
/// #     type Metadata = String;
/// #     const NAME: &'static str = "registration_worker";
/// #     const MAX_ATTEMPTS: u16 = 2;
/// #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
/// #         ExecutionResult::Done
/// #     }
/// # }
/// let config = PrunerConfig::new(cron::Schedule::from_str("0 0 * * * *").unwrap())
///     .with_max_concurrency(Some(2))
///     .with_pruner(
///         Pruner::max_age(TimeDelta::days(31), JobStatus::Complete)
///             .only::<RefreshWorker>()
///             .and::<EmailScheduler>(),
///     )
///     .with_pruner(
///         Pruner::max_length(200, JobStatus::Discarded)
///             .except::<RefreshWorker>()
///             .and::<EmailScheduler>(),
///     );
///
/// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
/// let backend = InMemoryBackend::new();
/// Rexecutor::new(backend)
///     .with_executor::<RefreshWorker>()
///     .with_executor::<EmailScheduler>()
///     .with_executor::<RegistrationWorker>()
///     .with_job_pruner(config);
/// # });
/// ```
///
/// # Shutting rexecutor down
///
/// To avoid jobs getting killed mid way through their executions it is important to make use of
/// graceful shutdown. This can either explicitly be called using [`Rexecutor::graceful_shutdown`],
/// or via use of the [`DropGuard`] obtained via [`Rexecutor::drop_guard`].
///
/// Using [`Rexecutor::graceful_shutdown`] or [`Rexecutor::drop_guard`] will ensure that all
/// currently executing jobs will be allowed time to complete before shutting rexecutor down.
///
/// ## Example using the `DropGuard`
///
/// ```
/// # use rexecutor::prelude::*;
/// # use std::str::FromStr;
/// # use chrono::TimeDelta;
/// # use rexecutor::backend::memory::InMemoryBackend;
/// # pub(crate) struct RefreshWorker;
/// # pub(crate) struct EmailScheduler;
/// # pub(crate) struct RegistrationWorker;
/// #
/// # #[async_trait::async_trait]
/// # impl Executor for RefreshWorker {
/// #     type Data = String;
/// #     type Metadata = String;
/// #     const NAME: &'static str = "refresh_worker";
/// #     const MAX_ATTEMPTS: u16 = 2;
/// #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
/// #         ExecutionResult::Done
/// #     }
/// # }
/// # #[async_trait::async_trait]
/// # impl Executor for EmailScheduler {
/// #     type Data = String;
/// #     type Metadata = String;
/// #     const NAME: &'static str = "email_scheduler";
/// #     const MAX_ATTEMPTS: u16 = 2;
/// #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
/// #         ExecutionResult::Done
/// #     }
/// # }
/// # #[async_trait::async_trait]
/// # impl Executor for RegistrationWorker {
/// #     type Data = String;
/// #     type Metadata = String;
/// #     const NAME: &'static str = "registration_worker";
/// #     const MAX_ATTEMPTS: u16 = 2;
/// #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
/// #         ExecutionResult::Done
/// #     }
/// # }
/// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
/// let backend = InMemoryBackend::new();
/// // Note this must be given a name to ensure it is dropped at the end of the scope.
/// // See https://doc.rust-lang.org/book/ch18-03-pattern-syntax.html#ignoring-an-unused-variable-by-starting-its-name-with-_
/// let _guard = Rexecutor::new(backend)
///     .with_executor::<RefreshWorker>()
///     .with_executor::<EmailScheduler>()
///     .with_executor::<RegistrationWorker>()
///     .drop_guard();
/// # });
/// ```
///
/// # Global backend
///
/// Rexecutor can be ran either with use of a global backend. This enables the use of the
/// convenience [`job::builder::JobBuilder::enqueue`] method which does not require a reference to
/// the backend to be passed down to the code that needs to enqueue a job.
///
/// The global backend can be set using [`Rexecutor::set_global_backend`] this should only be
/// called once otherwise it will return an error.
///
/// In fact for a single possible [`Rexecutor`] instance it is impossible to call this twice
///
/// ```compile_fail
/// # use rexecutor::prelude::*;
/// let backend = rexecutor::backend::memory::InMemoryBackend::new();
/// Rexecutor::new(backend).set_global_backend().set_global_backend();
/// ```
///
/// Note, using a global backend has many of the same drawbacks of any global variable in
/// particular it can make unit testing more difficult.
#[derive(Debug)]
#[allow(private_bounds)]
pub struct Rexecutor<B: Backend, State: InternalRexecutorState> {
    backend: B,
    cancellation_token: CancellationToken,
    _state: PhantomData<State>,
}

impl<B> Default for Rexecutor<B, GlobalUnset>
where
    B: Backend + Default,
{
    fn default() -> Self {
        Self::new(Default::default())
    }
}

impl<B> Rexecutor<B, GlobalUnset>
where
    B: Backend,
{
    /// Create an instance of [`Rexecutor`] using the given `backend`.
    pub fn new(backend: B) -> Self {
        Self {
            cancellation_token: Default::default(),
            backend,
            _state: PhantomData,
        }
    }
}

/// To enable automatic clean up this guard will shut down rexucutor and all associated tasks when
/// dropped.
#[allow(dead_code)]
pub struct DropGuard(tokio_util::sync::DropGuard);

impl<B> Rexecutor<B, GlobalUnset>
where
    B: Backend + Send + 'static + Sync + Clone,
{
    /// Sets the global backend to the backend associated with the current instance of
    /// [`Rexecutor`].
    ///
    /// This should only be called once. If called a second time it will return
    /// [`RexecutorError::GlobalBackend`].
    ///
    /// Calling this makes is possible to enqueue jobs without maintaining a reference to the
    /// backend throughout the codebase and enables the use of
    /// [`job::builder::JobBuilder::enqueue`].
    ///
    /// Note is is not possible to call this twice for the same [`Rexecutor`] instance
    ///
    /// ```compile_fail
    /// # use rexecutor::prelude::*;
    /// let backend = rexecutor::backend::memory::InMemoryBackend::new();
    /// Rexecutor::new(backend).set_global_backend().set_global_backend();
    /// ```
    pub fn set_global_backend(self) -> Result<Rexecutor<B, GlobalSet>, RexecutorError> {
        GlobalBackend::set(self.backend.clone())?;

        Ok(Rexecutor {
            cancellation_token: self.cancellation_token,
            backend: self.backend,
            _state: PhantomData,
        })
    }
}

#[allow(private_bounds)]
impl<B, State> Rexecutor<B, State>
where
    B: Backend + Send + 'static + Sync + Clone,
    State: InternalRexecutorState,
{
    /// Enable the execution of jobs for the provided [`Executor`].
    ///
    /// If this isn't called for an executor, then it's jobs will not be ran on this instance.
    /// This can be used to only run jobs on specific executor nodes.
    ///
    /// Jobs can still be enqueued to the backend without calling this method, but they will not be
    /// executed unless this method has been called for at least one instance of the running
    /// application.
    pub fn with_executor<E>(self) -> Self
    where
        E: Executor + 'static + Sync + Send,
        E::Data: Send + DeserializeOwned,
        E::Metadata: Serialize + DeserializeOwned + Send,
    {
        JobRunner::<B, E>::new(self.backend.clone()).spawn(self.cancellation_token.clone());
        self
    }

    /// Setup a cron job to run on the given schedule with the given data.
    ///
    /// Note this will run the schedule according to UTC. To schedule the job in another timezone use
    /// [`Rexecutor::with_cron_executor_for_timezone`].
    ///
    /// # Example
    ///
    /// To setup a cron jobs to run every day at midnight you can use the following code.
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    /// # use rexecutor::backend::{Backend, memory::InMemoryBackend};
    /// struct CronJob;
    /// #[async_trait::async_trait]
    /// impl Executor for CronJob {
    ///     type Data = String;
    ///     type Metadata = ();
    ///     const NAME: &'static str = "cron_job";
    ///     const MAX_ATTEMPTS: u16 = 1;
    ///     async fn execute(job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
    ///         /// Do something important
    ///         ExecutionResult::Done
    ///     }
    /// }
    /// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
    /// let schedule = cron::Schedule::try_from("0 0 0 * * *").unwrap();
    ///
    /// let backend = InMemoryBackend::new();
    /// Rexecutor::new(backend).with_cron_executor::<CronJob>(schedule, "important data".to_owned());
    /// # });
    /// ```
    pub fn with_cron_executor<E>(self, schedule: cron::Schedule, data: E::Data) -> Self
    where
        E: Executor + 'static + Sync + Send,
        E::Data: Send + Sync + Serialize + DeserializeOwned + Clone + Hash,
        E::Metadata: Serialize + DeserializeOwned + Send + Sync,
    {
        self.with_cron_executor_for_timezone::<E, _>(schedule, data, Utc)
    }

    /// Setup a cron job to run on the given schedule with the given data in the given timezome.
    ///
    /// # Example
    ///
    /// To setup a cron jobs to run every day at midnight you can use the following code.
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    /// # use rexecutor::backend::{Backend, memory::InMemoryBackend};
    /// struct CronJob;
    /// #[async_trait::async_trait]
    /// impl Executor for CronJob {
    ///     type Data = String;
    ///     type Metadata = ();
    ///     const NAME: &'static str = "cron_job";
    ///     const MAX_ATTEMPTS: u16 = 1;
    ///     async fn execute(job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
    ///         /// Do something important
    ///         ExecutionResult::Done
    ///     }
    /// }
    /// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
    /// let schedule = cron::Schedule::try_from("0 0 0 * * *").unwrap();
    ///
    /// let backend = InMemoryBackend::new();
    /// Rexecutor::new(backend).with_cron_executor_for_timezone::<CronJob, _>(
    ///     schedule,
    ///     "important data".to_owned(),
    ///     chrono::Local,
    /// );
    /// # });
    /// ```
    pub fn with_cron_executor_for_timezone<E, Z>(
        self,
        schedule: cron::Schedule,
        data: E::Data,
        timezone: Z,
    ) -> Self
    where
        E: Executor + 'static + Sync + Send,
        E::Data: Send + Sync + Serialize + DeserializeOwned + Clone + Hash,
        E::Metadata: Serialize + DeserializeOwned + Send + Sync,
        Z: TimeZone + Send + 'static,
    {
        CronRunner::<B, E>::new(self.backend.clone(), schedule, data)
            .spawn(timezone, self.cancellation_token.clone());

        self.with_executor::<E>()
    }

    /// Set the job pruner config.
    ///
    /// After jobs have completed, been cancelled, or discarded it is useful to be able to clean up.
    ///
    /// Given the different ways in which jobs can finish it is often useful to be able to have fine
    /// grained control over how old jobs should be cleaned up. [`PrunerConfig`] enables such control.
    ///
    /// When constructing [`PrunerConfig`] a [`cron::Schedule`] is provided to specify when the pruner
    /// should run.
    ///
    /// Depending on the load/throughput of the system the pruner can be scheduled to run anywhere
    /// from once a year through to multiple times per hour.
    ///
    /// # Example
    ///
    /// To remove all completed jobs more than a month old for both the `RefreshWorker` and
    /// `EmailScheduler` while only maintaining the last 200 discarded jobs for all executors expect
    /// the `EmailScheduler` and `RefreshWorker`, you can do the following:
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    /// # use std::str::FromStr;
    /// # use chrono::TimeDelta;
    /// # use rexecutor::backend::memory::InMemoryBackend;
    /// # pub(crate) struct RefreshWorker;
    /// # pub(crate) struct EmailScheduler;
    /// # pub(crate) struct RegistrationWorker;
    /// #
    /// # #[async_trait::async_trait]
    /// # impl Executor for RefreshWorker {
    /// #     type Data = String;
    /// #     type Metadata = String;
    /// #     const NAME: &'static str = "refresh_worker";
    /// #     const MAX_ATTEMPTS: u16 = 2;
    /// #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
    /// #         ExecutionResult::Done
    /// #     }
    /// # }
    /// # #[async_trait::async_trait]
    /// # impl Executor for EmailScheduler {
    /// #     type Data = String;
    /// #     type Metadata = String;
    /// #     const NAME: &'static str = "email_scheduler";
    /// #     const MAX_ATTEMPTS: u16 = 2;
    /// #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
    /// #         ExecutionResult::Done
    /// #     }
    /// # }
    /// # #[async_trait::async_trait]
    /// # impl Executor for RegistrationWorker {
    /// #     type Data = String;
    /// #     type Metadata = String;
    /// #     const NAME: &'static str = "registration_worker";
    /// #     const MAX_ATTEMPTS: u16 = 2;
    /// #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
    /// #         ExecutionResult::Done
    /// #     }
    /// # }
    /// let config = PrunerConfig::new(cron::Schedule::from_str("0 0 * * * *").unwrap())
    ///     .with_max_concurrency(Some(2))
    ///     .with_pruner(
    ///         Pruner::max_age(TimeDelta::days(31), JobStatus::Complete)
    ///             .only::<RefreshWorker>()
    ///             .and::<EmailScheduler>(),
    ///     )
    ///     .with_pruner(
    ///         Pruner::max_length(200, JobStatus::Discarded)
    ///             .except::<RefreshWorker>()
    ///             .and::<EmailScheduler>(),
    ///     );
    ///
    /// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
    /// let backend = InMemoryBackend::new();
    /// Rexecutor::new(backend)
    ///     .with_executor::<RefreshWorker>()
    ///     .with_executor::<EmailScheduler>()
    ///     .with_executor::<RegistrationWorker>()
    ///     .with_job_pruner(config);
    /// # });
    /// ```
    pub fn with_job_pruner(self, config: PrunerConfig) -> Self {
        PrunerRunner::new(self.backend.clone(), config).spawn(self.cancellation_token.clone());
        self
    }

    /// Instruct rexecutor to shutdown gracefully giving time for any currently executing jobs to
    /// complete before shutting down.
    pub fn graceful_shutdown(self) {
        tracing::debug!("Shutting down Rexecutor tasks");
        self.cancellation_token.cancel();
    }

    /// Returns a drop guard which will gracefully shutdown rexecutor when droped.
    ///
    /// # Example
    /// ```
    /// # use rexecutor::prelude::*;
    /// # use std::str::FromStr;
    /// # use chrono::TimeDelta;
    /// # use rexecutor::backend::memory::InMemoryBackend;
    /// # pub(crate) struct RefreshWorker;
    /// # pub(crate) struct EmailScheduler;
    /// # pub(crate) struct RegistrationWorker;
    /// #
    /// # #[async_trait::async_trait]
    /// # impl Executor for RefreshWorker {
    /// #     type Data = String;
    /// #     type Metadata = String;
    /// #     const NAME: &'static str = "refresh_worker";
    /// #     const MAX_ATTEMPTS: u16 = 2;
    /// #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
    /// #         ExecutionResult::Done
    /// #     }
    /// # }
    /// # #[async_trait::async_trait]
    /// # impl Executor for EmailScheduler {
    /// #     type Data = String;
    /// #     type Metadata = String;
    /// #     const NAME: &'static str = "email_scheduler";
    /// #     const MAX_ATTEMPTS: u16 = 2;
    /// #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
    /// #         ExecutionResult::Done
    /// #     }
    /// # }
    /// # #[async_trait::async_trait]
    /// # impl Executor for RegistrationWorker {
    /// #     type Data = String;
    /// #     type Metadata = String;
    /// #     const NAME: &'static str = "registration_worker";
    /// #     const MAX_ATTEMPTS: u16 = 2;
    /// #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
    /// #         ExecutionResult::Done
    /// #     }
    /// # }
    /// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
    /// let backend = InMemoryBackend::new();
    /// // Note this must be given a name to ensure it is dropped at the end of the scope.
    /// // See https://doc.rust-lang.org/book/ch18-03-pattern-syntax.html#ignoring-an-unused-variable-by-starting-its-name-with-_
    /// let _guard = Rexecutor::new(backend)
    ///     .with_executor::<RefreshWorker>()
    ///     .with_executor::<EmailScheduler>()
    ///     .with_executor::<RegistrationWorker>()
    ///     .drop_guard();
    /// # });
    #[must_use]
    pub fn drop_guard(self) -> DropGuard {
        DropGuard(self.cancellation_token.drop_guard())
    }
}

/// Errors that can occur when working with rexecutor.
// TODO: split errors
#[derive(Debug, Error)]
pub enum RexecutorError {
    /// Errors that result from the specific backend chosen.
    #[error("Error communicating with the backend")]
    BackendError(#[from] BackendError),

    /// This error results from trying to enqueue a job to the global backend when it is not set or
    /// when trying to set the global backend multiple times.
    #[error("Error setting global backend")]
    GlobalBackend,

    /// Error encoding or decoding data from json.
    #[error("Error encoding or decoding value")]
    EncodeError(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use super::*;
    use crate::{
        backend::{Job, MockBackend},
        executor::test::{MockError, MockExecutionResult, MockReturnExecutor},
        job::JobStatus,
        pruner::Pruner,
    };

    #[tokio::test]
    async fn run_job_error_in_stream() {
        let mut backend = MockBackend::default();
        let sender = backend.expect_subscribe_to_ready_jobs_with_stream();
        let backend = Arc::new(backend);

        let _guard = Rexecutor::<_, _>::new(backend.clone())
            .with_executor::<MockReturnExecutor>()
            .drop_guard();

        tokio::task::yield_now().await;

        sender.send(Err(BackendError::BadState)).unwrap();

        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn run_job_success() {
        let mut backend = MockBackend::default();
        backend.expect_mark_job_complete().returning(|_| Ok(()));

        let job = Job::mock_job::<MockReturnExecutor>().with_data(MockExecutionResult::Done);

        run_job(backend, job).await;
    }

    #[tokio::test]
    async fn run_job_success_error_marking_success() {
        let mut backend = MockBackend::default();
        backend
            .expect_mark_job_complete()
            .returning(|_| Err(BackendError::BadState));

        let job = Job::mock_job::<MockReturnExecutor>().with_data(MockExecutionResult::Done);

        run_job(backend, job).await;
    }

    #[tokio::test]
    async fn run_job_retryable() {
        let mut backend = MockBackend::default();
        backend
            .expect_mark_job_retryable()
            .returning(|_, _, _| Ok(()));

        let job = Job::mock_job::<MockReturnExecutor>().with_data(MockExecutionResult::Error {
            error: MockError("oh no".to_owned()),
        });

        run_job(backend, job).await;
    }

    #[tokio::test]
    async fn run_job_retryable_error_marking_retryable() {
        let mut backend = MockBackend::default();
        backend
            .expect_mark_job_retryable()
            .returning(|_, _, _| Err(BackendError::BadState));

        let job = Job::mock_job::<MockReturnExecutor>().with_data(MockExecutionResult::Error {
            error: MockError("oh no".to_owned()),
        });

        run_job(backend, job).await;
    }

    #[tokio::test]
    async fn run_job_retryable_timeout() {
        let mut backend = MockBackend::default();
        backend
            .expect_mark_job_retryable()
            .returning(|_, _, _| Ok(()));

        let job = Job::mock_job::<MockReturnExecutor>().with_data(MockExecutionResult::Timeout);

        run_job(backend, job).await;
    }

    #[tokio::test]
    async fn run_job_discarded() {
        let mut backend = MockBackend::default();
        backend.expect_mark_job_discarded().returning(|_, _| Ok(()));

        let job = Job::mock_job::<MockReturnExecutor>()
            .with_data(MockExecutionResult::Error {
                error: MockError("oh no".to_owned()),
            })
            .with_max_attempts(1)
            .with_attempt(1);

        run_job(backend, job).await;
    }

    #[tokio::test]
    async fn run_job_discarded_error_marking_job_discarded() {
        let mut backend = MockBackend::default();
        backend
            .expect_mark_job_discarded()
            .returning(|_, _| Err(BackendError::BadState));

        let job = Job::mock_job::<MockReturnExecutor>()
            .with_data(MockExecutionResult::Error {
                error: MockError("oh no".to_owned()),
            })
            .with_max_attempts(1)
            .with_attempt(1);

        run_job(backend, job).await;
    }

    #[tokio::test]
    async fn run_job_discarded_panic() {
        let mut backend = MockBackend::default();
        backend.expect_mark_job_discarded().returning(|_, _| Ok(()));

        let job = Job::mock_job::<MockReturnExecutor>()
            .with_data(MockExecutionResult::Panic)
            .with_max_attempts(1)
            .with_attempt(1);

        run_job(backend, job).await;
    }

    #[tokio::test]
    async fn run_job_discarded_panic_error_marking_job_discarded() {
        let mut backend = MockBackend::default();
        backend
            .expect_mark_job_discarded()
            .returning(|_, _| Err(BackendError::BadState));

        let job = Job::mock_job::<MockReturnExecutor>()
            .with_data(MockExecutionResult::Panic)
            .with_max_attempts(1)
            .with_attempt(1);

        run_job(backend, job).await;
    }

    #[tokio::test]
    async fn run_job_snoozed() {
        let mut backend = MockBackend::default();
        backend.expect_mark_job_snoozed().returning(|_, _| Ok(()));

        let job = Job::mock_job::<MockReturnExecutor>()
            .with_data(MockExecutionResult::Snooze {
                delay: Duration::from_secs(10),
            })
            .with_max_attempts(1)
            .with_attempt(1);

        run_job(backend, job).await;
    }

    #[tokio::test]
    async fn run_job_snoozed_error_marking_job_snoozed() {
        let mut backend = MockBackend::default();
        backend
            .expect_mark_job_snoozed()
            .returning(|_, _| Err(BackendError::BadState));

        let job = Job::mock_job::<MockReturnExecutor>()
            .with_data(MockExecutionResult::Snooze {
                delay: Duration::from_secs(10),
            })
            .with_max_attempts(1)
            .with_attempt(1);

        run_job(backend, job).await;
    }

    #[tokio::test]
    async fn run_job_cancelled() {
        let mut backend = MockBackend::default();
        backend.expect_mark_job_cancelled().returning(|_, _| Ok(()));

        let job = Job::mock_job::<MockReturnExecutor>()
            .with_data(MockExecutionResult::Cancelled {
                reason: "No need anymore".to_owned(),
            })
            .with_max_attempts(1)
            .with_attempt(1);

        run_job(backend, job).await;
    }

    #[tokio::test]
    async fn run_job_cancelled_error_marking_job_cancelled() {
        let mut backend = MockBackend::default();
        backend
            .expect_mark_job_cancelled()
            .returning(|_, _| Err(BackendError::BadState));

        let job = Job::mock_job::<MockReturnExecutor>()
            .with_data(MockExecutionResult::Cancelled {
                reason: "No need anymore".to_owned(),
            })
            .with_max_attempts(1)
            .with_attempt(1);

        run_job(backend, job).await;
    }

    #[tokio::test]
    async fn cron_job() {
        let every_second = cron::Schedule::try_from("* * * * * *").unwrap();
        let mut backend = MockBackend::default();
        let _sender = backend.expect_subscribe_to_ready_jobs_with_stream();
        backend.expect_enqueue().returning(|_| Ok(0.into()));
        let backend = Arc::new(backend);

        let _guard = Rexecutor::new(backend.clone())
            .with_cron_executor::<MockReturnExecutor>(every_second, MockExecutionResult::Done)
            .drop_guard();

        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn cron_job_error() {
        let every_second = cron::Schedule::try_from("* * * * * *").unwrap();
        let mut backend = MockBackend::default();
        let _sender = backend.expect_subscribe_to_ready_jobs_with_stream();
        backend
            .expect_enqueue()
            .returning(|_| Err(BackendError::BadState));
        let backend = Arc::new(backend);

        let _guard = Rexecutor::new(backend.clone())
            .with_cron_executor::<MockReturnExecutor>(every_second, MockExecutionResult::Done)
            .drop_guard();

        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn with_pruner() {
        let schedule = cron::Schedule::try_from("* * * * * *").unwrap();
        let mut backend = MockBackend::default();
        backend.expect_prune_jobs().returning(|_| Ok(()));
        let backend = Arc::new(backend);

        let pruner = PrunerConfig::new(schedule)
            .with_pruner(Pruner::max_length(5, JobStatus::Complete).only::<MockReturnExecutor>());

        let _guard = Rexecutor::new(backend.clone())
            .with_job_pruner(pruner)
            .drop_guard();

        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn with_pruner_error() {
        let schedule = cron::Schedule::try_from("* * * * * *").unwrap();
        let mut backend = MockBackend::default();
        backend
            .expect_prune_jobs()
            .returning(|_| Err(BackendError::BadState));
        let backend = Arc::new(backend);

        let pruner = PrunerConfig::new(schedule)
            .with_pruner(Pruner::max_length(5, JobStatus::Complete).only::<MockReturnExecutor>());

        let _guard = Rexecutor::new(backend.clone())
            .with_job_pruner(pruner)
            .drop_guard();

        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    async fn run_job(mut backend: MockBackend, job: Job) {
        let sender = backend.expect_subscribe_to_ready_jobs_with_stream();
        let backend = Arc::new(backend);
        let _guard = Rexecutor::new(backend.clone())
            .with_executor::<MockReturnExecutor>()
            .drop_guard();

        tokio::task::yield_now().await;
        sender.send(Ok(job)).unwrap();
        tokio::task::yield_now().await;
    }
}
