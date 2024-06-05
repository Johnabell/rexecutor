//! The API for configuring the job pruner.
//!
//! After jobs have completed, been cancelled, or discarded it is useful to be able to clean up.
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
//! [`PrunerConfig`] can have a number of individual [`Pruner`]s configured, each one specific for
//! cleaning up jobs matching specify criteria. This way fine grained control can be achieved to.
//! For example, is is possible to configure the pruner to achieve all of the following:
//!
//! - clean up completed jobs for a single executor so there is only 10 completed jobs at a time,
//! - clean up all completed jobs for all executors that are more than a month old,
//! - clean up cancelled jobs that are a year old, and
//! - keeping all discarded jobs indefinitely.
//!
//!
//! # Example
//!
//! To remove all completed jobs more than a month old for both the `RefreshWorker` and
//! `EmailScheduler` while only maintaining the last 200 discarded jobs for all executors expect
//! the `EmailScheduler` and `RefreshWorker`, you could use the following config.
//!
//! ```
//! # use rexecutor::prelude::*;
//! # use std::str::FromStr;
//! # use chrono::TimeDelta;
//! # pub(crate) struct RefreshWorker;
//! # pub(crate) struct EmailScheduler;
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
//! ```

use chrono::TimeDelta;

pub(crate) mod runner;

use crate::{executor::Executor, job::JobStatus};

/// Fine grained configuration for how jobs should be cleaned up.
///
/// When constructing [`PrunerConfig`] a [`cron::Schedule`]
/// is provided to specify when the pruner should run.
///
/// Depending on the load/throughput of the system the pruner can be scheduled to run anywhere
/// from once a year through to multiple times per hour.
///
/// [`PrunerConfig`] can have a number of individual [`Pruner`]s configured, each one specific for
/// cleaning up jobs matching specify criteria. This way fine grained control can be achieved to.
/// For example, is is possible to configure the pruner to achieve all of the following:
///
/// - clean up completed jobs for a single executor so there is only 10 completed jobs
///   at a time,
/// - clean up all completed jobs for all executors that are more than a month old
/// - clean up cancelled jobs that are a year old
/// - keeping all discarded jobs indefinitely
///
/// Once constructed, [`PrunerConfig`] it should be passed to [`crate::Rexecutor::with_job_pruner`].
///
/// # Example
///
/// To remove all completed jobs more than a month old for both the `RefreshWorker` and
/// `EmailScheduler` while only maintaining the last 200 discarded jobs for all executors expect
/// the `EmailScheduler` and `RefreshWorker`, you could use the following config.
///
/// ```
/// # use rexecutor::prelude::*;
/// # use std::str::FromStr;
/// # use chrono::TimeDelta;
/// # pub(crate) struct RefreshWorker;
/// # pub(crate) struct EmailScheduler;
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
/// let config = PrunerConfig::new(cron::Schedule::from_str("0 0 * * * *").unwrap())
///     .with_max_concurrency(Some(2))
///     .with_pruners([
///         Pruner::max_age(TimeDelta::days(31), JobStatus::Complete)
///             .only::<RefreshWorker>()
///             .and::<EmailScheduler>(),
///         Pruner::max_length(200, JobStatus::Discarded)
///             .only::<RefreshWorker>()
///             .and::<EmailScheduler>(),
///     ]);
/// ```
pub struct PrunerConfig {
    schedule: cron::Schedule,
    max_concurrency: Option<usize>,
    pruners: Vec<PruneSpec>,
}

impl PrunerConfig {
    /// Construct a new instance of [`PrunerConfig`] scheduled to run on the provided cron
    /// schedule.
    pub fn new(schedule: cron::Schedule) -> Self {
        Self {
            schedule,
            max_concurrency: Some(10),
            pruners: Default::default(),
        }
    }

    /// Specify the maximum number of pruners that should be ran simultaneously.
    ///
    /// If the pruner runs when the rest of the system is busy, it might be advisable to limit the
    /// concurrency of the pruner to avoid too much load on the backend.
    pub fn with_max_concurrency(mut self, limit: Option<usize>) -> Self {
        self.max_concurrency = limit;
        self
    }

    /// Add a single [`Pruner`] to the config.
    #[allow(private_bounds)]
    pub fn with_pruner<T>(mut self, pruner: Pruner<T>) -> Self
    where
        T: IntoSpec,
    {
        self.pruners.push(pruner.into());
        self
    }

    /// Add multiple [`Pruner`]s of a single type to the config.
    #[allow(private_bounds)]
    pub fn with_pruners<T>(mut self, pruner: impl IntoIterator<Item = Pruner<T>>) -> Self
    where
        T: IntoSpec,
    {
        self.pruners.extend(pruner.into_iter().map(Into::into));
        self
    }
}

/// The specification of a single pruner for consumption by the backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PruneSpec {
    /// The status of the jobs that should be affected by this pruner.
    pub status: JobStatus,
    /// The particular pruning strategy to apply, either max length or max age.
    pub prune_by: PruneBy,
    /// The specification of the executors to be affected by this pruner.
    pub executors: Spec,
}

impl<T> From<Pruner<T>> for PruneSpec
where
    T: IntoSpec,
{
    fn from(value: Pruner<T>) -> Self {
        Self {
            status: value.status,
            prune_by: value.prune_by,
            executors: value.executors.into_spec(),
        }
    }
}

/// Configuration for a single pruner.
///
/// Pruners can either be configured to prune by the age of jobs constructed via
/// [`Pruner::max_age`] or the total number of job via [`Pruner::max_length`].
///
/// By default when constructed a pruner will prune jobs for all executors. To only prune jobs
/// related to a specific set of of executors, you can use [`Pruner::only`] followed by
/// [`Pruner::and`].
///
/// Similarly, to prune jobs for all but a specified set of executors, you can call
/// [`Pruner::except`] followed by [`Pruner::and`].
///
/// # Example
///
/// To prune all completed jobs which are older than 2 weeks:
///
/// ```
/// # use rexecutor::prelude::*;
/// # use chrono::TimeDelta;
/// let pruner = Pruner::max_age(TimeDelta::days(14), JobStatus::Complete);
/// ```
///
/// Similarly to only prune a specific set of executors:
///
/// ```
/// # use rexecutor::prelude::*;
/// # pub(crate) struct RefreshWorker;
/// # pub(crate) struct EmailScheduler;
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
/// let pruner = Pruner::max_length(200, JobStatus::Discarded)
///     .only::<RefreshWorker>()
///     .and::<EmailScheduler>();
/// ```
#[allow(private_bounds)]
pub struct Pruner<T>
where
    T: IntoSpec,
{
    status: JobStatus,
    prune_by: PruneBy,
    executors: T,
}

impl Pruner<All> {
    /// Constructs a pruner that will prune jobs on the bases of their age.
    pub const fn max_age(age: TimeDelta, status: JobStatus) -> Self {
        Self {
            status,
            prune_by: PruneBy::MaxAge(age),
            executors: All,
        }
    }

    /// Constructs a pruner that will prune jobs on the bases of the number of matching jobs.
    pub const fn max_length(length: u32, status: JobStatus) -> Self {
        Self {
            status,
            prune_by: PruneBy::MaxLength(length),
            executors: All,
        }
    }

    /// Restrict this pruner to only prune jobs for the given [`Executor`].
    pub fn only<E: Executor>(self) -> Pruner<Only> {
        Pruner {
            status: self.status,
            prune_by: self.prune_by,
            executors: Only(vec![E::NAME]),
        }
    }

    /// Restrict this pruner to not prune jobs for the given [`Executor`].
    pub fn except<E: Executor>(self) -> Pruner<Except> {
        Pruner {
            status: self.status,
            prune_by: self.prune_by,
            executors: Except(vec![E::NAME]),
        }
    }
}

impl Pruner<Only> {
    /// Additionally prune jobs for the given [`Executor`].
    pub fn and<E: Executor>(mut self) -> Self {
        self.executors.0.push(E::NAME);
        self
    }
}

impl Pruner<Except> {
    /// Add another [`Executor`] to the exclusion list for this pruner.
    pub fn and<E: Executor>(mut self) -> Self {
        self.executors.0.push(E::NAME);
        self
    }
}

/// The strategy to prune by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PruneBy {
    /// Each time the pruner is ran it should remove all jobs older than then given [`TimeDelta`]
    MaxAge(TimeDelta),
    /// Each time the pruner runs it should ensure that the max length of remaining jobs is equal
    /// to the given length.
    ///
    /// It should prune the oldest jobs first.
    MaxLength(u32),
}

/// An exclusion/inclusion specification.
///
/// For the pruner this should specify whether the pruner should prune on the basis of exception or
/// inclusion in the given [`Vec`]s.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Spec {
    /// Prune jobs for all executors except those given.
    Except(Vec<&'static str>),
    /// Prune jobs only for the executors provided.
    Only(Vec<&'static str>),
}

trait IntoSpec {
    fn into_spec(self) -> Spec;
}
#[doc(hidden)]
pub struct All;
impl IntoSpec for All {
    fn into_spec(self) -> Spec {
        Spec::Except(Vec::new())
    }
}
#[doc(hidden)]
pub struct Except(Vec<&'static str>);
impl IntoSpec for Except {
    fn into_spec(self) -> Spec {
        Spec::Except(self.0)
    }
}
#[doc(hidden)]
pub struct Only(Vec<&'static str>);
impl IntoSpec for Only {
    fn into_spec(self) -> Spec {
        Spec::Only(self.0)
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use crate::executor::test::{MockReturnExecutor, SimpleExecutor};

    use super::*;

    #[test]
    fn config() {
        let config = PrunerConfig::new(cron::Schedule::from_str("0 0 * * * *").unwrap())
            .with_pruner(
                Pruner::max_age(TimeDelta::days(31), JobStatus::Complete)
                    .only::<SimpleExecutor>()
                    .and::<MockReturnExecutor>(),
            )
            .with_pruner(
                Pruner::max_length(200, JobStatus::Discarded)
                    .except::<SimpleExecutor>()
                    .and::<MockReturnExecutor>(),
            );

        assert_eq!(config.pruners.len(), 2);
    }

    #[test]
    fn into_spec_all() {
        let spec = IntoSpec::into_spec(All);
        assert_eq!(spec, Spec::Except(Vec::new()));
    }
}
