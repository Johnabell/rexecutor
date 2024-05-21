//! The mechanism for specifying uniqueness for jobs and the resolution on conflict.
//!
//! # Specifying uniqueness criteria
//!
//! [`UniquenessCriteria`] lets you specify constraints to prevent enqueueing duplicate jobs.
//!
//! Uniqueness is can be determined any combination of the following criterion
//!
//! - executor: via [`crate::Executor::NAME`]
//! - duration: to only apply uniqueness criteria to jobs enqueued within a given time frame.
//! - the [`JobStatus`] the combination of statuses to apply the criteria to.
//! - a [`Hash`]able key for specifying an arbitrary condition on which to apply uniqueness.
//!
//! # Resolving conflicts
//!
//! Given a conflict it is possible to specify what action should be taken.
//!
//! The default behaviour is to do nothing, i.e. to leave the current job unaffected and skip
//! inserting a duplicate. This is the same as explicitly calling
//! [`UniquenessCriteria::on_conflict_do_nothing`].
//!
//! However, it is also possible to override any combination of the following details of the
//! previously inserted job:
//!
//! - scheduled_at: the time which the jobs should be executed at
//! - data: replacing the data with the next value
//! - metadata: replacing the metadata with the next value
//! - priority: updating the job priority
//! - max_attempts: updating the maximum number of attempts for the job
//!
//! Additionally, it is possible to only override these values when the status is within a
//! specified set of [`JobStatus`]es.
//!
//! # Example
//!
//! ```
//! # use rexecutor::prelude::*;
//! # use chrono::TimeDelta;
//! let uniqueness_criteria = UniquenessCriteria::by_executor()
//!     .and_within(TimeDelta::seconds(60))
//!     .on_conflict(
//!         Replace::priority()
//!             .and_metadata()
//!             .for_statuses(&JobStatus::ALL),
//!     );
//!
//! assert!(uniqueness_criteria.executor);
//! assert_eq!(uniqueness_criteria.duration, Some(TimeDelta::seconds(60)));
//! assert_eq!(uniqueness_criteria.statuses, &[JobStatus::Scheduled]);
//! ```
use chrono::TimeDelta;
use std::hash::Hash;

use crate::job::JobStatus;

/// [`UniquenessCriteria`] lets you specify constraints to prevent enqueueing duplicate jobs.
///
/// Uniqueness is can be determined any combination of the following criterion
/// - executor: via [`crate::Executor::NAME`]
/// - duration: to only apply uniqueness criteria to jobs enqueued within a given time frame.
/// - the [`JobStatus`] the combination of statuses to apply the criteria to.
/// - a [`Hash`]able key for specifying an arbitrary condition on which to apply uniqueness.
///
/// For each possible constraint the API provides a constructor method:
///
/// - [`UniquenessCriteria::by_executor`]
/// - [`UniquenessCriteria::within`]
/// - [`UniquenessCriteria::by_statuses`]
/// - [`UniquenessCriteria::by_key`]
///
/// and a combination method all prefixed by `and_`:
///
/// - [`UniquenessCriteria::and_executor`]
/// - [`UniquenessCriteria::and_within`]
/// - [`UniquenessCriteria::and_statuses`]
/// - [`UniquenessCriteria::and_key`]
///
/// The conflict resolution behaviour is specified using on of the two methods:
///
/// - [`UniquenessCriteria::on_conflict_do_nothing`] (the default behaviour).
/// - [`UniquenessCriteria::on_conflict`]
///
/// # Example
///
/// ```
/// # use rexecutor::prelude::*;
/// let uniqueness_criteria = UniquenessCriteria::by_executor()
///     .and_key(&"Special uniqueness identifier")
///     .and_statuses(&[JobStatus::Scheduled, JobStatus::Retryable])
///     .on_conflict(Replace::data());
///
/// assert!(uniqueness_criteria.executor);
/// assert!(uniqueness_criteria.key.is_some());
/// assert_eq!(
///     uniqueness_criteria.statuses,
///     &[JobStatus::Scheduled, JobStatus::Retryable]
/// );
/// ```
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct UniquenessCriteria<'a> {
    /// The unique key used to determine whether a job is a duplicate.
    ///
    /// If [`None`], then no key will be used and the jobs will be compared against all other jobs
    /// matching the remainder of the uniqueness criteria.
    pub key: Option<i64>,
    /// Whether the uniqueness criteria should be restricted to jobs from the same executor if
    /// false the job will be compared with all other jobs from all executors.
    pub executor: bool,
    /// The duration within which to consider a job to be a duplicate.
    ///
    /// If [`None`], this will be for all time (specifically, for all jobs that have not been
    /// cleaned up by the pruner.
    pub duration: Option<TimeDelta>,
    /// Which statuses should be considered when determining if a job is a duplicate.
    pub statuses: &'a [JobStatus],
    /// The action to take when a conflicting job is found.
    ///
    /// Defaults to [`Resolution::DoNothing`].
    pub on_conflict: Resolution<'a>,
}

/// The action to take when a conflict is found.
///
/// This will not generally be constructed directly  instead it will be constructed as a result of
/// calling on of:
///
/// - [`UniquenessCriteria::on_conflict_do_nothing`]
/// - [`UniquenessCriteria::on_conflict`]
///
/// The default is to do nothing, i.e. to leave the current job unaffected and skip
/// inserting a duplicate.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub enum Resolution<'a> {
    /// Take no action when a duplicate is found i.e. to leave the current job unaffected and skip
    /// the insert to avoid creating a duplicate job.
    ///
    /// This is the default.
    #[default]
    DoNothing,
    /// Replace certain aspects of the job determined by [`Replace`].
    Replace(Replace<'a>),
}

/// Determines what should be updated about the current job when a conflict is found.
///
/// Any combination of the following can be specified as to what should be replaced.
///
/// - scheduled_at: the time which the jobs should be executed at
/// - data: replacing the data with the next value
/// - metadata: replacing the metadata with the next value
/// - priority: updating the job priority
/// - max_attempts: updating the maximum number of attempts for the job
///
/// For each possible action the API provides a constructor method:
///
/// - [`Replace::scheduled_at`]
/// - [`Replace::data`]
/// - [`Replace::metadata`]
/// - [`Replace::priority`]
/// - [`Replace::max_attempts`]
///
/// and a combination method all prefixed by `and_`:
///
/// - [`Replace::and_scheduled_at`]
/// - [`Replace::and_data`]
/// - [`Replace::and_metadata`]
/// - [`Replace::and_priority`]
/// - [`Replace::and_max_attempts`]
///
/// It is also possible to specify which should statuses should be affected by the update using
/// [`Replace::for_statuses`]. The default is to only replace values for [`JobStatus::Scheduled`].
///
/// # Example
///
/// ```
/// # use rexecutor::prelude::*;
///
/// let replace = Replace::data()
///     .and_metadata()
///     .and_priority()
///     .for_statuses(&[JobStatus::Scheduled, JobStatus::Retryable]);
///
/// assert!(replace.data);
/// assert!(replace.metadata);
/// assert!(replace.priority);
/// assert!(!replace.scheduled_at);
/// assert!(!replace.max_attempts);
///
/// assert_eq!(
///     replace.for_statuses,
///     &[JobStatus::Scheduled, JobStatus::Retryable]
/// );
/// ```
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct Replace<'a> {
    /// Determines whether the job's scheduled_at field should be overriden when a conflict is
    /// detected.
    pub scheduled_at: bool,
    /// Determines whether the job's data should be overriden when a conflict is detected.
    pub data: bool,
    /// Determines whether the job's metadata should be overriden when a conflict is detected.
    pub metadata: bool,
    /// Determines whether the job's priority should be overriden when a conflict is detected.
    pub priority: bool,
    /// Determines whether the job's max_attempts should be overriden when a conflict is detected.
    pub max_attempts: bool,
    /// Determines for which [`JobStatus`]es the the specified values should be updated.
    pub for_statuses: &'a [JobStatus],
}

impl<'a> Replace<'a> {
    /// Constructs a [`Replace`] specifying that a job's scheduled_at field should be updated when
    /// a conflict is detected.
    ///
    /// # Example
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    ///
    /// let replace = Replace::scheduled_at();
    ///
    /// assert!(replace.scheduled_at);
    /// assert!(!replace.data);
    /// assert!(!replace.metadata);
    /// assert!(!replace.priority);
    /// assert!(!replace.max_attempts);
    /// assert_eq!(replace.for_statuses, &[JobStatus::Scheduled]);
    /// ```
    pub const fn scheduled_at() -> Self {
        Self {
            scheduled_at: true,
            data: false,
            metadata: false,
            priority: false,
            max_attempts: false,
            for_statuses: &[JobStatus::Scheduled],
        }
    }

    /// Constructs a [`Replace`] specifying that a job's data should be updated when
    /// a conflict is detected.
    ///
    /// # Example
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    ///
    /// let replace = Replace::data();
    ///
    /// assert!(!replace.scheduled_at);
    /// assert!(replace.data);
    /// assert!(!replace.metadata);
    /// assert!(!replace.priority);
    /// assert!(!replace.max_attempts);
    /// assert_eq!(replace.for_statuses, &[JobStatus::Scheduled]);
    /// ```
    pub const fn data() -> Self {
        Self {
            scheduled_at: false,
            data: true,
            metadata: false,
            priority: false,
            max_attempts: false,
            for_statuses: &[JobStatus::Scheduled],
        }
    }

    /// Constructs a [`Replace`] specifying that a job's metadata should be updated when
    /// a conflict is detected.
    ///
    /// # Example
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    ///
    /// let replace = Replace::metadata();
    ///
    /// assert!(!replace.scheduled_at);
    /// assert!(!replace.data);
    /// assert!(replace.metadata);
    /// assert!(!replace.priority);
    /// assert!(!replace.max_attempts);
    /// assert_eq!(replace.for_statuses, &[JobStatus::Scheduled]);
    /// ```
    pub const fn metadata() -> Self {
        Self {
            scheduled_at: false,
            data: false,
            metadata: true,
            priority: false,
            max_attempts: false,
            for_statuses: &[JobStatus::Scheduled],
        }
    }

    /// Constructs a [`Replace`] specifying that a job's priority should be updated when
    /// a conflict is detected.
    ///
    /// # Example
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    ///
    /// let replace = Replace::priority();
    ///
    /// assert!(!replace.scheduled_at);
    /// assert!(!replace.data);
    /// assert!(!replace.metadata);
    /// assert!(replace.priority);
    /// assert!(!replace.max_attempts);
    /// assert_eq!(replace.for_statuses, &[JobStatus::Scheduled]);
    /// ```
    pub const fn priority() -> Self {
        Self {
            scheduled_at: false,
            data: false,
            metadata: false,
            priority: true,
            max_attempts: false,
            for_statuses: &[JobStatus::Scheduled],
        }
    }

    /// Constructs a [`Replace`] specifying that a job's max_attempts should be updated when
    /// a conflict is detected.
    ///
    /// # Example
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    ///
    /// let replace = Replace::max_attempts();
    ///
    /// assert!(!replace.scheduled_at);
    /// assert!(!replace.data);
    /// assert!(!replace.metadata);
    /// assert!(!replace.priority);
    /// assert!(replace.max_attempts);
    /// assert_eq!(replace.for_statuses, &[JobStatus::Scheduled]);
    /// ```
    pub const fn max_attempts() -> Self {
        Self {
            scheduled_at: false,
            data: false,
            metadata: false,
            priority: false,
            max_attempts: true,
            for_statuses: &[JobStatus::Scheduled],
        }
    }

    /// Specifies that a job's scheduled_at field should also be updated when a conflict is
    /// detected.
    ///
    /// # Example
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    ///
    /// let replace = Replace::max_attempts().and_scheduled_at();
    ///
    /// assert!(replace.scheduled_at);
    /// assert!(!replace.data);
    /// assert!(!replace.metadata);
    /// assert!(!replace.priority);
    /// assert!(replace.max_attempts);
    /// assert_eq!(replace.for_statuses, &[JobStatus::Scheduled]);
    /// ```
    pub const fn and_scheduled_at(mut self) -> Self {
        self.scheduled_at = true;
        self
    }

    /// Specifies that a job's data should also be updated when a conflict is detected.
    ///
    /// # Example
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    ///
    /// let replace = Replace::max_attempts().and_data();
    ///
    /// assert!(!replace.scheduled_at);
    /// assert!(replace.data);
    /// assert!(!replace.metadata);
    /// assert!(!replace.priority);
    /// assert!(replace.max_attempts);
    /// assert_eq!(replace.for_statuses, &[JobStatus::Scheduled]);
    /// ```
    pub const fn and_data(mut self) -> Self {
        self.data = true;
        self
    }

    /// Specifies that a job's metadata should also be updated when a conflict is detected.
    ///
    /// # Example
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    ///
    /// let replace = Replace::max_attempts().and_metadata();
    ///
    /// assert!(!replace.scheduled_at);
    /// assert!(!replace.data);
    /// assert!(replace.metadata);
    /// assert!(!replace.priority);
    /// assert!(replace.max_attempts);
    /// assert_eq!(replace.for_statuses, &[JobStatus::Scheduled]);
    /// ```
    pub const fn and_metadata(mut self) -> Self {
        self.metadata = true;
        self
    }

    /// Specifies that a job's priority should also be updated when a conflict is detected.
    ///
    /// # Example
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    ///
    /// let replace = Replace::max_attempts().and_priority();
    ///
    /// assert!(!replace.scheduled_at);
    /// assert!(!replace.data);
    /// assert!(!replace.metadata);
    /// assert!(replace.priority);
    /// assert!(replace.max_attempts);
    /// assert_eq!(replace.for_statuses, &[JobStatus::Scheduled]);
    /// ```
    pub const fn and_priority(mut self) -> Self {
        self.priority = true;
        self
    }

    /// Specifies that a job's priority should also be updated when a conflict is detected.
    ///
    /// # Example
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    ///
    /// let replace = Replace::scheduled_at().and_max_attempts();
    ///
    /// assert!(replace.scheduled_at);
    /// assert!(!replace.data);
    /// assert!(!replace.metadata);
    /// assert!(!replace.priority);
    /// assert!(replace.max_attempts);
    /// assert_eq!(replace.for_statuses, &[JobStatus::Scheduled]);
    /// ```
    pub const fn and_max_attempts(mut self) -> Self {
        self.max_attempts = true;
        self
    }

    /// Specifies that the given update should only be applied for jobs whose statuses match one of
    /// those of the provided statuses.
    ///
    /// # Example
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    ///
    /// let replace = Replace::scheduled_at()
    ///     .and_data()
    ///     .and_metadata()
    ///     .for_statuses(&[JobStatus::Scheduled, JobStatus::Retryable]);
    ///
    /// assert!(replace.scheduled_at);
    /// assert!(replace.data);
    /// assert!(replace.metadata);
    /// assert!(!replace.priority);
    /// assert!(!replace.max_attempts);
    /// assert_eq!(
    ///     replace.for_statuses,
    ///     &[JobStatus::Scheduled, JobStatus::Retryable]
    /// );
    /// ```
    pub const fn for_statuses(mut self, statuses: &'a [JobStatus]) -> Self {
        self.for_statuses = statuses;
        self
    }
}

impl<'a> UniquenessCriteria<'a> {
    /// Constructs a [`UniquenessCriteria`] specifying that a duplicate jobs should only be
    /// detected for jobs for the same [`crate::Executor`].
    ///
    /// Note: by default this will only match jobs of with status [`JobStatus::Scheduled`].
    ///
    /// # Example
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    /// # use rexecutor::job::uniqueness_criteria::Resolution;
    ///
    /// let uniqueness_criteria = UniquenessCriteria::by_executor();
    ///
    /// assert!(uniqueness_criteria.executor);
    /// assert!(uniqueness_criteria.key.is_none());
    /// assert!(uniqueness_criteria.duration.is_none());
    /// assert_eq!(uniqueness_criteria.statuses, &[JobStatus::Scheduled]);
    /// assert_eq!(uniqueness_criteria.on_conflict, Resolution::DoNothing);
    /// ```
    pub const fn by_executor() -> Self {
        Self {
            key: None,
            executor: true,
            duration: None,
            statuses: &[JobStatus::Scheduled],
            on_conflict: Resolution::DoNothing,
        }
    }

    /// Constructs a [`UniquenessCriteria`] specifying that a duplicate jobs should only be
    /// detected for jobs within the given duration.
    ///
    /// Note: by default this will only match jobs of with status [`JobStatus::Scheduled`].
    ///
    /// # Example
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    /// # use rexecutor::job::uniqueness_criteria::Resolution;
    /// # use chrono::TimeDelta;
    ///
    /// let uniqueness_criteria = UniquenessCriteria::within(TimeDelta::seconds(60));
    ///
    /// assert!(!uniqueness_criteria.executor);
    /// assert!(uniqueness_criteria.key.is_none());
    /// assert_eq!(uniqueness_criteria.duration, Some(TimeDelta::seconds(60)));
    /// assert_eq!(uniqueness_criteria.statuses, &[JobStatus::Scheduled]);
    /// assert_eq!(uniqueness_criteria.on_conflict, Resolution::DoNothing);
    /// ```
    pub const fn within(duration: TimeDelta) -> Self {
        Self {
            key: None,
            executor: false,
            duration: Some(duration),
            statuses: &[JobStatus::Scheduled],
            on_conflict: Resolution::DoNothing,
        }
    }

    /// Constructs a [`UniquenessCriteria`] specifying that a duplicate jobs should only be
    /// detected for jobs matching one of the given statuses.
    ///
    /// # Example
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    /// # use rexecutor::job::uniqueness_criteria::Resolution;
    ///
    /// let uniqueness_criteria = UniquenessCriteria::by_statuses(&[JobStatus::Scheduled]);
    ///
    /// assert!(!uniqueness_criteria.executor);
    /// assert!(uniqueness_criteria.key.is_none());
    /// assert!(uniqueness_criteria.duration.is_none());
    /// assert_eq!(uniqueness_criteria.statuses, &[JobStatus::Scheduled]);
    /// assert_eq!(uniqueness_criteria.on_conflict, Resolution::DoNothing);
    /// ```
    pub const fn by_statuses(statuses: &'a [JobStatus]) -> Self {
        Self {
            key: None,
            executor: false,
            duration: None,
            statuses,
            on_conflict: Resolution::DoNothing,
        }
    }

    /// Constructs a [`UniquenessCriteria`] specifying that a duplicate jobs should only be
    /// detected for jobs with the given uniqueness key.
    ///
    /// Note: by default this will only match jobs of with status [`JobStatus::Scheduled`].
    ///
    /// # Example
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    /// # use rexecutor::job::uniqueness_criteria::Resolution;
    ///
    /// let uniqueness_criteria = UniquenessCriteria::by_key(&"Some hashable value");
    ///
    /// assert!(!uniqueness_criteria.executor);
    /// assert!(uniqueness_criteria.key.is_some());
    /// assert!(uniqueness_criteria.duration.is_none());
    /// assert_eq!(uniqueness_criteria.statuses, &[JobStatus::Scheduled]);
    /// assert_eq!(uniqueness_criteria.on_conflict, Resolution::DoNothing);
    /// ```
    pub fn by_key<H: Hash>(value: &H) -> Self {
        let key = fxhash::hash64(value);
        Self {
            key: Some(key as i64),
            executor: false,
            duration: None,
            statuses: &[JobStatus::Scheduled],
            on_conflict: Resolution::DoNothing,
        }
    }

    /// Specifies that jobs should be only be considered a duplicate if in addition
    /// it is for the same executor.
    ///
    /// # Example
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    /// # use rexecutor::job::uniqueness_criteria::Resolution;
    ///
    /// let uniqueness_criteria = UniquenessCriteria::by_key(
    ///     &"Some hashable
    /// value",
    /// )
    /// .and_executor();
    ///
    /// assert!(uniqueness_criteria.executor);
    /// assert!(uniqueness_criteria.key.is_some());
    /// assert!(uniqueness_criteria.duration.is_none());
    /// assert_eq!(uniqueness_criteria.statuses, &[JobStatus::Scheduled]);
    /// assert_eq!(uniqueness_criteria.on_conflict, Resolution::DoNothing);
    /// ```
    pub const fn and_executor(mut self) -> Self {
        self.executor = true;
        self
    }

    /// Specifies that jobs should be only be considered a duplicate if in addition
    /// it is within the given duration.
    ///
    /// # Example
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    /// # use rexecutor::job::uniqueness_criteria::Resolution;
    /// # use chrono::TimeDelta;
    ///
    /// let uniqueness_criteria = UniquenessCriteria::by_key(
    ///     &"Some hashable
    /// value",
    /// )
    /// .and_within(TimeDelta::hours(1));
    ///
    /// assert!(!uniqueness_criteria.executor);
    /// assert!(uniqueness_criteria.key.is_some());
    /// assert_eq!(uniqueness_criteria.duration, Some(TimeDelta::hours(1)));
    /// assert_eq!(uniqueness_criteria.statuses, &[JobStatus::Scheduled]);
    /// assert_eq!(uniqueness_criteria.on_conflict, Resolution::DoNothing);
    /// ```
    pub const fn and_within(mut self, duration: TimeDelta) -> Self {
        self.duration = Some(duration);
        self
    }

    /// Specifies that jobs should be only be considered a duplicate if in addition
    /// it is matches one of the given statuses.
    ///
    /// # Example
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    /// # use rexecutor::job::uniqueness_criteria::Resolution;
    ///
    /// let uniqueness_criteria = UniquenessCriteria::by_key(
    ///     &"Some hashable
    /// value",
    /// )
    /// .and_statuses(&[JobStatus::Retryable]);
    ///
    /// assert!(!uniqueness_criteria.executor);
    /// assert!(uniqueness_criteria.key.is_some());
    /// assert!(uniqueness_criteria.duration.is_none());
    /// assert_eq!(uniqueness_criteria.statuses, &[JobStatus::Retryable]);
    /// assert_eq!(uniqueness_criteria.on_conflict, Resolution::DoNothing);
    /// ```
    pub const fn and_statuses(mut self, statuses: &'a [JobStatus]) -> Self {
        self.statuses = statuses;
        self
    }

    /// Specifies that jobs should be only be considered a duplicate if in addition
    /// it is has a matching uniqueness key.
    ///
    /// # Example
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    /// # use rexecutor::job::uniqueness_criteria::Resolution;
    ///
    /// let uniqueness_criteria = UniquenessCriteria::by_executor().and_key(&10985109);
    ///
    /// assert!(uniqueness_criteria.executor);
    /// assert!(uniqueness_criteria.key.is_some());
    /// assert!(uniqueness_criteria.duration.is_none());
    /// assert_eq!(uniqueness_criteria.statuses, &[JobStatus::Scheduled]);
    /// assert_eq!(uniqueness_criteria.on_conflict, Resolution::DoNothing);
    /// ```
    pub fn and_key<H: Hash>(mut self, value: &H) -> Self {
        let key = fxhash::hash64(value);
        self.key = Some(key as i64);
        self
    }

    /// Specifies that when a duplicate job is found that it should be updated according to what is
    /// specified in the given [`Replace`].
    ///
    /// # Example
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    /// # use rexecutor::job::uniqueness_criteria::Resolution;
    ///
    /// let uniqueness_criteria = UniquenessCriteria::by_executor()
    ///     .and_key(&10985109)
    ///     .on_conflict(Replace::data());
    ///
    /// assert!(uniqueness_criteria.executor);
    /// assert!(uniqueness_criteria.key.is_some());
    /// assert!(uniqueness_criteria.duration.is_none());
    /// assert_eq!(uniqueness_criteria.statuses, &[JobStatus::Scheduled]);
    /// assert_eq!(
    ///     uniqueness_criteria.on_conflict,
    ///     Resolution::Replace(Replace::data())
    /// );
    /// ```
    pub const fn on_conflict(mut self, replace: Replace<'a>) -> Self {
        self.on_conflict = Resolution::Replace(replace);
        self
    }

    /// Specifies that when a duplicate job is found that nothing should be done.
    /// i.e. that the current job should be unaffected and no duplicate job should be inserted.
    ///
    /// Note: this is the default behaviour so generally it is unnecessary to call this function
    /// unless dynamically building a [`UniquenessCriteria`] simply to be completely explicit.
    ///
    /// # Example
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    /// # use rexecutor::job::uniqueness_criteria::Resolution;
    ///
    /// let uniqueness_criteria = UniquenessCriteria::by_executor()
    ///     .and_key(&10985109)
    ///     .on_conflict_do_nothing();
    ///
    /// assert!(uniqueness_criteria.executor);
    /// assert!(uniqueness_criteria.key.is_some());
    /// assert!(uniqueness_criteria.duration.is_none());
    /// assert_eq!(uniqueness_criteria.statuses, &[JobStatus::Scheduled]);
    /// assert_eq!(uniqueness_criteria.on_conflict, Resolution::DoNothing);
    /// ```
    pub const fn on_conflict_do_nothing(mut self) -> Self {
        self.on_conflict = Resolution::DoNothing;
        self
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn api_test() {
        let uniqueness_criteria = UniquenessCriteria::by_executor()
            .on_conflict(Replace::scheduled_at().and_metadata().and_data());

        assert!(uniqueness_criteria.executor);
    }
}
