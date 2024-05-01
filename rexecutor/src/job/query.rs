//! APIs for querying jobs.
//!
//! The main entry point for the query API is [`Where`]. This struct can be constructed using one
//! of the many constructor methods:
//!
//! - [`Where::data_equals`]
//! - [`Where::metadata_equals`]
//! - [`Where::id_equals`]
//! - [`Where::id_in`]
//! - [`Where::status_equals`]
//! - [`Where::tagged_by_one_of`]
//! - [`Where::tagged_by_all_of`]
//! - [`Where::scheduled_at_older_than`]
//! - [`Where::scheduled_at_before`]
//! - [`Where::scheduled_at_within_the_last`]
//! - [`Where::scheduled_at_after`]
//! - [`Where::scheduled_at_equals`]
//!
//! These can then be combine together using the [`Where::and`], [`Where::or`], and through the
//! [`std::ops::Not`] implementation.
//!
//! # Examples
//!
//! To simply lookup a job by id you could use
//!
//! ```
//! # use rexecutor::prelude::*;
//! # let job_id = 1.into();
//! let query = Where::<'_, (), ()>::id_equals(job_id);
//! ```
//!
//! or to lookup a job by it's `scheduled_at` field
//!
//! ```
//! # use rexecutor::prelude::*;
//! # use chrono::Utc;
//! let query = Where::<'_, (), ()>::scheduled_at_equals(Utc::now());
//! ```
//!
//! or more complex criteria can be constructed in the following manor
//!
//! ```
//! # use rexecutor::prelude::*;
//! # use chrono::TimeDelta;
//! let query = Where::<'_, (), ()>::tagged_by_all_of(&["customer_comms", "mandatory"])
//!     .and(!Where::tagged_by_one_of(&["thank_you_comms"]))
//!     .and(Where::scheduled_at_within_the_last(TimeDelta::days(20)))
//!     .and(
//!         Where::status_equals(JobStatus::Scheduled)
//!             .or(Where::status_equals(JobStatus::Retryable)),
//!     )
//!     .or(Where::status_equals(JobStatus::Discarded)
//!         .and(Where::scheduled_at_older_than(TimeDelta::days(60))));
//! ```
use super::JobId;
use super::JobStatus;
use chrono::TimeDelta;
use chrono::{DateTime, Utc};

/// Entry point for building job queries.
///
/// This struct can be constructed using one of the many constructor methods:
///
/// - [`Where::data_equals`]
/// - [`Where::metadata_equals`]
/// - [`Where::id_equals`]
/// - [`Where::id_in`]
/// - [`Where::status_equals`]
/// - [`Where::tagged_by_one_of`]
/// - [`Where::tagged_by_all_of`]
/// - [`Where::scheduled_at_older_than`]
/// - [`Where::scheduled_at_before`]
/// - [`Where::scheduled_at_within_the_last`]
/// - [`Where::scheduled_at_after`]
/// - [`Where::scheduled_at_equals`]
///
/// These can then be combine together using the [`Where::and`], [`Where::or`], and through the
/// [`std::ops::Not`] implementation.
///
/// # Examples
///
/// To simply lookup a job by id you could use
///
/// ```
/// # use rexecutor::prelude::*;
/// # let job_id = 1.into();
/// let query = Where::<'_, (), ()>::id_equals(job_id);
/// ```
///
/// or to lookup a job by it's `scheduled_at` field
///
/// ```
/// # use rexecutor::prelude::*;
/// # use chrono::Utc;
/// let query = Where::<'_, (), ()>::scheduled_at_equals(Utc::now());
/// ```
///
/// or more complex criteria can be constructed in the following manor
///
/// ```
/// # use rexecutor::prelude::*;
/// # use chrono::{TimeDelta, Utc};
/// # let job_ids = [1.into(), 2.into()];
/// let query = Where::<'_, (), ()>::id_in(&job_ids)
///     .and(Where::tagged_by_all_of(&["customer_comms", "mandatory"]))
///     .and(!Where::tagged_by_one_of(&["thank_you_comms"]))
///     .and(Where::scheduled_at_within_the_last(TimeDelta::days(20)))
///     .and(
///         Where::status_equals(JobStatus::Scheduled)
///             .or(Where::status_equals(JobStatus::Retryable)),
///     )
///     .or(Where::status_equals(JobStatus::Discarded)
///         .and(Where::scheduled_at_after(Utc::now() - TimeDelta::days(60))));
/// ```
#[derive(Debug, Eq, PartialEq, Clone)]
#[non_exhaustive]
pub struct Where<'a, D, M>(pub(crate) Query<'a, D, M>);

/// Recursive data structure for defining job queries.
///
/// Note: this will generally not be constructed directly, but be created and combined via
/// the methods on [`Where`].
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Query<'a, D, M> {
    /// Represents the negation of the inner query.
    ///
    /// Usually, constructed via the [`std::ops::Not`] implementation on [`Where`].
    Not(Box<Query<'a, D, M>>),
    /// Combines queries with logical `and`.
    ///
    /// Usually, constructed via [`Where::and`].
    And(Vec<Query<'a, D, M>>),
    /// Combines queries with logical `or`.
    ///
    /// Usually, constructed via [`Where::or`].
    Or(Vec<Query<'a, D, M>>),
    /// Query based on equality with the job's data.
    ///
    /// Usually, constructed via [`Where::data_equals`].
    DataEquals(&'a D),
    /// Query based on equality with the job's metadata.
    ///
    /// Usually, constructed via [`Where::metadata_equals`].
    MetadataEquals(&'a M),
    /// Query based on equality with the job's id.
    ///
    /// Usually, constructed via [`Where::id_equals`].
    IdEquals(JobId),
    /// Query for jobs where matching one of the contained ids.
    ///
    /// Usually, constructed via [`Where::id_in`].
    IdIn(&'a [JobId]),
    /// Query based on equality with the given [`JobStatus`].
    ///
    /// Usually, constructed via [`Where::status_equals`].
    StatusEqual(JobStatus),
    /// Query for jobs where all of the job's tags are in the provided set of tags.
    ///
    /// Usually, constructed via [`Where::tagged_by_all_of`].
    TagsAllOf(&'a [&'a str]),
    /// Query for jobs where one of the job's tags are in the provided set of tags.
    ///
    /// Usually, constructed via [`Where::tagged_by_one_of`].
    TagsOneOf(&'a [&'a str]),
    /// Query for jobs where the job's `scheduled_at` field is before the given [`DateTime`].
    ///
    /// Usually, constructed via [`Where::scheduled_at_older_than`] or
    /// [`Where::scheduled_at_before`].
    ScheduledAtBefore(DateTime<Utc>),
    /// Query for jobs where the job's `scheduled_at` field is after the given [`DateTime`].
    ///
    /// Usually, constructed via [`Where::scheduled_at_within_the_last`] or
    /// [`Where::scheduled_at_after`].
    ScheduledAtAfter(DateTime<Utc>),
    /// Query for jobs where the job's `scheduled_at` field is equal to the given [`DateTime`].
    ///
    /// Usually, constructed via [`Where::scheduled_at_equals`].
    ScheduledAtEqual(DateTime<Utc>),
}

impl<'a, D, M> Where<'a, D, M> {
    /// Combine this query via a logical `and` with the provided query.
    ///
    /// # Example
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    /// # use chrono::TimeDelta;
    /// let query = Where::data_equals(&"Data")
    ///     .and(Where::metadata_equals(&42))
    ///     .and(Where::scheduled_at_within_the_last(TimeDelta::days(3)));
    /// ```
    pub fn and(mut self, other: Where<'a, D, M>) -> Self {
        if let Query::And(ref mut constraints) = self.0 {
            constraints.push(other.0);
        } else {
            self.0 = Query::And(vec![self.0, other.0]);
        }
        self
    }

    /// Combine this query via a logical `or` with the provided query.
    ///
    /// # Example
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    /// let query = Where::<'_, (), ()>::status_equals(JobStatus::Scheduled)
    ///     .or(Where::status_equals(JobStatus::Retryable))
    ///     .or(Where::status_equals(JobStatus::Discarded));
    /// ```
    pub fn or(mut self, other: Where<'a, D, M>) -> Self {
        if let Query::Or(ref mut constraints) = self.0 {
            constraints.push(other.0);
        } else {
            self.0 = Query::Or(vec![self.0, other.0]);
        }
        self
    }

    /// Creates a query based on equality with the job's data.
    pub fn data_equals(data: &'a D) -> Self {
        Self(Query::DataEquals(data))
    }

    /// Creates a query based on equality with the job's metadata.
    pub fn metadata_equals(metadata: &'a M) -> Self {
        Self(Query::MetadataEquals(metadata))
    }

    /// Creates a query based on equality with the job's id.
    pub fn id_equals(id: JobId) -> Self {
        Self(Query::IdEquals(id))
    }

    /// Creates a query based on the job's id being within a given collection.
    pub fn id_in(ids: &'a [JobId]) -> Self {
        Self(Query::IdIn(ids))
    }

    /// Creates a query based equality with the job's status.
    pub fn status_equals(status: JobStatus) -> Self {
        Self(Query::StatusEqual(status))
    }

    /// Creates a query for jobs where all of the job's tags are in the provided set of tags.
    pub fn tagged_by_all_of(tags: &'a [&'a str]) -> Self {
        Self(Query::TagsAllOf(tags))
    }

    /// Creates a query for jobs where one of the job's tags are in the provided set of tags.
    pub fn tagged_by_one_of(tags: &'a [&'a str]) -> Self {
        Self(Query::TagsOneOf(tags))
    }

    /// Query for jobs where the job's `scheduled_at` field is equal to the given [`DateTime`].
    pub fn scheduled_at_equals(scheduled_at: DateTime<Utc>) -> Self {
        Self(Query::ScheduledAtEqual(scheduled_at))
    }

    /// Query for jobs where the job's `scheduled_at` field is before to the given [`DateTime`].
    pub fn scheduled_at_before(scheduled_at: DateTime<Utc>) -> Self {
        Self(Query::ScheduledAtBefore(scheduled_at))
    }

    /// Query for jobs where the job's `scheduled_at` field is after to the given [`DateTime`].
    pub fn scheduled_at_after(scheduled_at: DateTime<Utc>) -> Self {
        Self(Query::ScheduledAtAfter(scheduled_at))
    }

    /// Query for jobs where the job's `scheduled_at` field is outside of the given [`TimeDelta`].
    pub fn scheduled_at_older_than(duration: TimeDelta) -> Self {
        Self(Query::ScheduledAtBefore(Utc::now() - duration))
    }

    /// Query for jobs where the job's `scheduled_at` field is within of the given [`TimeDelta`].
    pub fn scheduled_at_within_the_last(duration: TimeDelta) -> Self {
        Self(Query::ScheduledAtAfter(Utc::now() - duration))
    }
}

impl<'a, D, M> std::ops::Not for Where<'a, D, M> {
    type Output = Self;
    fn not(self) -> Self {
        Self(Query::Not(Box::new(self.0)))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn query_builder_api() {
        let query = Where::data_equals(&"Hello world")
            .and(Where::metadata_equals(&30))
            .and(Where::scheduled_at_before(Utc::now()))
            .and(!Where::status_equals(JobStatus::Executing));

        dbg!(query);
    }
}
