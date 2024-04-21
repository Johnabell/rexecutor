use super::JobId;
use super::JobStatus;
use chrono::TimeDelta;
use chrono::{DateTime, Utc};

#[derive(Debug, Eq, PartialEq, Clone)]
#[non_exhaustive]
pub struct Where<'a, D, M>(pub(crate) Query<'a, D, M>);

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Query<'a, D, M> {
    Not(Box<Query<'a, D, M>>),
    And(Vec<Query<'a, D, M>>),
    Or(Vec<Query<'a, D, M>>),
    DataEquals(&'a D),
    MetadataEquals(&'a M),
    IdEquals(JobId),
    IdIn(&'a [JobId]),
    StatusEqual(JobStatus),
    TagsAllOf(&'a [&'a str]),
    TagsOneOf(&'a [&'a str]),
    ScheduledAtBefore(DateTime<Utc>),
    ScheduledAtAfter(DateTime<Utc>),
    ScheduledAtEqual(DateTime<Utc>),
}

impl<'a, D, M> Where<'a, D, M> {
    pub fn and(mut self, other: Where<'a, D, M>) -> Self {
        if let Query::And(ref mut constraints) = self.0 {
            constraints.push(other.0);
        } else {
            self.0 = Query::And(vec![self.0, other.0]);
        }
        self
    }

    pub fn or(mut self, other: Where<'a, D, M>) -> Self {
        if let Query::Or(ref mut constraints) = self.0 {
            constraints.push(other.0);
        } else {
            self.0 = Query::Or(vec![self.0, other.0]);
        }
        self
    }

    pub fn data_equals(data: &'a D) -> Self {
        Self(Query::DataEquals(data))
    }

    pub fn metadata_equals(metadata: &'a M) -> Self {
        Self(Query::MetadataEquals(metadata))
    }

    pub fn id_equals(id: JobId) -> Self {
        Self(Query::IdEquals(id))
    }

    pub fn id_in(ids: &'a [JobId]) -> Self {
        Self(Query::IdIn(ids))
    }

    pub fn status_equal(status: JobStatus) -> Self {
        Self(Query::StatusEqual(status))
    }

    pub fn tagged_by_all_off(tags: &'a [&'a str]) -> Self {
        Self(Query::TagsAllOf(tags))
    }

    pub fn tagged_by_one_of(tags: &'a [&'a str]) -> Self {
        Self(Query::TagsOneOf(tags))
    }

    pub fn scheduled_at_equals(scheduled_at: DateTime<Utc>) -> Self {
        Self(Query::ScheduledAtEqual(scheduled_at))
    }

    pub fn scheduled_at_before(scheduled_at: DateTime<Utc>) -> Self {
        Self(Query::ScheduledAtBefore(scheduled_at))
    }

    pub fn scheduled_at_after(scheduled_at: DateTime<Utc>) -> Self {
        Self(Query::ScheduledAtAfter(scheduled_at))
    }

    pub fn scheduled_at_older_than(duration: TimeDelta) -> Self {
        Self(Query::ScheduledAtBefore(Utc::now() - duration))
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
            .and(!Where::status_equal(JobStatus::Executing));

        dbg!(query);
    }
}
