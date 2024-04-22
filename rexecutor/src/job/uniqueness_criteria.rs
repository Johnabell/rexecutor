use chrono::TimeDelta;
use std::hash::Hash;

use crate::job::JobStatus;

#[allow(private_bounds)]
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct UniquenessCriteria<'a> {
    pub key: Option<i64>,
    pub executor: bool,
    pub duration: Option<TimeDelta>,
    pub statuses: &'a [JobStatus],
    pub on_conflict: Resolution<'a>,
}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub enum Resolution<'a> {
    #[default]
    DoNothing,
    Replace(Replace<'a>),
}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct Replace<'a> {
    pub scheduled_at: bool,
    pub data: bool,
    pub metadata: bool,
    pub priority: bool,
    pub max_attempts: bool,
    pub for_statuses: &'a [JobStatus],
}

impl<'a> Replace<'a> {
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

    pub const fn and_scheduled_at(mut self) -> Self {
        self.scheduled_at = true;
        self
    }

    pub const fn and_data(mut self) -> Self {
        self.data = true;
        self
    }

    pub const fn and_metadata(mut self) -> Self {
        self.metadata = true;
        self
    }

    pub const fn and_priority(mut self) -> Self {
        self.priority = true;
        self
    }

    pub const fn and_max_attempts(mut self) -> Self {
        self.max_attempts = true;
        self
    }

    pub const fn for_statuses(mut self, statuses: &'a [JobStatus]) -> Self {
        self.for_statuses = statuses;
        self
    }
}

impl<'a> UniquenessCriteria<'a> {
    pub const fn by_executor() -> Self {
        Self {
            key: None,
            executor: true,
            duration: None,
            statuses: &[],
            on_conflict: Resolution::DoNothing,
        }
    }

    pub const fn within(duration: TimeDelta) -> Self {
        Self {
            key: None,
            executor: false,
            duration: Some(duration),
            statuses: &[],
            on_conflict: Resolution::DoNothing,
        }
    }

    pub const fn by_statuses(statuses: &'a [JobStatus]) -> Self {
        Self {
            key: None,
            executor: false,
            duration: None,
            statuses,
            on_conflict: Resolution::DoNothing,
        }
    }

    pub fn by_key<H: Hash>(value: &H) -> Self {
        let key = fxhash::hash64(value);
        Self {
            key: Some(key as i64),
            executor: false,
            duration: None,
            statuses: &[],
            on_conflict: Resolution::DoNothing,
        }
    }

    pub const fn and_executor(mut self) -> Self {
        self.executor = true;
        self
    }

    pub const fn and_within(mut self, duration: TimeDelta) -> Self {
        self.duration = Some(duration);
        self
    }

    pub const fn and_statuses(mut self, statuses: &'a [JobStatus]) -> Self {
        self.statuses = statuses;
        self
    }

    pub fn and_key<H: Hash>(mut self, value: &H) -> Self {
        let key = fxhash::hash64(value);
        self.key = Some(key as i64);
        self
    }

    pub const fn on_conflict(mut self, replace: Replace<'a>) -> Self {
        self.on_conflict = Resolution::Replace(replace);
        self
    }

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
