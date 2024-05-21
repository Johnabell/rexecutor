use chrono::Utc;

use crate::{
    job::uniqueness_criteria::UniquenessCriteria,
    pruner::{PruneSpec, Spec},
};

use super::{memory::KeyedJob, EnqueuableJob, Query};

pub(super) trait Queryable {
    fn matches(&self, job: &KeyedJob) -> bool;
}

impl<'a> Queryable for Query<'a> {
    fn matches(&self, job: &KeyedJob) -> bool {
        match self {
            Query::Not(inner) => !inner.matches(job),
            Query::And(inner) => inner.iter().all(|query| query.matches(job)),
            Query::Or(inner) => inner.iter().any(|query| query.matches(job)),
            Query::ExecutorEqual(executor) => job.executor == *executor,
            Query::DataEquals(data) => &job.data == data,
            Query::MetadataEquals(metadata) => &job.metadata == metadata,
            Query::IdEquals(id) => job.id == i32::from(*id),
            Query::IdIn(ids) => ids.contains(&job.id.into()),
            Query::StatusEqual(status) => job.status == *status,
            Query::TagsAllOf(tags) => tags.iter().all(|tag| job.tags.iter().any(|t| t == *tag)),
            Query::TagsOneOf(tags) => tags.iter().any(|tag| job.tags.iter().any(|t| t == *tag)),
            Query::ScheduledAtBefore(scheduled_at) => job.scheduled_at < *scheduled_at,
            Query::ScheduledAtAfter(scheduled_at) => job.scheduled_at > *scheduled_at,
            Query::ScheduledAtEqual(scheduled_at) => job.scheduled_at == *scheduled_at,
        }
    }
}

impl Queryable for PruneSpec {
    fn matches(&self, job: &KeyedJob) -> bool {
        job.status == self.status && self.executors.matches(job)
    }
}

impl Queryable for Spec {
    fn matches(&self, job: &KeyedJob) -> bool {
        match self {
            Spec::Except(executors) => executors.iter().all(|&executor| executor != job.executor),
            Spec::Only(executors) => executors.iter().any(|&executor| executor == job.executor),
        }
    }
}

impl<'a> Queryable for EnqueuableJob<'a> {
    fn matches(&self, job: &KeyedJob) -> bool {
        match self.uniqueness_criteria {
            None => false,
            Some(ref uniqueness_criteria @ UniquenessCriteria { duration: None, .. }) => {
                uniqueness_criteria.key == job.key
                    && (!uniqueness_criteria.executor || self.executor == job.executor)
                    && (uniqueness_criteria.statuses.contains(&job.status))
            }
            Some(
                ref uniqueness_criteria @ UniquenessCriteria {
                    duration: Some(duration),
                    ..
                },
            ) => {
                uniqueness_criteria.key == job.key
                    && (!uniqueness_criteria.executor || self.executor == job.executor)
                    && (uniqueness_criteria.statuses.contains(&job.status))
                    && (job.scheduled_at - Utc::now()).abs() < duration
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::ops::{Add, Sub};

    use chrono::TimeDelta;

    use crate::{backend::Job, job::JobStatus};

    use super::*;

    #[test]
    fn query_matches() {
        let job = KeyedJob::raw_job();

        let ids = [job.id.into(), 42.into()];
        let matching_queries = [
            Query::ExecutorEqual(&job.executor),
            Query::IdEquals(job.id.into()),
            Query::IdIn(&ids),
            Query::DataEquals(job.data.clone()),
            Query::MetadataEquals(job.metadata.clone()),
            Query::StatusEqual(job.status),
            Query::ScheduledAtEqual(job.scheduled_at),
            Query::ScheduledAtBefore(job.scheduled_at.add(TimeDelta::hours(1))),
            Query::ScheduledAtAfter(job.scheduled_at.sub(TimeDelta::hours(1))),
            Query::TagsAllOf(&[]),
            // What is the correct behaviour here?
            // Query::TagsOneOf(&[]),
        ];

        matching_queries
            .clone()
            .into_iter()
            .for_each(|query| assert!(query.matches(&job)));

        matching_queries
            .clone()
            .into_iter()
            .map(|query| Query::Not(Box::new(query)))
            .for_each(|query| assert!(!query.matches(&job)));

        assert!(Query::And(matching_queries.to_vec()).matches(&job));
        assert!(Query::Or(matching_queries.to_vec()).matches(&job));

        let ids = [42.into(), (job.id + 1).into()];
        let non_matching_queries = [
            Query::ExecutorEqual("another_executor"),
            Query::IdEquals((job.id + 1).into()),
            Query::IdIn(&ids),
            Query::DataEquals(serde_json::Value::Number(42.into())),
            Query::MetadataEquals(serde_json::Value::Number(42.into())),
            Query::StatusEqual(JobStatus::Executing),
            Query::ScheduledAtEqual(job.scheduled_at.add(TimeDelta::hours(1))),
            Query::ScheduledAtBefore(job.scheduled_at.sub(TimeDelta::hours(1))),
            Query::ScheduledAtAfter(job.scheduled_at.add(TimeDelta::hours(1))),
            Query::TagsAllOf(&["tag"]),
            Query::TagsOneOf(&["tag"]),
        ];

        non_matching_queries
            .clone()
            .into_iter()
            .for_each(|query| assert!(!query.matches(&job)));

        non_matching_queries
            .clone()
            .into_iter()
            .map(|query| Query::Not(Box::new(query)))
            .for_each(|query| assert!(query.matches(&job)));

        assert!(!Query::And(non_matching_queries.to_vec()).matches(&job));
        assert!(!Query::Or(non_matching_queries.to_vec()).matches(&job));

        let all_queries: Vec<_> = non_matching_queries
            .into_iter()
            .chain(matching_queries)
            .collect();

        assert!(!Query::And(all_queries.clone()).matches(&job));
        assert!(Query::Or(all_queries).matches(&job));
    }

    #[test]
    fn query_matches_tags() {
        let tags = ["tag1", "tag2"];
        let job = KeyedJob::raw_job().with_tags(tags.iter().map(|&tag| tag.to_owned()).collect());

        [
            Query::TagsAllOf(&tags),
            Query::TagsOneOf(&tags),
            Query::TagsAllOf(&["tag1"]),
            Query::TagsOneOf(&["tag1"]),
            Query::TagsAllOf(&["tag2"]),
            Query::TagsOneOf(&["tag2"]),
        ]
        .into_iter()
        .for_each(|query| assert!(query.matches(&job)));

        [
            Query::TagsAllOf(&["tag1", "tag2", "tag3"]),
            Query::TagsOneOf(&["tag3"]),
            Query::TagsAllOf(&["tag3"]),
        ]
        .into_iter()
        .for_each(|query| assert!(!query.matches(&job)));
    }

    #[test]
    fn prune_spec_matches() {
        let job = KeyedJob::raw_job();
        let pruner_spec = PruneSpec {
            status: job.status,
            prune_by: crate::pruner::PruneBy::MaxLength(5),
            executors: Spec::Only(vec![Job::DEFAULT_EXECUTOR]),
        };
        assert!(pruner_spec.matches(&job));

        let pruner_spec = PruneSpec {
            status: JobStatus::Executing,
            prune_by: crate::pruner::PruneBy::MaxLength(5),
            executors: Spec::Only(vec![Job::DEFAULT_EXECUTOR]),
        };
        assert!(!pruner_spec.matches(&job));
    }

    #[test]
    fn spec_matches() {
        let job = KeyedJob::raw_job();

        assert!(!Spec::Except(vec![Job::DEFAULT_EXECUTOR]).matches(&job));
        assert!(Spec::Only(vec![Job::DEFAULT_EXECUTOR]).matches(&job));

        assert!(Spec::Except(vec!["other_executor"]).matches(&job));
        assert!(!Spec::Only(vec!["other_executor"]).matches(&job));
    }

    #[test]
    fn enqueuable_job_no_uniqueness_criteria() {
        let enqueuable_job = EnqueuableJob::mock_job().with_uniqueness_criteria(None);
        let job = enqueuable_job.clone().into_job(42);

        assert!(!enqueuable_job.matches(&job));
    }

    #[test]
    fn enqueuable_job_uniqueness_criteria_by_executor() {
        let uniqueness_criteria = UniquenessCriteria::by_executor();
        let enqueuable_job =
            EnqueuableJob::mock_job().with_uniqueness_criteria(Some(uniqueness_criteria));
        let job1 = enqueuable_job.clone().into_job(42);
        let job2 = EnqueuableJob::mock_job()
            .with_executor("other")
            .into_job(43);

        assert!(enqueuable_job.matches(&job1));
        assert!(!enqueuable_job.matches(&job2));
    }

    #[test]
    fn enqueuable_job_uniqueness_criteria_by_key() {
        let uniqueness_criteria = UniquenessCriteria::by_key(&42);
        let enqueuable_job =
            EnqueuableJob::mock_job().with_uniqueness_criteria(Some(uniqueness_criteria));
        let job1 = enqueuable_job.clone().into_job(42);
        let job2 = enqueuable_job
            .clone()
            .with_uniqueness_criteria(Some(UniquenessCriteria::by_key(&43)))
            .into_job(43);

        assert!(enqueuable_job.matches(&job1));
        assert!(!enqueuable_job.matches(&job2));
    }

    #[test]
    fn enqueuable_job_uniqueness_criteria_by_statuses() {
        let uniqueness_criteria = UniquenessCriteria::by_statuses(&[JobStatus::Retryable]);
        let enqueuable_job =
            EnqueuableJob::mock_job().with_uniqueness_criteria(Some(uniqueness_criteria));
        let job1 = enqueuable_job.clone().into_job(42);
        let job2 = enqueuable_job
            .clone()
            .into_job(43)
            .with_status(JobStatus::Retryable);

        assert!(!enqueuable_job.matches(&job1));
        assert!(enqueuable_job.matches(&job2));
    }

    #[test]
    fn enqueuable_job_uniqueness_criteria_within() {
        let uniqueness_criteria = UniquenessCriteria::within(TimeDelta::days(1));
        let enqueuable_job =
            EnqueuableJob::mock_job().with_uniqueness_criteria(Some(uniqueness_criteria));

        [
            enqueuable_job.clone().into_job(42),
            enqueuable_job
                .clone()
                .into_job(43)
                .with_scheduled_at(Utc::now() + TimeDelta::days(1)),
            enqueuable_job
                .clone()
                .into_job(43)
                .with_scheduled_at(Utc::now() - TimeDelta::days(1) + TimeDelta::minutes(1)),
        ]
        .iter()
        .for_each(|job| assert!(enqueuable_job.matches(job)));

        [
            enqueuable_job
                .clone()
                .into_job(43)
                .with_scheduled_at(Utc::now() + TimeDelta::days(1) + TimeDelta::minutes(1)),
            enqueuable_job
                .clone()
                .into_job(43)
                .with_scheduled_at(Utc::now() - TimeDelta::days(1)),
        ]
        .iter()
        .for_each(|job| assert!(!enqueuable_job.matches(job)));
    }
}
