use chrono::Utc;
use rexecutor::backend::Query;
use rexecutor::pruner::{PruneBy, PruneSpec, Spec};
use sqlx::{Postgres, QueryBuilder};

use crate::types::JobStatus;

pub(crate) trait ToQuery {
    fn query(&self) -> QueryBuilder<'_, Postgres>;
}

impl ToQuery for PruneSpec {
    fn query(&self) -> QueryBuilder<'_, Postgres> {
        let status: JobStatus = self.status.into();
        let mut builder = QueryBuilder::new("DELETE FROM rexecutor_jobs WHERE id in (");
        builder.push("SELECT id FROM rexecutor_jobs WHERE status = ");
        builder.push_bind::<JobStatus>(status);
        match &self.executors {
            Spec::Except(executors) => {
                builder.push(" AND executor != ALL(");
                builder.push_bind(executors);
            }
            Spec::Only(executors) => {
                builder.push(" AND executor = ANY(");
                builder.push_bind(executors);
            }
        }
        builder.push(")");
        let in_past = match status {
            JobStatus::Scheduled | JobStatus::Retryable | JobStatus::Executing => false,
            JobStatus::Complete | JobStatus::Cancelled | JobStatus::Discarded => true,
        };
        match self.prune_by {
            PruneBy::MaxAge(time) => {
                builder.push(" AND inserted_at");
                if in_past {
                    builder.push(" < ");
                    builder.push_bind(Utc::now() - time);
                } else {
                    builder.push(" > ");
                    builder.push_bind(Utc::now() + time);
                }
            }
            PruneBy::MaxLength(count) => {
                builder.push(" ORDER BY inserted_at");
                if in_past {
                    builder.push(" DESC ");
                } else {
                    builder.push(" ASC ");
                }
                builder.push("OFFSET ");
                builder.push_bind(count as i64);
            }
        }
        builder.push(")");

        builder
    }
}

impl ToQuery for Query<'_> {
    fn query(&self) -> QueryBuilder<'_, Postgres> {
        let mut builder = QueryBuilder::new(
            r#"SELECT 
                id,
                status,
                executor,
                data,
                metadata,
                attempt,
                max_attempts,
                priority,
                tags,
                errors,
                inserted_at,
                scheduled_at,
                attempted_at,
                completed_at,
                cancelled_at,
                discarded_at
            FROM rexecutor_jobs WHERE"#,
        );
        handle_query(&mut builder, self);
        builder.push(" ORDER BY inserted_at DESC");
        builder
    }
}

fn handle_query<'a>(builder: &mut QueryBuilder<'a, Postgres>, query: &'a Query<'_>) {
    match query {
        Query::Not(inner) => {
            builder.push(" NOT");
            handle_query(builder, inner);
        }
        Query::And(queries) => {
            if let Some((last, elements)) = &queries[..].split_last() {
                elements.iter().for_each(|query| {
                    handle_query(builder, query);
                    builder.push(" AND");
                });
                handle_query(builder, last);
            }
        }
        Query::Or(queries) => {
            if let Some((last, elements)) = &queries[..].split_last() {
                elements.iter().for_each(|query| {
                    handle_query(builder, query);
                    builder.push(" OR");
                });
                handle_query(builder, last);
            }
        }
        Query::DataEquals(data) => {
            builder.push(" data = ");
            builder.push_bind(data);
        }
        Query::MetadataEquals(metadata) => {
            builder.push(" metadata = ");
            builder.push_bind(metadata);
        }
        Query::ExecutorEqual(executor) => {
            builder.push(" executor = ");
            builder.push_bind(executor);
        }
        Query::IdEquals(id) => {
            builder.push(" id = ");
            builder.push_bind(Into::<i32>::into(*id));
        }
        Query::IdIn(ids) => {
            let ids: Vec<i32> = ids
                .iter()
                .map(|job_id| Into::<i32>::into(*job_id))
                .collect();
            builder.push(" id = All(");
            builder.push_bind(ids);
            builder.push(")");
        }
        Query::StatusEqual(status) => {
            builder.push(" status = ");
            builder.push_bind(JobStatus::from(*status));
        }
        Query::TagsAllOf(tags) => {
            builder.push(" tags @> ");
            builder.push_bind(&tags[..]);
            builder.push("::varchar[]");
        }
        Query::TagsOneOf(tags) => {
            builder.push(" tags && ");
            builder.push_bind(&tags[..]);
            builder.push("::varchar[]");
        }
        Query::ScheduledAtBefore(scheduled_at) => {
            builder.push(" scheduled_at < ");
            builder.push_bind(scheduled_at);
        }
        Query::ScheduledAtAfter(scheduled_at) => {
            builder.push(" scheduled_at > ");
            builder.push_bind(scheduled_at);
        }
        Query::ScheduledAtEqual(scheduled_at) => {
            builder.push(" scheduled_at = ");
            builder.push_bind(scheduled_at);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::TimeDelta;

    #[test]
    fn to_query_for_prune_spec_test() {
        let spec = PruneSpec {
            status: JobStatus::Complete.into(),
            prune_by: PruneBy::MaxAge(TimeDelta::hours(2)),
            executors: Spec::Only(vec!["simple_job"]),
        };

        assert_eq!(
            spec.query().into_sql(),
            "DELETE FROM rexecutor_jobs WHERE id in (SELECT id FROM rexecutor_jobs \
            WHERE status = $1 AND executor = ANY($2) AND inserted_at < $3)"
        );

        let spec = PruneSpec {
            status: JobStatus::Complete.into(),
            prune_by: PruneBy::MaxLength(10),
            executors: Spec::Only(vec!["simple_job"]),
        };

        assert_eq!(
            spec.query().into_sql(),
            "DELETE FROM rexecutor_jobs WHERE id in (SELECT id FROM rexecutor_jobs \
            WHERE status = $1 AND executor = ANY($2) ORDER BY inserted_at DESC OFFSET $3)"
        );

        let spec = PruneSpec {
            status: JobStatus::Complete.into(),
            prune_by: PruneBy::MaxLength(10),
            executors: Spec::Except(vec!["simple_job"]),
        };

        assert_eq!(
            spec.query().into_sql(),
            "DELETE FROM rexecutor_jobs WHERE id in (SELECT id FROM rexecutor_jobs \
            WHERE status = $1 AND executor != ALL($2) ORDER BY inserted_at DESC OFFSET $3)"
        );

        let spec = PruneSpec {
            status: JobStatus::Complete.into(),
            prune_by: PruneBy::MaxAge(TimeDelta::days(1)),
            executors: Spec::Except(vec!["simple_job"]),
        };

        assert_eq!(
            spec.query().into_sql(),
            "DELETE FROM rexecutor_jobs WHERE id in (SELECT id FROM rexecutor_jobs \
            WHERE status = $1 AND executor != ALL($2) AND inserted_at < $3)"
        );
    }

    #[test]
    fn to_query_for_query_test() {
        let query = Query::And(vec![
            Query::ScheduledAtEqual(Utc::now()),
            Query::TagsOneOf(&["tag1", "tag2"]),
        ]);
        dbg!(query.query().into_sql());
        panic!()
    }
}
