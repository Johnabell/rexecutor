use std::hash::{Hash, Hasher};

use chrono::{DateTime, Utc};
use rexecutor::job::uniqueness_criteria::UniquenessCriteria;
use sqlx::{Postgres, QueryBuilder};

pub(crate) trait Unique {
    fn unique_identifier(&self, executor_identifier: &'_ str) -> i64;
    fn query(&self, key: i64, scheduled_at: DateTime<Utc>) -> QueryBuilder<'_, Postgres>;
}

impl<'a> Unique for UniquenessCriteria<'a> {
    fn unique_identifier(&self, executor_identifier: &'_ str) -> i64 {
        let mut state = fxhash::FxHasher64::default();
        self.key.hash(&mut state);
        if self.executor {
            executor_identifier.hash(&mut state);
        }
        self.statuses.hash(&mut state);
        state.finish() as i64
    }

    fn query(&self, key: i64, scheduled_at: DateTime<Utc>) -> QueryBuilder<'_, Postgres> {
        let mut builder =
            QueryBuilder::new("SELECT id, status FROM rexecutor_jobs WHERE uniqueness_key = ");
        builder.push_bind(key);
        if let Some(duration) = self.duration {
            let cutoff = scheduled_at - duration;
            builder.push(" AND scheduled_at >= ").push_bind(cutoff);
            let cutoff = scheduled_at + duration;
            builder.push(" AND scheduled_at <= ").push_bind(cutoff);
        }
        // TODO: orderby closest to today
        // builder.push(" ORDER BY scheduled_at ")
        builder.push(" LIMIT 1");

        builder
    }
}
