use crate::{executor::Executor, RexecuterError};
use chrono::{DateTime, Duration, Utc};
use serde::{de::DeserializeOwned, Serialize};

pub struct JobBuilder<E>
where
    E: Executor,
    E::Data: Serialize + DeserializeOwned,
{
    data: Option<E::Data>,
    max_attempts: i32,
    tags: Vec<String>,
    schedule_at: DateTime<Utc>,
}

impl<E> Default for JobBuilder<E>
where
    E: Executor,
    E::Data: Serialize + DeserializeOwned,
{
    fn default() -> Self {
        Self {
            data: Default::default(),
            max_attempts: 5,
            tags: Default::default(),
            schedule_at: Default::default(),
        }
    }
}

impl<E> JobBuilder<E>
where
    E: Executor,
    E::Data: Serialize + DeserializeOwned,
{
    pub fn with_data(self, data: E::Data) -> Self {
        Self {
            data: Some(data),
            ..self
        }
    }

    pub fn with_max_attempts(self, max_attempts: i32) -> Self {
        Self {
            max_attempts,
            ..self
        }
    }
    pub fn schedule_at(self, schedule_at: DateTime<Utc>) -> Self {
        Self {
            schedule_at,
            ..self
        }
    }
    pub fn schedule_in(self, schedule_in: Duration) -> Self {
        Self {
            schedule_at: Utc::now() + schedule_in,
            ..self
        }
    }
    pub fn add_tag(self, tag: String) -> Self {
        let mut tags = self.tags;
        tags.push(tag);
        Self { tags, ..self }
    }
    pub fn with_tags(self, tags: Vec<impl Into<String>>) -> Self {
        let tags = tags.into_iter().map(Into::into).collect();
        Self { tags, ..self }
    }
    // TODO: add a function to ensure uniqueness of jobs
    pub fn unique(self) -> Self {
        self
    }
    // TODO: add optional metadata
    pub fn metadata(self) -> Self {
        self
    }

    pub async fn enqueue(self) -> Result<(), RexecuterError> {
        // TODO: write to DB
        // Should this wake the Rexecutor
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::executor::test::SimpleExecutor;

    use super::*;

    #[tokio::test]
    async fn enqueue() {
        JobBuilder::<SimpleExecutor>::default()
            .with_max_attempts(2)
            .with_tags(vec!["initial_job"])
            .with_data("First job".into())
            .schedule_in(Duration::hours(2))
            .enqueue()
            .await
            .unwrap();
    }
}
