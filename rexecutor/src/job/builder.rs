use crate::{
    backend::{Backend, EnqueuableJob},
    executor::Executor,
    notifier::{InProcessNotifier, Notifier},
    RexecuterError,
};
use chrono::{DateTime, Duration, Utc};
use serde::{de::DeserializeOwned, Serialize};

use super::JobId;

pub struct JobBuilder<E>
where
    E: Executor + 'static,
    E::Data: Serialize + DeserializeOwned,
{
    data: Option<E::Data>,
    max_attempts: Option<u16>,
    tags: Vec<String>,
    scheduled_at: DateTime<Utc>,
}

impl<E> Default for JobBuilder<E>
where
    E: Executor,
    E::Data: Serialize + DeserializeOwned,
{
    fn default() -> Self {
        Self {
            data: Default::default(),
            max_attempts: None,
            tags: Default::default(),
            scheduled_at: Utc::now(),
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

    pub fn with_max_attempts(self, max_attempts: u16) -> Self {
        Self {
            max_attempts: Some(max_attempts),
            ..self
        }
    }
    pub fn schedule_at(self, schedule_at: DateTime<Utc>) -> Self {
        Self {
            scheduled_at: schedule_at,
            ..self
        }
    }
    pub fn schedule_in(self, schedule_in: Duration) -> Self {
        Self {
            scheduled_at: Utc::now() + schedule_in,
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

    pub async fn enqueue<B: Backend>(self, backend: &B) -> Result<JobId, RexecuterError>
    where
        E::Data: 'static + Send,
    {
        let job_id = backend
            .enqueue(EnqueuableJob {
                data: self.data,
                executor: E::NAME.to_owned(),
                max_attempts: self.max_attempts.unwrap_or(E::MAX_ATTEMPTS),
                scheduled_at: self.scheduled_at,
                tags: self.tags,
            })
            .await?;

        InProcessNotifier::notify(E::NAME);

        Ok(job_id)
    }
}

#[cfg(test)]
mod tests {
    use crate::{backend::test::MockBackend, executor::test::SimpleExecutor, job::JobId};

    use super::*;

    #[tokio::test]
    async fn enqueue() {
        let expected_job_id = JobId(0);

        let mut backend = MockBackend::default();
        backend.expect_enqueue_returning(Ok(expected_job_id));

        let job_id = SimpleExecutor::builder()
            .with_max_attempts(2)
            .with_tags(vec!["initial_job"])
            .with_data("First job".into())
            .schedule_in(Duration::hours(2))
            .enqueue(&backend)
            .await
            .unwrap();

        assert_eq!(job_id, expected_job_id);
    }
}
