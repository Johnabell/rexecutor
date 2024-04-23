use crate::{
    backend::{Backend, EnqueuableJob},
    executor::Executor,
    job::uniqueness_criteria::UniquenessCriteria,
    RexecuterError, GLOBAL_BACKEND,
};
use chrono::{DateTime, Duration, Utc};
use serde::{de::DeserializeOwned, Serialize};

use super::JobId;

// TODO add api to add as a cron job
pub struct JobBuilder<'a, E>
where
    E: Executor + 'static,
    E::Data: Serialize + DeserializeOwned,
    E::Metadata: Serialize + DeserializeOwned,
{
    data: Option<E::Data>,
    metadata: Option<E::Metadata>,
    max_attempts: Option<u16>,
    tags: Vec<String>,
    scheduled_at: DateTime<Utc>,
    priority: u16,
    uniqueness_criteria: Option<UniquenessCriteria<'a>>,
}

impl<'a, E> Default for JobBuilder<'a, E>
where
    E: Executor,
    E::Data: Serialize + DeserializeOwned,
    E::Metadata: Serialize + DeserializeOwned,
{
    fn default() -> Self {
        Self {
            data: Default::default(),
            metadata: Default::default(),
            max_attempts: None,
            tags: Default::default(),
            scheduled_at: Utc::now(),
            priority: 0,
            uniqueness_criteria: None,
        }
    }
}

impl<'a, E> JobBuilder<'a, E>
where
    E: Executor,
    E::Data: Serialize + DeserializeOwned,
    E::Metadata: Serialize + DeserializeOwned,
{
    pub fn with_data(self, data: E::Data) -> Self {
        Self {
            data: Some(data),
            ..self
        }
    }

    pub fn with_metadata(self, metadata: E::Metadata) -> Self {
        Self {
            metadata: Some(metadata),
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
    pub fn add_tag(self, tag: impl Into<String>) -> Self {
        let mut tags = self.tags;
        tags.push(tag.into());
        Self { tags, ..self }
    }

    pub fn with_tags(self, tags: Vec<impl Into<String>>) -> Self {
        let tags = tags.into_iter().map(Into::into).collect();
        Self { tags, ..self }
    }

    pub fn unique(self, criteria: UniquenessCriteria<'a>) -> Self {
        Self {
            uniqueness_criteria: Some(criteria),
            ..self
        }
    }

    pub fn with_priority(self, priority: u16) -> Self {
        Self { priority, ..self }
    }

    pub async fn enqueue(self) -> Result<JobId, RexecuterError>
    where
        E::Data: 'static + Send,
    {
        let backend = GLOBAL_BACKEND.get().ok_or(RexecuterError::GlobalBackend)?;

        self.enqueue_to_backend(backend.as_ref()).await
    }

    pub async fn enqueue_to_backend<B: Backend + ?Sized>(
        self,
        backend: &B,
    ) -> Result<JobId, RexecuterError>
    where
        E::Data: 'static + Send,
    {
        let job_id = backend
            .enqueue(EnqueuableJob {
                data: serde_json::to_value(self.data)?,
                metadata: serde_json::to_value(self.metadata)?,
                executor: E::NAME.to_owned(),
                max_attempts: self.max_attempts.unwrap_or(E::MAX_ATTEMPTS),
                scheduled_at: self.scheduled_at,
                tags: self.tags,
                priority: self.priority,
                uniqueness_criteria: self.uniqueness_criteria.or(E::UNIQUENESS_CRITERIA),
            })
            .await?;

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

        let backend = MockBackend::default();
        backend.expect_enqueue_returning(Ok(expected_job_id));

        let job_id = SimpleExecutor::builder()
            .with_max_attempts(2)
            .with_tags(vec!["initial_job"])
            .with_data("First job".into())
            .schedule_in(Duration::hours(2))
            .enqueue_to_backend(&backend)
            .await
            .unwrap();

        assert_eq!(job_id, expected_job_id);
    }
}
