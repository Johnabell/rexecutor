use std::{hash::Hash, marker::PhantomData, ops::Sub, time::Duration};

use chrono::{DateTime, TimeDelta, Utc};
use cron::Schedule;
use serde::{de::DeserializeOwned, Serialize};
use tokio_util::sync::CancellationToken;

use crate::{backend::Backend, executor::Executor, job::uniqueness_criteria::UniquenessCriteria};

pub(crate) struct CronRunner<B, E>
where
    B: Backend,
    E: Executor + 'static,
    E::Data: Send + DeserializeOwned,
    E::Metadata: Serialize + DeserializeOwned,
{
    backend: B,
    data: E::Data,
    schedule: Schedule,
    _executor: PhantomData<E>,
}

impl<B, E> CronRunner<B, E>
where
    B: Backend + Send + 'static + Sync + Clone,
    E: Executor + 'static + Sync + Send,
    E::Data: Send + Serialize + DeserializeOwned + Hash + Clone + Sync,
    E::Metadata: Serialize + DeserializeOwned + Send + Sync,
{
    pub(crate) fn new(backend: B, schedule: Schedule, data: E::Data) -> Self {
        Self {
            backend,
            schedule,
            data,
            _executor: PhantomData,
        }
    }

    pub(crate) fn spawn(self, cancellation_token: CancellationToken) {
        tokio::spawn({
            async move {
                loop {
                    let next = self
                        .schedule
                        .upcoming(Utc)
                        .next()
                        .expect("No future scheduled time for cron job");
                    let delay = next
                        .sub(Utc::now())
                        .sub(TimeDelta::milliseconds(10))
                        .to_std()
                        .unwrap_or(Duration::ZERO);
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {
                            self.enqueue_job(next).await;
                            let delay = next - Utc::now();
                            if delay > TimeDelta::zero() {
                                tokio::time::sleep(delay.to_std().unwrap()).await;
                            }
                        },
                        _ = cancellation_token.cancelled() => {
                            tracing::debug!("Shutting down cron scheduler for {}", E::NAME);
                            break;
                        },
                    }
                }
            }
        });
    }

    async fn enqueue_job(&self, scheduled_at: DateTime<Utc>) {
        let criteria = UniquenessCriteria::by_executor()
            .and_within(TimeDelta::zero())
            .and_key(&self.data)
            .and_executor();

        let _ = E::builder()
            .schedule_at(scheduled_at)
            .with_data(self.data.clone())
            .unique(criteria)
            .enqueue_to_backend(&self.backend)
            .await
            .inspect_err(|err| {
                tracing::error!(?err, "Failed to enqueue cron job {} with {err}", E::NAME);
            });
    }
}
