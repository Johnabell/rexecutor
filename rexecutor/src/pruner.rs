use std::{ops::Sub, time::Duration};

use chrono::{TimeDelta, Utc};
use futures::{stream::FuturesOrdered, StreamExt};
use tokio_util::sync::CancellationToken;

use crate::{backend::Backend, executor::Executor, job::JobStatus};

pub struct PrunerConfig {
    schedule: cron::Schedule,
    max_concurrency: Option<usize>,
    pruners: Vec<PruneSpec>,
}

impl PrunerConfig {
    pub fn new(schedule: cron::Schedule) -> Self {
        Self {
            schedule,
            max_concurrency: Some(10),
            pruners: Default::default(),
        }
    }

    pub fn with_max_concurrency(mut self, limit: Option<usize>) -> Self {
        self.max_concurrency = limit;
        self
    }

    #[allow(private_bounds)]
    pub fn with_pruner<T>(mut self, pruner: Pruner<T>) -> Self
    where
        T: IntoSpec,
    {
        self.pruners.push(pruner.into());
        self
    }

    #[allow(private_bounds)]
    pub fn with_pruners<T>(mut self, pruner: impl IntoIterator<Item = Pruner<T>>) -> Self
    where
        T: IntoSpec,
    {
        self.pruners.extend(pruner.into_iter().map(Into::into));
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PruneSpec {
    pub status: JobStatus,
    pub prune_by: PruneBy,
    pub executors: Spec,
}

impl<T> From<Pruner<T>> for PruneSpec
where
    T: IntoSpec,
{
    fn from(value: Pruner<T>) -> Self {
        Self {
            status: value.status,
            prune_by: value.prune_by,
            executors: value.executors.into_spec(),
        }
    }
}

#[allow(private_bounds)]
pub struct Pruner<T>
where
    T: IntoSpec,
{
    status: JobStatus,
    prune_by: PruneBy,
    executors: T,
}

impl Pruner<All> {
    pub fn max_age(age: TimeDelta, status: JobStatus) -> Self {
        Self {
            status,
            prune_by: PruneBy::MaxAge(age),
            executors: All,
        }
    }
    pub fn max_length(length: u32, status: JobStatus) -> Self {
        Self {
            status,
            prune_by: PruneBy::MaxLength(length),
            executors: All,
        }
    }
    pub fn only<E: Executor>(self) -> Pruner<Only> {
        Pruner {
            status: self.status,
            prune_by: self.prune_by,
            executors: Only(vec![E::NAME]),
        }
    }
    pub fn except<E: Executor>(self) -> Pruner<Except> {
        Pruner {
            status: self.status,
            prune_by: self.prune_by,
            executors: Except(vec![E::NAME]),
        }
    }
}

impl Pruner<Only> {
    pub fn and<E: Executor>(mut self) -> Self {
        self.executors.0.push(E::NAME);
        self
    }
}

impl Pruner<Except> {
    pub fn and<E: Executor>(mut self) -> Self {
        self.executors.0.push(E::NAME);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PruneBy {
    MaxAge(TimeDelta),
    MaxLength(u32),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Spec {
    Except(Vec<&'static str>),
    Only(Vec<&'static str>),
}

trait IntoSpec {
    fn into_spec(self) -> Spec;
}
pub struct All;
impl IntoSpec for All {
    fn into_spec(self) -> Spec {
        Spec::Except(Vec::new())
    }
}
pub struct Except(Vec<&'static str>);
impl IntoSpec for Except {
    fn into_spec(self) -> Spec {
        Spec::Except(self.0)
    }
}
pub struct Only(Vec<&'static str>);
impl IntoSpec for Only {
    fn into_spec(self) -> Spec {
        Spec::Only(self.0)
    }
}

pub(crate) struct PrunerRunner<B: Backend> {
    config: PrunerConfig,
    backend: B,
}
impl<B> PrunerRunner<B>
where
    B: Backend + Send + Sync + 'static,
{
    pub fn new(backend: B, config: PrunerConfig) -> Self {
        Self { backend, config }
    }

    pub fn spawn(self, cancellation_token: CancellationToken) {
        tokio::spawn({
            async move {
                loop {
                    let next = self
                        .config
                        .schedule
                        .upcoming(Utc)
                        .next()
                        .expect("No future scheduled time for pruner");
                    let delay = next
                        .sub(Utc::now())
                        .sub(TimeDelta::milliseconds(10))
                        .to_std()
                        .unwrap_or(Duration::ZERO);
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {
                            self.prune().await;
                            let delay = next - Utc::now();
                            if delay > TimeDelta::zero() {
                                tokio::time::sleep(delay.to_std().unwrap()).await;
                            }
                        }
                        _ = cancellation_token.cancelled() => {
                            tracing::debug!("Shutting down the job pruner");
                            break;
                        },
                    }
                }
            }
        });
    }

    async fn prune(&self) {
        self.config
            .pruners
            .iter()
            .map(|prune_spec| self.backend.prune_jobs(prune_spec))
            .collect::<FuturesOrdered<_>>()
            .filter_map(|res| async { res.err() })
            .for_each_concurrent(self.config.max_concurrency, |err| async move {
                tracing::error!(?err, "Failed to clean up jobs with error {err}")
            })
            .await;
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use crate::executor::test::{MockReturnExecutor, SimpleExecutor};

    use super::*;

    #[test]
    fn config() {
        let config = PrunerConfig::new(cron::Schedule::from_str("0 0 * * * *").unwrap())
            .with_pruner(
                Pruner::max_age(TimeDelta::days(31), JobStatus::Complete)
                    .only::<SimpleExecutor>()
                    .and::<MockReturnExecutor>(),
            )
            .with_pruner(
                Pruner::max_length(200, JobStatus::Discarded)
                    .except::<SimpleExecutor>()
                    .and::<MockReturnExecutor>(),
            );

        assert_eq!(config.pruners.len(), 2);
    }
}
