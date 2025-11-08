use std::{ops::Sub, time::Duration};

use chrono::{TimeDelta, Utc};
use futures::{StreamExt, stream::FuturesOrdered};
use tokio_util::sync::CancellationToken;

use crate::backend::Backend;

use super::PrunerConfig;

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
