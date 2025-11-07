//! Test suite for ensuring a correct implementation of a backend.
use std::{ops::Add, time::Duration};

use crate::{
    job::uniqueness_criteria::Replace,
    pruner::{PruneBy, Spec},
};
use chrono::TimeDelta;
use futures::StreamExt;

use super::*;

const DEFAULT_EXECUTOR: &str = "executor";
const DELTA: TimeDelta = TimeDelta::milliseconds(1);

impl<'a> EnqueuableJob<'a> {
    pub(crate) const DEFAULT_EXECUTOR: &'static str = DEFAULT_EXECUTOR;

    pub(crate) fn mock_job() -> Self {
        Self {
            executor: Self::DEFAULT_EXECUTOR.to_owned(),
            data: serde_json::Value::String("data".to_owned()),
            metadata: serde_json::Value::String("metadata".to_owned()),
            max_attempts: 5,
            scheduled_at: Utc::now(),
            tags: Default::default(),
            priority: 0,
            uniqueness_criteria: None,
        }
    }

    pub(crate) fn with_executor(self, executor: impl ToString) -> Self {
        Self {
            executor: executor.to_string(),
            ..self
        }
    }

    pub(crate) fn with_priority(self, priority: u16) -> Self {
        Self { priority, ..self }
    }

    pub(crate) fn with_scheduled_at(self, scheduled_at: DateTime<Utc>) -> Self {
        Self {
            scheduled_at,
            ..self
        }
    }

    pub(crate) fn with_uniqueness_criteria(
        self,
        uniqueness_criteria: Option<UniquenessCriteria<'a>>,
    ) -> Self {
        Self {
            uniqueness_criteria,
            ..self
        }
    }
}

/// Create test suite for rexecutor backend.
///
/// For backend implementors, it is useful to include this are part of your test suites.
///
/// # Example
///
/// ```
/// # use rexecutor::prelude::*;
/// use rexecutor::test_suite;
/// use rexecutor::backend::memory::InMemoryBackend;
/// test_suite!(for: InMemoryBackend::new());
/// ```
///
/// If you using a different async test attribute you can configure the marco to use that instead.
/// For example when using `sqlx::test` you could do the following:
///
/// ```ignore
/// # use rexecutor::prelude::*;
/// use rexecutor::test_suite;
/// test_suite!(
///     attr: sqlx::test,
///     args: (pool: PgPool),
///     backend: BackendImplementation::from_pool(pool).await.unwrap()
/// );
/// ```
#[macro_export]
macro_rules! test_suite {
    (for: $backend:expr) => {
        test_suite!(attr: tokio::test, args: (), backend: $backend);
    };
    (attr: $attr:meta, args: $args:tt, backend: $backend:expr) => {
        #[$attr]
        async fn subscribe_ready_jobs $args {
          let backend = $backend;
          $crate::backend::testing::subscribe_ready_jobs(backend).await;
        }
        #[$attr]
        async fn subscribe_ready_jobs_enqueuing_wakes_subscriber $args {
          let backend = $backend;
          $crate::backend::testing::subscribe_ready_jobs_enqueuing_wakes_subscriber(backend).await;
        }
        #[$attr]
        async fn subscribe_ready_jobs_streams_jobs_by_priority $args {
          let backend = $backend;
          $crate::backend::testing::subscribe_ready_jobs_streams_jobs_by_priority(backend).await;
        }
        #[$attr]
        async fn subscribe_ready_jobs_streams_only_one_steam_receives_job $args {
          let backend = $backend;
          $crate::backend::testing::subscribe_ready_jobs_streams_only_one_steam_receives_job(backend).await;
        }
        #[$attr]
        async fn enqueue $args {
          let backend = $backend;
          $crate::backend::testing::enqueue(backend).await;
        }
        #[$attr]
        async fn enqueue_unique $args {
          let backend = $backend;
          $crate::backend::testing::enqueue_unique(backend).await;
        }
        #[$attr]
        async fn enqueue_unique_no_matching $args {
          let backend = $backend;
          $crate::backend::testing::enqueue_unique_no_matching(backend).await;
        }
        #[$attr]
        async fn enqueue_unique_replace $args {
          let backend = $backend;
          $crate::backend::testing::enqueue_unique_replace(backend).await;
        }
        #[$attr]
        async fn update_job $args {
          let backend = $backend;
          $crate::backend::testing::update_job(backend).await;
        }
        #[$attr]
        async fn update_job_not_found $args {
          let backend = $backend;
          $crate::backend::testing::update_job_not_found(backend).await;
        }
        #[$attr]
        async fn mark_job_complete $args {
          let backend = $backend;
          $crate::backend::testing::mark_job_complete(backend).await;
        }
        #[$attr]
        async fn mark_job_complete_not_found $args {
          let backend = $backend;
          $crate::backend::testing::mark_job_complete_not_found(backend).await;
        }
        #[$attr]
        async fn rerun_job_first_attempt $args {
          let backend = $backend;
          $crate::backend::testing::rerun_job_first_attempt(backend).await;
        }
        #[$attr]
        async fn rerun_job_first_subsequent_attempt $args {
          let backend = $backend;
          $crate::backend::testing::rerun_job_first_subsequent_attempt(backend).await;
        }
        #[$attr]
        async fn rerun_job_not_found $args {
          let backend = $backend;
          $crate::backend::testing::rerun_job_not_found(backend).await;
        }
        #[$attr]
        async fn mark_job_snoozed_first_attempt $args {
          let backend = $backend;
          $crate::backend::testing::mark_job_snoozed_first_attempt(backend).await;
        }
        #[$attr]
        async fn mark_job_snoozed_other_attempt $args {
          let backend = $backend;
          $crate::backend::testing::mark_job_snoozed_other_attempt(backend).await;
        }
        #[$attr]
        async fn mark_job_snoozed_not_found $args {
          let backend = $backend;
          $crate::backend::testing::mark_job_snoozed_not_found(backend).await;
        }
        #[$attr]
        async fn mark_job_discarded $args {
          let backend = $backend;
          $crate::backend::testing::mark_job_discarded(backend).await;
        }
        #[$attr]
        async fn mark_job_discarded_not_found $args {
          let backend = $backend;
          $crate::backend::testing::mark_job_discarded_not_found(backend).await;
        }
        #[$attr]
        async fn mark_job_retryable $args {
          let backend = $backend;
          $crate::backend::testing::mark_job_retryable(backend).await;
        }
        #[$attr]
        async fn mark_job_retryable_not_found $args {
          let backend = $backend;
          $crate::backend::testing::mark_job_retryable_not_found(backend).await;
        }
        #[$attr]
        async fn mark_job_cancelled $args {
          let backend = $backend;
          $crate::backend::testing::mark_job_cancelled(backend).await;
        }
        #[$attr]
        async fn mark_job_cancelled_not_found $args {
          let backend = $backend;
          $crate::backend::testing::mark_job_cancelled_not_found(backend).await;
        }
        #[$attr]
        async fn query $args {
          let backend = $backend;
          $crate::backend::testing::query(backend).await;
        }
        #[$attr]
        async fn prune_jobs $args {
          let backend = $backend;
          $crate::backend::testing::prune_jobs(backend).await;
        }
    };
}

pub use test_suite;

impl Job {
    pub(crate) const DEFAULT_EXECUTOR: &'static str = DEFAULT_EXECUTOR;

    pub(crate) fn raw_job() -> Self {
        let now = Utc::now();
        Self {
            id: 0,
            executor: Self::DEFAULT_EXECUTOR.to_owned(),
            status: JobStatus::Scheduled,
            data: serde_json::Value::Null,
            metadata: serde_json::Value::Null,
            attempt: 0,
            max_attempts: 3,
            priority: 0,
            tags: vec![],
            errors: vec![],
            inserted_at: now,
            scheduled_at: now + TimeDelta::hours(2),
            attempted_at: None,
            completed_at: None,
            cancelled_at: None,
            discarded_at: None,
        }
    }
}

#[doc(hidden)]
#[async_trait::async_trait]
pub trait BackendTesting: Backend + Sync {
    /// This will simulate running a job with retrying for the given executor.
    ///
    /// This method should only be used if there is a single job enqueued for the executor,
    /// otherwise it might mark other jobs as executing.
    async fn increment_job_attempt(
        &self,
        id: JobId,
        number_of_attempts: i32,
        executor: &'static str,
    ) {
        let mut stream = self.subscribe_ready_jobs(executor.into()).await;
        for _ in 0..number_of_attempts {
            let _ = stream.next().await;
            let error = ExecutionError {
                error_type: ErrorType::Other("custom".to_owned()),
                message: "Error Message".to_owned(),
            };
            self.mark_job_retryable(id, Utc::now(), error)
                .await
                .unwrap();
        }
    }

    async fn get_job(&self, id: JobId) -> Option<Job> {
        let mut jobs = self.query(Query::IdEquals(id)).await.unwrap();
        assert!(jobs.len() <= 1);
        jobs.pop()
    }
}

impl<T: Backend + Sync> BackendTesting for T {}

#[doc(hidden)]
pub async fn subscribe_ready_jobs(backend: impl Backend) {
    let executor = "executor";
    let mut stream = backend.subscribe_ready_jobs(executor.into()).await;
    backend
        .enqueue(EnqueuableJob::mock_job().with_executor("another_executor"))
        .await
        .unwrap();
    let job_id = backend
        .enqueue(EnqueuableJob::mock_job().with_executor(executor))
        .await
        .unwrap();

    let job = stream.next().await.unwrap().unwrap();
    assert_eq!(job.id, job_id);
    assert_eq!(job.executor, executor);
}

#[doc(hidden)]
pub async fn subscribe_ready_jobs_enqueuing_wakes_subscriber(backend: impl Backend) {
    let executor = "executor";
    let mut stream = backend.subscribe_ready_jobs(executor.into()).await;
    let handle = tokio::spawn(async move {
        match tokio::time::timeout(Duration::from_secs(2), stream.next()).await {
            Ok(Some(Ok(job))) => assert_eq!(job.executor, executor),
            Err(_) => panic!("Didn't get woken by enqueue of new job"),
            _ => panic!("Bad things happened"),
        }
    });
    tokio::task::yield_now().await;
    backend
        .enqueue(EnqueuableJob::mock_job().with_executor("another_executor"))
        .await
        .unwrap();
    backend
        .enqueue(EnqueuableJob::mock_job().with_executor(executor))
        .await
        .unwrap();
    handle.await.unwrap();
}

#[doc(hidden)]
pub async fn subscribe_ready_jobs_streams_jobs_by_priority(backend: impl Backend) {
    let executor = "executor";
    let scheduled_at1 = Utc::now();
    let scheduled_at2 = Utc::now() + TimeDelta::milliseconds(500);
    let mut stream = backend.subscribe_ready_jobs(executor.into()).await;
    let job_id1 = backend
        .enqueue(
            EnqueuableJob::mock_job()
                .with_executor(executor)
                .with_scheduled_at(scheduled_at2),
        )
        .await
        .unwrap();
    let job_id2 = backend
        .enqueue(
            EnqueuableJob::mock_job()
                .with_executor(executor)
                .with_priority(2)
                .with_scheduled_at(scheduled_at1),
        )
        .await
        .unwrap();
    let job_id3 = backend
        .enqueue(
            EnqueuableJob::mock_job()
                .with_executor(executor)
                .with_scheduled_at(scheduled_at1),
        )
        .await
        .unwrap();

    let job_ids: [JobId; 3] = [
        stream.next().await.unwrap().unwrap().id.into(),
        stream.next().await.unwrap().unwrap().id.into(),
        stream.next().await.unwrap().unwrap().id.into(),
    ];
    assert_eq!(job_ids, [job_id3, job_id2, job_id1]);
}

#[doc(hidden)]
pub async fn subscribe_ready_jobs_streams_only_one_steam_receives_job(backend: impl Backend) {
    let executor = "executor";
    let mut stream1 = backend.subscribe_ready_jobs(executor.into()).await;
    let mut stream2 = backend.subscribe_ready_jobs(executor.into()).await;
    let job_id1 = backend
        .enqueue(EnqueuableJob::mock_job().with_executor(executor))
        .await
        .unwrap();
    let job_id2 = backend
        .enqueue(EnqueuableJob::mock_job().with_executor(executor))
        .await
        .unwrap();

    let job_ids: std::collections::HashSet<_> =
        futures::future::join_all([stream1.next(), stream2.next()])
            .await
            .into_iter()
            .map(|res| res.unwrap().unwrap().id)
            .collect();
    let expected = [i32::from(job_id1), i32::from(job_id2)]
        .into_iter()
        .collect();
    assert_eq!(job_ids, expected);
}

#[doc(hidden)]
pub async fn enqueue(backend: impl BackendTesting) {
    let id1 = backend.enqueue(EnqueuableJob::mock_job()).await.unwrap();
    let id2 = backend.enqueue(EnqueuableJob::mock_job()).await.unwrap();

    assert_ne!(id1, id2);
    assert!(backend.get_job(id1).await.is_some());
    assert!(backend.get_job(id2).await.is_some());
}

#[doc(hidden)]
pub async fn enqueue_unique(backend: impl BackendTesting) {
    let id1 = backend.enqueue(EnqueuableJob::mock_job()).await.unwrap();
    let id2 = backend
        .enqueue(EnqueuableJob::mock_job().with_uniqueness_criteria(Some(
            UniquenessCriteria::by_executor().and_within(TimeDelta::minutes(2)),
        )))
        .await
        .unwrap();

    assert_eq!(id2, id1);
    assert!(backend.get_job(id2).await.is_some());
}

#[doc(hidden)]
pub async fn enqueue_unique_no_matching(backend: impl BackendTesting) {
    let id1 = backend.enqueue(EnqueuableJob::mock_job()).await.unwrap();
    let id2 = backend
        .enqueue(
            EnqueuableJob::mock_job().with_uniqueness_criteria(Some(
                UniquenessCriteria::by_statuses(&[JobStatus::Retryable])
                    .and_within(TimeDelta::minutes(2)),
            )),
        )
        .await
        .unwrap();

    assert_ne!(id1, id2);
    assert!(backend.get_job(id1).await.is_some());
    assert!(backend.get_job(id2).await.is_some());
}

#[doc(hidden)]
pub async fn enqueue_unique_replace(backend: impl BackendTesting) {
    let id1 = backend
        .enqueue(EnqueuableJob::mock_job().with_priority(2))
        .await
        .unwrap();
    let id2 = backend
        .enqueue(
            EnqueuableJob::mock_job().with_uniqueness_criteria(Some(
                UniquenessCriteria::by_executor()
                    .and_within(TimeDelta::minutes(2))
                    .on_conflict(
                        Replace::priority()
                            .and_data()
                            .and_metadata()
                            .and_scheduled_at()
                            .and_max_attempts(),
                    ),
            )),
        )
        .await
        .unwrap();

    assert_eq!(id2, id1);
    let job = backend.get_job(id1).await.expect("Job should be enqueued");
    // Priority has been updated
    assert_eq!(job.priority, 0);
}

#[doc(hidden)]
pub async fn update_job(backend: impl BackendTesting) {
    let id = backend.enqueue(EnqueuableJob::mock_job()).await.unwrap();
    let mut job = backend.get_job(id).await.unwrap();
    job.priority = 3;

    assert!(backend.update_job(job).await.is_ok());

    let job = backend.get_job(id).await.unwrap();
    assert_eq!(job.priority, 3);
}

#[doc(hidden)]
pub async fn update_job_not_found(backend: impl BackendTesting) {
    let job = Job::raw_job();
    assert!(matches!(
        backend.update_job(job).await,
        Err(BackendError::JobNotFound(_))
    ));
}

#[doc(hidden)]
pub async fn mark_job_complete(backend: impl BackendTesting) {
    let id = backend.enqueue(EnqueuableJob::mock_job()).await.unwrap();

    assert!(backend.mark_job_complete(id).await.is_ok());
    assert_eq!(
        backend.get_job(id).await.unwrap().status,
        JobStatus::Complete
    );
}

#[doc(hidden)]
pub async fn mark_job_complete_not_found(backend: impl BackendTesting) {
    assert!(matches!(
        backend.mark_job_complete(42.into()).await,
        Err(BackendError::JobNotFound(_))
    ));
}

#[doc(hidden)]
pub async fn rerun_job_first_attempt(backend: impl BackendTesting) {
    let job = EnqueuableJob::mock_job();
    let original_max_attempts = job.max_attempts as i32;
    let id = backend.enqueue(job).await.unwrap();
    backend
        .increment_job_attempt(id, 1, EnqueuableJob::DEFAULT_EXECUTOR)
        .await;

    assert!(backend.rerun_job(id).await.is_ok());

    let job = backend.get_job(id).await.unwrap();
    assert_eq!(job.status, JobStatus::Scheduled);
    assert_eq!(job.attempt, 1);
    assert_eq!(job.max_attempts, original_max_attempts + 1);
    assert!(job.completed_at.is_none());
    assert!(job.cancelled_at.is_none());
    assert!(job.discarded_at.is_none());
}

#[doc(hidden)]
pub async fn rerun_job_first_subsequent_attempt(backend: impl BackendTesting) {
    let job = EnqueuableJob::mock_job();
    let original_max_attempts = job.max_attempts as i32;
    let id = backend.enqueue(job).await.unwrap();
    backend
        .increment_job_attempt(id, 2, EnqueuableJob::DEFAULT_EXECUTOR)
        .await;

    assert!(backend.rerun_job(id).await.is_ok());

    let job = backend.get_job(id).await.unwrap();
    assert_eq!(job.status, JobStatus::Retryable);
    assert_eq!(job.attempt, 2);
    assert_eq!(job.max_attempts, original_max_attempts + 1);
    assert!(job.completed_at.is_none());
    assert!(job.cancelled_at.is_none());
    assert!(job.discarded_at.is_none());
}

#[doc(hidden)]
pub async fn rerun_job_not_found(backend: impl BackendTesting) {
    assert!(matches!(
        backend.rerun_job(42.into()).await,
        Err(BackendError::JobNotFound(_))
    ));
}

#[doc(hidden)]
pub async fn mark_job_snoozed_first_attempt(backend: impl BackendTesting) {
    let scheduled_at = Utc::now().add(TimeDelta::days(1));
    let id = backend.enqueue(EnqueuableJob::mock_job()).await.unwrap();
    backend
        .increment_job_attempt(id, 1, EnqueuableJob::DEFAULT_EXECUTOR)
        .await;

    assert!(backend.mark_job_snoozed(id, scheduled_at).await.is_ok());

    let job = backend.get_job(id).await.unwrap();
    assert_eq!(job.status, JobStatus::Scheduled);
    assert_eq!(job.attempt, 0);
    assert!((job.scheduled_at - scheduled_at).abs() < DELTA);
}

#[doc(hidden)]
pub async fn mark_job_snoozed_other_attempt(backend: impl BackendTesting) {
    let scheduled_at = Utc::now().add(TimeDelta::days(1));
    let id = backend.enqueue(EnqueuableJob::mock_job()).await.unwrap();
    backend
        .increment_job_attempt(id, 2, EnqueuableJob::DEFAULT_EXECUTOR)
        .await;

    assert!(backend.mark_job_snoozed(id, scheduled_at).await.is_ok());

    let job = backend.get_job(id).await.unwrap();
    assert_eq!(job.status, JobStatus::Retryable);
    assert_eq!(job.attempt, 1);
    assert!((job.scheduled_at - scheduled_at).abs() < DELTA);
}

#[doc(hidden)]
pub async fn mark_job_snoozed_not_found(backend: impl BackendTesting) {
    let scheduled_at = Utc::now().add(TimeDelta::days(1));
    assert!(matches!(
        backend.mark_job_snoozed(42.into(), scheduled_at).await,
        Err(BackendError::JobNotFound(_))
    ));
}

#[doc(hidden)]
pub async fn mark_job_discarded(backend: impl BackendTesting) {
    let id = backend.enqueue(EnqueuableJob::mock_job()).await.unwrap();
    let error = ExecutionError {
        error_type: ErrorType::Other("custom".to_owned()),
        message: "Error Message".to_owned(),
    };

    assert!(backend.mark_job_discarded(id, error.clone()).await.is_ok());

    let job = backend.get_job(id).await.unwrap();
    assert_eq!(job.status, JobStatus::Discarded);
    assert_eq!(job.errors.len(), 1);

    let job_error = job.errors.first().unwrap();
    assert_eq!(job_error.attempt as i32, job.attempt);
    assert_eq!(job_error.error_type, error.error_type);
    assert_eq!(job_error.details, error.message);
}

#[doc(hidden)]
pub async fn mark_job_discarded_not_found(backend: impl BackendTesting) {
    let error = ExecutionError {
        error_type: ErrorType::Other("custom".to_owned()),
        message: "Error Message".to_owned(),
    };
    assert!(matches!(
        backend.mark_job_discarded(42.into(), error).await,
        Err(BackendError::JobNotFound(_))
    ));
}

#[doc(hidden)]
pub async fn mark_job_retryable(backend: impl BackendTesting) {
    let scheduled_at = Utc::now().add(TimeDelta::days(1));
    let id = backend.enqueue(EnqueuableJob::mock_job()).await.unwrap();
    let error = ExecutionError {
        error_type: ErrorType::Other("custom".to_owned()),
        message: "Error Message".to_owned(),
    };

    assert!(
        backend
            .mark_job_retryable(id, scheduled_at, error.clone())
            .await
            .is_ok()
    );

    let job = backend.get_job(id).await.unwrap();
    assert_eq!(job.status, JobStatus::Retryable);
    assert_eq!(job.errors.len(), 1);

    let job_error = job.errors.first().unwrap();
    assert_eq!(job_error.attempt as i32, job.attempt);
    assert_eq!(job_error.error_type, error.error_type);
    assert_eq!(job_error.details, error.message);
}

#[doc(hidden)]
pub async fn mark_job_retryable_not_found(backend: impl BackendTesting) {
    let scheduled_at = Utc::now().add(TimeDelta::days(1));
    let error = ExecutionError {
        error_type: ErrorType::Other("custom".to_owned()),
        message: "Error Message".to_owned(),
    };
    assert!(matches!(
        backend
            .mark_job_retryable(42.into(), scheduled_at, error)
            .await,
        Err(BackendError::JobNotFound(_))
    ));
}

#[doc(hidden)]
pub async fn mark_job_cancelled(backend: impl BackendTesting) {
    let id = backend.enqueue(EnqueuableJob::mock_job()).await.unwrap();
    let error = ExecutionError {
        error_type: ErrorType::Other("custom".to_owned()),
        message: "Error Message".to_owned(),
    };

    assert!(backend.mark_job_cancelled(id, error.clone()).await.is_ok());

    let job = backend.get_job(id).await.unwrap();
    assert_eq!(job.status, JobStatus::Cancelled);
    assert_eq!(job.errors.len(), 1);

    let job_error = job.errors.first().unwrap();
    assert_eq!(job_error.attempt as i32, job.attempt);
    assert_eq!(job_error.error_type, error.error_type);
    assert_eq!(job_error.details, error.message);
}

#[doc(hidden)]
pub async fn mark_job_cancelled_not_found(backend: impl BackendTesting) {
    let error = ExecutionError {
        error_type: ErrorType::Other("custom".to_owned()),
        message: "Error Message".to_owned(),
    };
    assert!(matches!(
        backend.mark_job_cancelled(42.into(), error).await,
        Err(BackendError::JobNotFound(_))
    ));
}

#[doc(hidden)]
pub async fn query(backend: impl BackendTesting) {
    let id = backend.enqueue(EnqueuableJob::mock_job()).await.unwrap();
    let _ = backend
        .enqueue(EnqueuableJob::mock_job().with_executor("other_executor"))
        .await
        .unwrap();
    let _ = backend.enqueue(EnqueuableJob::mock_job()).await.unwrap();

    assert_eq!(
        backend
            .query(Query::IdEquals(42.into()))
            .await
            .unwrap()
            .len(),
        0
    );
    assert_eq!(backend.query(Query::IdEquals(id)).await.unwrap().len(), 1);
    assert_eq!(
        backend
            .query(Query::ExecutorEqual(EnqueuableJob::DEFAULT_EXECUTOR))
            .await
            .unwrap()
            .len(),
        2
    );
}

#[doc(hidden)]
pub async fn prune_jobs(backend: impl BackendTesting) {
    let now = Utc::now();
    for i in 0..100 {
        let id = backend
            .enqueue(EnqueuableJob::mock_job().with_scheduled_at(now - TimeDelta::hours(i)))
            .await
            .unwrap();
        backend.mark_job_complete(id).await.unwrap();
    }
    for _ in 0..100 {
        let id = backend
            .enqueue(EnqueuableJob::mock_job().with_executor("other_executor"))
            .await
            .unwrap();
        backend.mark_job_complete(id).await.unwrap();
    }

    backend
        .prune_jobs(&PruneSpec {
            status: JobStatus::Cancelled,
            prune_by: PruneBy::MaxLength(10),
            executors: Spec::Except(vec![]),
        })
        .await
        .unwrap();

    // No jobs have been pruned
    let all_jobs = Query::Not(Box::new(Query::ExecutorEqual("")));
    assert_eq!(backend.query(all_jobs).await.unwrap().len(), 200);

    backend
        .prune_jobs(&PruneSpec {
            status: JobStatus::Complete,
            prune_by: PruneBy::MaxLength(50),
            executors: Spec::Only(vec!["other_executor"]),
        })
        .await
        .unwrap();

    // 50 jobs have been pruned for `"other_executor"`
    let all_jobs = Query::Not(Box::new(Query::ExecutorEqual("")));
    assert_eq!(backend.query(all_jobs).await.unwrap().len(), 150);

    backend
        .prune_jobs(&PruneSpec {
            status: JobStatus::Complete,
            prune_by: PruneBy::MaxAge(TimeDelta::hours(50)),
            executors: Spec::Only(vec![EnqueuableJob::DEFAULT_EXECUTOR]),
        })
        .await
        .unwrap();

    // 50 more jobs have been pruned for `EnqueuableJob::DEFAULT_EXECUTOR`
    let all_jobs = Query::Not(Box::new(Query::ExecutorEqual("")));
    assert_eq!(backend.query(all_jobs).await.unwrap().len(), 100);
}
