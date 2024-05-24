//! Helpers for testing.
#![allow(clippy::vec_init_then_push)]
#![allow(unused)]

/// A macro for making assertions about what jobs should have been enqueued.
///
/// Facilitates making assertions about what jobs have been enqueued into the backend.
/// Assertions can be based on data, metadata, tags, executor, and scheduled_at.
///
/// # Example
///
/// ```
/// # use rexecutor::prelude::*;
/// # use rexecutor::testing::assert_enqueued;
/// # use chrono::{Utc, TimeDelta};
/// # pub(crate) struct SimpleExecutor;
/// #
/// # #[async_trait::async_trait]
/// # impl Executor for SimpleExecutor {
/// #     type Data = String;
/// #     type Metadata = String;
/// #     const NAME: &'static str = "simple_executor";
/// #     const MAX_ATTEMPTS: u16 = 2;
/// #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
/// #         ExecutionResult::Done
/// #     }
/// # }
/// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
/// use rexecutor::backend::memory::InMemoryBackend;
/// let backend = InMemoryBackend::new().paused();
/// let scheduled_at = Utc::now() + TimeDelta::minutes(5);
///
/// SimpleExecutor::builder()
///     .with_data("data".to_owned())
///     .with_metadata("metadata".to_owned())
///     .with_tags(vec!["tag1", "tag2"])
///     .schedule_at(scheduled_at)
///     .enqueue_to_backend(&backend)
///     .await
///     .unwrap();
///
/// SimpleExecutor::builder()
///     .with_data("data2".to_owned())
///     .with_metadata("metadata2".to_owned())
///     .with_tags(vec!["tag2", "tag3"])
///     .schedule_at(scheduled_at)
///     .enqueue_to_backend(&backend)
///     .await
///     .unwrap();
///
///
/// assert_enqueued!(
///     to: backend,
///     with_data: "data".to_owned(),
///     scheduled_at: scheduled_at,
///     for_executor: SimpleExecutor
/// );
///
/// assert_enqueued!(
///     1 job,
///     to: backend,
///     with_metadata: "metadata2".to_owned(),
///     scheduled_after: Utc::now(),
///     for_executor: SimpleExecutor
/// );
///
/// assert_enqueued!(
///     2 jobs,
///     to: backend,
///     tagged_with: ["tag2"],
///     scheduled_before: Utc::now() + TimeDelta::hours(1),
///     for_executor: SimpleExecutor
/// );
/// # });
/// ```
///
///
/// # Example global backend
///
/// When working with the global backend, it is possible to use the macro without stating to which
/// backend the job should have been enqueued.
///
/// ```
/// # use rexecutor::prelude::*;
/// # use rexecutor::testing::assert_enqueued;
/// # use chrono::Utc;
/// # pub(crate) struct SimpleExecutor;
/// #
/// # #[async_trait::async_trait]
/// # impl Executor for SimpleExecutor {
/// #     type Data = String;
/// #     type Metadata = String;
/// #     const NAME: &'static str = "simple_executor";
/// #     const MAX_ATTEMPTS: u16 = 2;
/// #     async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
/// #         ExecutionResult::Done
/// #     }
/// # }
/// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
/// use rexecutor::backend::memory::InMemoryBackend;
///
/// let backend = InMemoryBackend::new().paused();
///
/// Rexecutor::new(backend).set_global_backend().unwrap();
///
/// SimpleExecutor::builder()
///     .with_data("data".to_owned())
///     .enqueue()
///     .await
///     .unwrap();
///
/// assert_enqueued!(
///     with_data: "data".to_owned(),
///     for_executor: SimpleExecutor
/// );
///
/// SimpleExecutor::builder()
///     .with_data("data".to_owned())
///     .enqueue()
///     .await
///     .unwrap();
///
/// assert_enqueued!(
///     2 jobs,
///     with_data: "data".to_owned(),
///     for_executor: SimpleExecutor
/// );
/// # });
/// ```
#[macro_export]
macro_rules! assert_enqueued {
    (1 job, to: $backend:ident, $($tail:tt)*) => {
        assert_enqueued!(@internal 1 to: $backend, $($tail)*);
    };
    ($n:literal jobs, to: $backend:ident, $($tail:tt)*) => {
        assert_enqueued!(@internal $n to: $backend, $($tail)*);
    };
    (1 job, $($tail:tt)*) => {
        assert_enqueued!(@global 1 $($tail)*);
    };
    ($n:literal jobs, $($tail:tt)*) => {
        assert_enqueued!(@global $n $($tail)*);
    };
    (to: $backend:ident, $($tail:tt)*) => {
        assert_enqueued!(@internal 1 to: $backend, $($tail)*);
    };
    (@global $n:literal $($tail:tt)*) => {{
        let backend = $crate::global_backend::GlobalBackend::as_ref().expect("Global backend not set");
        assert_enqueued!(@internal $n to: backend, $($tail)*);
    }};
    (@internal 1 to: $backend:ident, $($tail:tt)*) => {{
        use $crate::backend::Query;
        use $crate::backend::Backend;
        let mut queries = Vec::new();
        assert_enqueued!(@query queries; $($tail)*);
        let jobs = $backend.query(Query::And(queries)).await.unwrap();
        let all_jobs = $backend.query(Query::TagsAllOf(&[])).await.unwrap();
        assert!(
            !jobs.is_empty(),
            "No jobs enqueue {}\n\nAll enqueued jobs:\n{all_jobs:#?}",
            stringify!($($tail)*)
        );
    }};
    (@internal $n:literal to: $backend:ident, $($tail:tt)*) => {{
        use $crate::backend::Query;
        use $crate::backend::Backend;
        let mut queries = Vec::new();
        assert_enqueued!(@query queries; $($tail)*);
        let jobs = $backend.query(Query::And(queries)).await.unwrap();
        let all_jobs = $backend.query(Query::TagsAllOf(&[])).await.unwrap();
        assert!(
            jobs.len() == $n,
            "Unexpected number of jobs enqueued {},\n\n\
            Expected {} jobs, found {} matching job enqueued:\n\n\
            Matching jobs:\n\
            {jobs:#?}\n\n\
            All enqueued jobs:\n\
            {all_jobs:#?}",
            stringify!($($tail)*),
            $n,
            jobs.len(),
        );
    }};
    (@query $vec:ident; with_data: $data:expr,) => {
        $vec.push(Query::DataEquals(serde_json::to_value($data).unwrap()));
    };
    (@query $vec:ident; with_data: $data:expr $(, $($tail:tt)*)?) => {
        $vec.push(Query::DataEquals(serde_json::to_value($data).unwrap()));
        $(assert_enqueued!(@query $vec; $($tail)*))?
    };
    (@query $vec:ident; with_metadata: $metadata:expr,) => {
        $vec.push(Query::MetadataEquals(serde_json::to_value($metadata).unwrap()));
    };
    (@query $vec:ident; with_metadata: $metadata:expr $(, $($tail:tt)*)?) => {
        $vec.push(Query::MetadataEquals(serde_json::to_value($metadata).unwrap()));
        $(assert_enqueued!(@query $vec; $($tail)*))?
    };
    (@query $vec:ident; tagged_with: $tags:expr,) => {
        $vec.push(Query::TagsAllOf(&$tags));
    };
    (@query $vec:ident; tagged_with: $tags:expr $(, $($tail:tt)*)?) => {
        $vec.push(Query::TagsAllOf(&$tags));
        $(assert_enqueued!(@query $vec; $($tail)*))?
    };
    (@query $vec:ident; scheduled_at: $scheduled_at:expr,) => {
        $vec.push(Query::ScheduledAtEqual($scheduled_at));
    };
    (@query $vec:ident; scheduled_at: $scheduled_at:expr $(, $($tail:tt)*)?) => {
        $vec.push(Query::ScheduledAtEqual($scheduled_at));
        $(assert_enqueued!(@query $vec; $($tail)*))?
    };
    (@query $vec:ident; scheduled_after: $scheduled_after:expr,) => {
        $vec.push(Query::ScheduledAtAfter($scheduled_after));
    };
    (@query $vec:ident; scheduled_after: $scheduled_after:expr $(, $($tail:tt)*)?) => {
        $vec.push(Query::ScheduledAtAfter($scheduled_after));
        $(assert_enqueued!(@query $vec; $($tail)*))?
    };
    (@query $vec:ident; scheduled_before: $scheduled_before:expr,) => {
        $vec.push(Query::ScheduledAtBefore($scheduled_before));
    };
    (@query $vec:ident; scheduled_before: $scheduled_before:expr $(, $($tail:tt)*)?) => {
        $vec.push(Query::ScheduledAtBefore($scheduled_before));
        $(assert_enqueued!(@query $vec; $($tail)*))?
    };
    (@query $vec:ident; for_executor: $executor:literal,) => {
        $vec.push(Query::ExecutorEqual($executor));
    };
    (@query $vec:ident; for_executor: $executor:literal $(, $($tail:tt)*)?) => {
        $vec.push(Query::ExecutorEqual($executor));
        $(assert_enqueued!(@query $vec; $($tail)*))?
    };
    (@query $vec:ident; for_executor: $executor:path,) => {{
        use $executor as base;
        $vec.push(Query::ExecutorEqual(base::NAME));
    }};
    (@query $vec:ident; for_executor: $executor:path $(, $($tail:tt)*)?) => {
        use $executor as base;
        $vec.push(Query::ExecutorEqual(base::NAME));
        $(assert_enqueued!(@query $vec; $($tail)*))?
    };
    ($($tail:tt)*) => {
        assert_enqueued!(@global 1 $($tail)*);
    };
}

pub use assert_enqueued;

#[cfg(test)]
mod test {
    use chrono::{TimeDelta, Utc};

    use crate::{
        backend::memory::InMemoryBackend,
        executor::{test::SimpleExecutor, Executor},
        Rexecutor,
    };

    #[tokio::test]
    async fn assert_enqueued() {
        let backend = InMemoryBackend::new().paused();
        let scheduled_at = Utc::now() + TimeDelta::minutes(5);

        SimpleExecutor::builder()
            .with_data("data".to_owned())
            .with_metadata("metadata".to_owned())
            .with_tags(vec!["tag1", "tag2", "tag3"])
            .schedule_at(scheduled_at)
            .enqueue_to_backend(&backend)
            .await
            .unwrap();

        assert_enqueued!(
            1 job,
            to: backend,
            with_data: "data".to_owned(),
            for_executor: SimpleExecutor
        );
        assert_enqueued!(
            to: backend,
            with_data: "data".to_owned(),
            for_executor: "simple_executor",
        );
        assert_enqueued!(
            to: backend,
            with_metadata: "metadata".to_owned(),
            for_executor: SimpleExecutor
        );
        assert_enqueued!(
            to: backend,
            scheduled_at: scheduled_at,
            for_executor: SimpleExecutor
        );
        assert_enqueued!(
            to: backend,
            scheduled_after: Utc::now(),
            for_executor: SimpleExecutor
        );
        assert_enqueued!(
            to: backend,
            scheduled_before: Utc::now() + TimeDelta::hours(1),
            for_executor: SimpleExecutor
        );
        assert_enqueued!(
            to: backend,
            with_data: "data".to_owned(),
            for_executor: SimpleExecutor,
        );
        assert_enqueued!(
            to: backend,
            with_data: "data".to_owned(),
            tagged_with: ["tag1", "tag2"],
            for_executor: SimpleExecutor,
        );
        assert_enqueued!(
            0 jobs,
            to: backend,
            with_data: "data2".to_owned(),
            for_executor: SimpleExecutor
        );
    }
}
