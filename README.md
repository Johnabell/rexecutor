# Rexecutor

A robust job execution library for rust built on the tokio runtime.

For example usage see [postgres example](examples/postgres/src/main.rs).

## Setting up `Rexecutor`

To create an instance of `Rexecutor` you will need to have an implementation of `Backend`.

The rexecutor library only provides and in memory implementation `backend::memory::InMemoryBackend`
which is primarily provided for testing purposes. Instead a seperate crate implementing the Backend
should be used for example `rexecutor-sqlx`

## Creating executors

Jobs are defined by creating a struct/enum and implementing `Executor` for it.

### Example defining an executor

You can define and enqueue a job as follows:

```rust
use rexecutor::prelude::*;
use chrono::{Utc, TimeDelta};
use rexecutor::backend::memory::InMemoryBackend;
use rexecutor::assert_enqueued;
let backend = InMemoryBackend::new().paused();
Rexecutor::new(backend).set_global_backend().unwrap();
struct EmailJob;

#[async_trait::async_trait]
impl Executor for EmailJob {
    type Data = String;
    type Metadata = String;
    const NAME: &'static str = "email_job";
    const MAX_ATTEMPTS: u16 = 2;
    async fn execute(job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
        println!("{} running, with args: {}", Self::NAME, job.data);
        /// Do something important with an email
        ExecutionResult::Done
    }
}

tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
    let _ = EmailJob::builder()
        .with_data("bob.shuruncle@example.com".to_owned())
        .schedule_in(TimeDelta::hours(3))
        .enqueue()
        .await;

    assert_enqueued!(
        with_data: "bob.shuruncle@example.com".to_owned(),
        scheduled_after: Utc::now() + TimeDelta::minutes(170),
        scheduled_before: Utc::now() + TimeDelta::minutes(190),
        for_executor: EmailJob
    );
});
```

### Unique jobs

It is possible to ensure uniqueness of jobs based on certain criteria. This can be defined as
part of the implementation of `Executor` via `Executor::UNIQUENESS_CRITERIA` or when
inserting the job via `job::builder::JobBuilder::unique`.

For example to ensure that only one unique job is ran every five minutes it is possible to use
the following uniqueness criteria.

```rust
use rexecutor::prelude::*;
use chrono::{Utc, TimeDelta};
use rexecutor::backend::memory::InMemoryBackend;
use rexecutor::assert_enqueued;
let backend = InMemoryBackend::new().paused();
Rexecutor::new(backend).set_global_backend().unwrap();
struct UniqueJob;

#[async_trait::async_trait]
impl Executor for UniqueJob {
    type Data = ();
    type Metadata = ();
    const NAME: &'static str = "unique_job";
    const MAX_ATTEMPTS: u16 = 1;
    const UNIQUENESS_CRITERIA: Option<UniquenessCriteria<'static>> = Some(
        UniquenessCriteria::by_executor()
            .and_within(TimeDelta::seconds(300))
            .on_conflict(Replace::priority().for_statuses(&JobStatus::ALL)),
    );
    async fn execute(job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
        println!("{} running, with args: {:?}", Self::NAME, job.data);
        // Do something important
        ExecutionResult::Done
    }
}

tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
    let _ = UniqueJob::builder().enqueue().await;
    let _ = UniqueJob::builder().enqueue().await;

    // Only one of jobs was enqueued
    assert_enqueued!(
        1 job,
        scheduled_before: Utc::now(),
        for_executor: UniqueJob
    );
});
```

Additionally it is possible to specify what action should be taken when there is a conflicting
job. In the example above the priority is override. For more details of how to use uniqueness
see `job::uniqueness_criteria::UniquenessCriteria`.


## Overriding `Executor` default values

When defining an `Executor` you specify the maximum number of attempts via
`Executor::MAX_ATTEMPTS`. However, when inserting a job it is possible to override this value
by calling `job::builder::JobBuilder::with_max_attempts` (if not called the max attempts will be equal to
`Executor::MAX_ATTEMPTS`).

Similarly, the executor can define a job uniqueness criteria via
`Executor::UNIQUENESS_CRITERIA`. However, using `job::builder::JobBuilder::unique` it is possible to
override this value for a specific job.

## Setting up executors to run

For each executor you would like to run `Rexecutor::with_executor` should be called. Being
explicit about this opens the possibility of having specific nodes in a cluster running as
worker nodes for certain enqueued jobs while other node not responsible for their execution.

### Example setting up executors

```rust
use rexecutor::prelude::*;
use std::str::FromStr;
use chrono::TimeDelta;
use rexecutor::backend::memory::InMemoryBackend;
pub(crate) struct RefreshWorker;
pub(crate) struct EmailScheduler;
pub(crate) struct RegistrationWorker;

#[async_trait::async_trait]
impl Executor for RefreshWorker {
    type Data = String;
    type Metadata = String;
    const NAME: &'static str = "refresh_worker";
    const MAX_ATTEMPTS: u16 = 2;
    async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
        ExecutionResult::Done
    }
}
#[async_trait::async_trait]
impl Executor for EmailScheduler {
    type Data = String;
    type Metadata = String;
    const NAME: &'static str = "email_scheduler";
    const MAX_ATTEMPTS: u16 = 2;
    async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
        ExecutionResult::Done
    }
}
#[async_trait::async_trait]
impl Executor for RegistrationWorker {
    type Data = String;
    type Metadata = String;
    const NAME: &'static str = "registration_worker";
    const MAX_ATTEMPTS: u16 = 2;
    async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
        ExecutionResult::Done
    }
}
tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
    let backend = InMemoryBackend::new();
    Rexecutor::new(backend)
        .with_executor::<RefreshWorker>()
        .with_executor::<EmailScheduler>()
        .with_executor::<RegistrationWorker>();
});
```

## Enqueuing jobs

Generally jobs will be enqueued using the `job::builder::JobBuilder` returned by `Executor::builder`.

When enqueuing jobs the data and metadata of the job can be specified. Additionally, the
default value of the `Executor` can be overriden.

### Overriding `Executor` default values

When defining an `Executor` you specify the maximum number of attempts via
`Executor::MAX_ATTEMPTS`. However, when inserting a job it is possible to override this value
by calling `job::builder::JobBuilder::with_max_attempts` (if not called the max attempts will be equal to
`Executor::MAX_ATTEMPTS`.

Similarly, the executor can define a job uniqueness criteria via
`Executor::UNIQUENESS_CRITERIA`. However, using `job::builder::JobBuilder::unique` it is possible to
override this value for a specific job.

### Example enqueuing a job

```rust
use rexecutor::prelude::*;
use std::sync::Arc;
use chrono::{Utc, TimeDelta};
use rexecutor::backend::memory::InMemoryBackend;
use rexecutor::assert_enqueued;
pub(crate) struct ExampleExecutor;

#[async_trait::async_trait]
impl Executor for ExampleExecutor {
    type Data = String;
    type Metadata = String;
    const NAME: &'static str = "simple_executor";
    const MAX_ATTEMPTS: u16 = 2;
    async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
        ExecutionResult::Done
    }
}
tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
    let backend = Arc::new(InMemoryBackend::new().paused());
    Rexecutor::new(backend.clone()).set_global_backend().unwrap();

    ExampleExecutor::builder()
        .with_max_attempts(2)
        .with_tags(vec!["initial_job", "delayed"])
        .with_data("First job".into())
        .schedule_in(TimeDelta::hours(2))
        .enqueue_to_backend(&backend)
        .await
        .unwrap();

    assert_enqueued!(
        to: backend,
        with_data: "First job".to_owned(),
        tagged_with: ["initial_job", "delayed"],
        scheduled_after: Utc::now() + TimeDelta::minutes(110),
        scheduled_before: Utc::now() + TimeDelta::minutes(130),
        for_executor: ExampleExecutor
    );
});
```

## Compile time scheduling of cron jobs

It can be useful to have jobs that run on a given schedule. Jobs like this can be setup using
either `Rexecutor::with_cron_executor` or `Rexecutor::with_cron_executor_for_timezone`. The
later is use to specify the specific timezone that the jobs should be scheduled to run in.

### Example setting up a UTC cron job

To setup a cron jobs to run every day at midnight you can use the following code.

```rust
use rexecutor::prelude::*;
use rexecutor::backend::{Backend, memory::InMemoryBackend};
struct CronJob;
#[async_trait::async_trait]
impl Executor for CronJob {
    type Data = String;
    type Metadata = ();
    const NAME: &'static str = "cron_job";
    const MAX_ATTEMPTS: u16 = 1;
    async fn execute(job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
        /// Do something important
        ExecutionResult::Done
    }
}
tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
    let schedule = cron::Schedule::try_from("0 0 0 * * *").unwrap();

    let backend = InMemoryBackend::new();
    Rexecutor::new(backend).with_cron_executor::<CronJob>(schedule, "important data".to_owned());
});
```

## Pruning jobs

After jobs have completed, been cancelled, or discarded it is useful to be able to clean up.
To setup the job pruner `Rexecutor::with_job_pruner` should be called passing in the given
`PrunerConfig`.

Given the different ways in which jobs can finish it is often useful to be able to have fine
grained control over how old jobs should be cleaned up. `PrunerConfig` enables such control.

When constructing `PrunerConfig` a `cron::Schedule` is provided to specify when the pruner
should run.

Depending on the load/throughput of the system the pruner can be scheduled to run anywhere
from once a year through to multiple times per hour.

### Example configuring the job pruner

```rust
use rexecutor::prelude::*;
use std::str::FromStr;
use chrono::TimeDelta;
use rexecutor::backend::memory::InMemoryBackend;
pub(crate) struct RefreshWorker;
pub(crate) struct EmailScheduler;
pub(crate) struct RegistrationWorker;

#[async_trait::async_trait]
impl Executor for RefreshWorker {
    type Data = String;
    type Metadata = String;
    const NAME: &'static str = "refresh_worker";
    const MAX_ATTEMPTS: u16 = 2;
    async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
        ExecutionResult::Done
    }
}
#[async_trait::async_trait]
impl Executor for EmailScheduler {
    type Data = String;
    type Metadata = String;
    const NAME: &'static str = "email_scheduler";
    const MAX_ATTEMPTS: u16 = 2;
    async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
        ExecutionResult::Done
    }
}
#[async_trait::async_trait]
impl Executor for RegistrationWorker {
    type Data = String;
    type Metadata = String;
    const NAME: &'static str = "registration_worker";
    const MAX_ATTEMPTS: u16 = 2;
    async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
        ExecutionResult::Done
    }
}
let config = PrunerConfig::new(cron::Schedule::from_str("0 0 * * * *").unwrap())
    .with_max_concurrency(Some(2))
    .with_pruner(
        Pruner::max_age(TimeDelta::days(31), JobStatus::Complete)
            .only::<RefreshWorker>()
            .and::<EmailScheduler>(),
    )
    .with_pruner(
        Pruner::max_length(200, JobStatus::Discarded)
            .except::<RefreshWorker>()
            .and::<EmailScheduler>(),
    );

tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
    let backend = InMemoryBackend::new();
    Rexecutor::new(backend)
        .with_executor::<RefreshWorker>()
        .with_executor::<EmailScheduler>()
        .with_executor::<RegistrationWorker>()
        .with_job_pruner(config);
});
```

## Shutting rexecutor down

To avoid jobs getting killed mid way through their executions it is important to make use of
graceful shutdown. This can either explicitly be called using `Rexecutor::graceful_shutdown`,
or via use of the `DropGuard` obtained via `Rexecutor::drop_guard`.

Using `Rexecutor::graceful_shutdown` or `Rexecutor::drop_guard` will ensure that all
currently executing jobs will be allowed time to complete before shutting rexecutor down.

### Example using the `DropGuard`

```rust
use rexecutor::prelude::*;
use std::str::FromStr;
use chrono::TimeDelta;
use rexecutor::backend::memory::InMemoryBackend;
pub(crate) struct RefreshWorker;
pub(crate) struct EmailScheduler;
pub(crate) struct RegistrationWorker;

#[async_trait::async_trait]
impl Executor for RefreshWorker {
    type Data = String;
    type Metadata = String;
    const NAME: &'static str = "refresh_worker";
    const MAX_ATTEMPTS: u16 = 2;
    async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
        ExecutionResult::Done
    }
}
#[async_trait::async_trait]
impl Executor for EmailScheduler {
    type Data = String;
    type Metadata = String;
    const NAME: &'static str = "email_scheduler";
    const MAX_ATTEMPTS: u16 = 2;
    async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
        ExecutionResult::Done
    }
}
#[async_trait::async_trait]
impl Executor for RegistrationWorker {
    type Data = String;
    type Metadata = String;
    const NAME: &'static str = "registration_worker";
    const MAX_ATTEMPTS: u16 = 2;
    async fn execute(_job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
        ExecutionResult::Done
    }
}
tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
    let backend = InMemoryBackend::new();
    // Note this must be given a name to ensure it is dropped at the end of the scope.
    // See https://doc.rust-lang.org/book/ch18-03-pattern-syntax.html#ignoring-an-unused-variable-by-starting-its-name-with-_
    let _guard = Rexecutor::new(backend)
        .with_executor::<RefreshWorker>()
        .with_executor::<EmailScheduler>()
        .with_executor::<RegistrationWorker>()
        .drop_guard();
});
```

## Global backend

Rexecutor can be ran either with use of a global backend. This enables the use of the
convenience `job::builder::JobBuilder::enqueue` method which does not require a reference to
the backend to be passed down to the code that needs to enqueue a job.

The global backend can be set using `Rexecutor::set_global_backend` this should only be
called once otherwise it will return an error.

In fact for a single `Rexecutor` instance it is impossible to call this twice, the following code
snippet will fail to compile

```rust
use rexecutor::prelude::*;
let backend = rexecutor::backend::memory::InMemoryBackend::new();
Rexecutor::new(backend).set_global_backend().set_global_backend();
```

Note, using a global backend has many of the same drawbacks of any global variable in
particular it can make unit testing more difficult.

## Code of conduct

We follow the [Rust code of conduct](https://www.rust-lang.org/policies/code-of-conduct).

Currently the moderation team consists of John Bell only. We would welcome more members: if you would like to join the moderation team, please contact John Bell.

## Licence

The project is licensed under the [MIT license](https://github.com/Johnabell/atom_box/blob/master/LICENSE).

