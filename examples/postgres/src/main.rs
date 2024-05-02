use std::fmt::Display;
use std::time::Duration;

use async_trait::async_trait;
use chrono::TimeDelta;
use rexecutor::prelude::*;
use rexecutor_sqlx::RexecutorPgBackend;

const DEFAULT_DATABASE_URL: &str = "postgresql://postgres:postgres@localhost:5432/postgres";
const DATABASE_URL: &str = "DATABASE_URL";

#[tokio::main]
pub async fn main() {
    let db_url = std::env::var(DATABASE_URL).unwrap_or_else(|_| DEFAULT_DATABASE_URL.to_owned());
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .init();

    let pruner_config = PrunerConfig::new("0/2 * * * * *".try_into().unwrap())
        .with_pruner(Pruner::max_length(20, JobStatus::Complete).only::<CronJob>())
        .with_pruner(Pruner::max_age(TimeDelta::days(1), JobStatus::Complete).except::<CronJob>())
        .with_pruners([
            Pruner::max_age(TimeDelta::days(2), JobStatus::Discarded),
            Pruner::max_age(TimeDelta::days(2), JobStatus::Cancelled),
        ]);

    let every_second = cron::Schedule::try_from("* * * * * *").unwrap();

    let backend = RexecutorPgBackend::from_db_url(&db_url).await.unwrap();
    backend.run_migrations().await.unwrap();

    let _guard = Rexecutor::new(backend)
        .with_executor::<BasicJob>()
        .with_executor::<TimeoutJob>()
        .with_executor::<CancelledJob>()
        .with_executor::<SnoozeJob>()
        .with_executor::<FlakyJob>()
        .with_cron_executor::<CronJob>(every_second.clone(), "tick".to_owned())
        .with_cron_executor::<CronJob>(every_second, "tock".to_owned())
        .with_job_pruner(pruner_config)
        .set_global_backend()
        .unwrap()
        .drop_guard();

    let job_id = BasicJob::builder()
        .with_max_attempts(3)
        .with_tags(vec!["initial_job"])
        .with_data("First job".into())
        .with_metadata("Delayed attempt".into())
        .schedule_in(TimeDelta::seconds(2))
        .enqueue()
        .await
        .unwrap();
    println!("Inserted job {job_id}");

    let job_id_2 = BasicJob::builder()
        .with_max_attempts(2)
        .add_tag("second_job")
        .add_tag("running_again")
        .with_data("Second job".into())
        .with_priority(2)
        .enqueue()
        .await
        .unwrap();
    println!("Inserted job {job_id_2}");

    let job_id = FlakyJob::builder()
        .with_max_attempts(3)
        .with_tags(vec!["flaky_job"])
        .with_data("Flaky job".into())
        .schedule_in(TimeDelta::seconds(1))
        .enqueue()
        .await
        .unwrap();
    println!("Inserted job {job_id}");

    let job_id = FlakyJob::builder()
        .with_data("Discarded job".into())
        .enqueue()
        .await
        .unwrap();
    println!("Inserted job {job_id}");

    let job_id = TimeoutJob::builder().with_data(15).enqueue().await.unwrap();
    println!("Inserted job {job_id}");

    let job_id = CancelledJob::builder().enqueue().await.unwrap();
    println!("Inserted job {job_id}");

    let job_id = SnoozeJob::builder().enqueue().await.unwrap();
    println!("Inserted job {job_id}");
    // Based on the uniqueness criteria of the job this will override the priority.
    let job_id = SnoozeJob::builder()
        .with_priority(42)
        .enqueue()
        .await
        .unwrap();
    println!("Inserted job {job_id}");

    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    BasicJob::rerun_job(job_id_2).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let recent_jobs = BasicJob::query_jobs(
        Where::tagged_by_one_of(&["second_job", "initial_job"])
            .and(!Where::status_equals(JobStatus::Discarded))
            .and(!Where::status_equals(JobStatus::Cancelled))
            .and(Where::scheduled_at_within_the_last(TimeDelta::minutes(10))),
    )
    .await
    .unwrap();

    assert!(recent_jobs.len() >= 2);
}

struct BasicJob;

#[async_trait]
impl Executor for BasicJob {
    type Data = String;
    type Metadata = String;
    const NAME: &'static str = "basic_job";
    const MAX_ATTEMPTS: u16 = 2;
    async fn execute(job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
        println!("{} running, with args: {}", Self::NAME, job.data);
        ExecutionResult::Done
    }
}

struct FlakyJob;

#[derive(Debug)]
struct FlakyJobError;

impl Display for FlakyJobError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for FlakyJobError {}

impl ExecutionError for FlakyJobError {
    fn error_type(&self) -> &'static str {
        "network_error"
    }
}

#[async_trait]
impl Executor for FlakyJob {
    type Data = String;
    type Metadata = ();
    const NAME: &'static str = "flaky_job";
    const MAX_ATTEMPTS: u16 = 1;
    async fn execute(job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
        println!("{} running, with args: {}", Self::NAME, job.data);
        match job.attempt {
            1 => panic!("Terrible things happened"),
            2 => FlakyJobError.into(),
            _ => ExecutionResult::Done,
        }
    }
    fn backoff(job: &Job<Self::Data, Self::Metadata>) -> TimeDelta {
        BackoffStrategy::constant(TimeDelta::seconds(1))
            .with_jitter(Jitter::Relative(0.5))
            .backoff(job.attempt)
    }
}

struct TimeoutJob;

#[async_trait]
impl Executor for TimeoutJob {
    type Data = u64;
    type Metadata = ();
    const NAME: &'static str = "timeout_job";
    const MAX_ATTEMPTS: u16 = 3;
    async fn execute(job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
        println!("{} running, with args: {}", Self::NAME, job.data);
        tokio::time::sleep(Duration::from_millis(job.data)).await;
        ExecutionResult::Done
    }

    fn backoff(job: &Job<Self::Data, Self::Metadata>) -> TimeDelta {
        BackoffStrategy::linear(TimeDelta::seconds(1))
            .with_jitter(Jitter::Absolute(TimeDelta::milliseconds(500)))
            .backoff(job.attempt)
    }

    fn timeout(job: &Job<Self::Data, Self::Metadata>) -> Option<Duration> {
        Some(Duration::from_millis(10 * job.attempt as u64))
    }
}

struct CancelledJob;

#[async_trait]
impl Executor for CancelledJob {
    type Data = ();
    type Metadata = ();
    const NAME: &'static str = "cancel_job";
    const MAX_ATTEMPTS: u16 = 1;
    async fn execute(job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
        println!("{} running, with args: {:?}", Self::NAME, job.data);
        ExecutionResult::Cancel {
            reason: Box::new("Didn't like the job"),
        }
    }
}

struct SnoozeJob;

#[async_trait]
impl Executor for SnoozeJob {
    type Data = ();
    type Metadata = ();
    const NAME: &'static str = "snooze_job";
    const MAX_ATTEMPTS: u16 = 1;
    const UNIQUENESS_CRITERIA: Option<UniquenessCriteria<'static>> = Some(
        UniquenessCriteria::by_executor()
            .and_within(TimeDelta::seconds(60))
            .on_conflict(Replace::priority().for_statuses(&JobStatus::ALL)),
    );
    async fn execute(job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
        println!("{} running, with args: {:?}", Self::NAME, job.data);
        if (job.scheduled_at - job.inserted_at).num_seconds() < 1 {
            ExecutionResult::Snooze {
                delay: TimeDelta::seconds(1),
            }
        } else {
            ExecutionResult::Done
        }
    }
}

struct CronJob;
#[async_trait]
impl Executor for CronJob {
    type Data = String;
    type Metadata = ();
    const NAME: &'static str = "cron_job";
    const MAX_ATTEMPTS: u16 = 1;
    async fn execute(job: Job<Self::Data, Self::Metadata>) -> ExecutionResult {
        println!("{} running, with args: {:?}", Self::NAME, job.data);
        ExecutionResult::Done
    }
}
