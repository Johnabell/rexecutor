use std::fmt::Display;
use std::time::Duration;

use async_trait::async_trait;
use chrono::TimeDelta;
use rexecutor::{
    executor::{ExecutionError, ExecutionResult, Executor},
    job::{uniqueness_criteria::UniquenessCriteria, Job},
    Rexecuter,
};
use rexecutor_sqlx::RexecutorPgBackend;
use sqlx::postgres::PgPoolOptions;

const DEFAULT_DATABASE_URL: &str = "postgresql://postgres:postgres@localhost:5432/postgres";
const DATABASE_URL: &str = "DATABASE_URL";

#[tokio::main]
pub async fn main() {
    let db_url = std::env::var(DATABASE_URL).unwrap_or_else(|_| DEFAULT_DATABASE_URL.to_owned());
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .init();

    let pool = PgPoolOptions::new().connect(&db_url).await.unwrap();
    let backend = RexecutorPgBackend::new(pool).await.unwrap();
    let schedule = cron::Schedule::try_from("* * * * * *").unwrap();

    let handle = Rexecuter::new(backend.clone())
        .with_executor::<BasicJob>()
        .with_executor::<TimeoutJob>()
        .with_executor::<CancelledJob>()
        .with_executor::<SnoozeJob>()
        .with_executor::<FlakyJob>()
        .with_cron_executor::<CronJob>(schedule.clone(), "tick".to_owned())
        .with_cron_executor::<CronJob>(schedule, "tock".to_owned())
        .set_global_backend()
        .unwrap();

    let job_id = BasicJob::builder()
        .with_max_attempts(3)
        .with_tags(vec!["initial_job"])
        .with_data("First job".into())
        .schedule_in(TimeDelta::seconds(2))
        .enqueue()
        .await
        .unwrap();
    println!("Inserted job {job_id}");

    let job_id = BasicJob::builder()
        .with_max_attempts(2)
        .add_tag("second_job")
        .add_tag("running_again")
        .with_data("Second job".into())
        .enqueue()
        .await
        .unwrap();
    println!("Inserted job {job_id}");

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

    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    let _ = handle.graceful_shutdown().await;
}

struct BasicJob;

#[async_trait]
impl Executor for BasicJob {
    type Data = String;
    const NAME: &'static str = "basic_job";
    const MAX_ATTEMPTS: u16 = 2;
    async fn execute(job: Job<Self::Data>) -> ExecutionResult {
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
    const NAME: &'static str = "flaky_job";
    const MAX_ATTEMPTS: u16 = 1;
    async fn execute(job: Job<Self::Data>) -> ExecutionResult {
        println!("{} running, with args: {}", Self::NAME, job.data);
        match job.attempt {
            1 => panic!("Terrible things happened"),
            2 => FlakyJobError.into(),
            _ => ExecutionResult::Done,
        }
    }
    fn backoff(_job: &Job<Self::Data>) -> TimeDelta {
        TimeDelta::seconds(1)
    }
}

struct TimeoutJob;

#[async_trait]
impl Executor for TimeoutJob {
    type Data = u64;
    const NAME: &'static str = "timeout_job";
    const MAX_ATTEMPTS: u16 = 3;
    async fn execute(job: Job<Self::Data>) -> ExecutionResult {
        println!("{} running, with args: {}", Self::NAME, job.data);
        tokio::time::sleep(Duration::from_millis(job.data)).await;
        ExecutionResult::Done
    }

    fn backoff(_job: &Job<Self::Data>) -> TimeDelta {
        TimeDelta::seconds(1)
    }

    fn timeout(job: &Job<Self::Data>) -> Option<Duration> {
        Some(Duration::from_millis(10 * job.attempt as u64))
    }
}

struct CancelledJob;

#[async_trait]
impl Executor for CancelledJob {
    type Data = ();
    const NAME: &'static str = "cancel_job";
    const MAX_ATTEMPTS: u16 = 1;
    async fn execute(job: Job<Self::Data>) -> ExecutionResult {
        println!("{} running, with args: {:?}", Self::NAME, job.data);
        ExecutionResult::Cancelled {
            reason: Box::new("Didn't like the job"),
        }
    }
}

struct SnoozeJob;

#[async_trait]
impl Executor for SnoozeJob {
    type Data = ();
    const NAME: &'static str = "snooze_job";
    const MAX_ATTEMPTS: u16 = 1;
    const UNIQUENESS_CRITERIA: Option<UniquenessCriteria<'static>> = Some(
        UniquenessCriteria::new()
            .by_executor()
            .by_duration(TimeDelta::seconds(60)),
    );
    async fn execute(job: Job<Self::Data>) -> ExecutionResult {
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
    const NAME: &'static str = "cron_job";
    const MAX_ATTEMPTS: u16 = 1;
    async fn execute(job: Job<Self::Data>) -> ExecutionResult {
        println!("{} running, with args: {:?}", Self::NAME, job.data);
        ExecutionResult::Done
    }
}
