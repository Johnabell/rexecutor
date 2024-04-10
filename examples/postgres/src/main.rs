use std::fmt::Display;
use std::time::Duration;

use async_trait::async_trait;
use chrono::TimeDelta;
use rexecutor::{
    executor::{ExecutionError, ExecutionResult, Executor},
    job::Job,
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
    // Might be nice to make this be able to take a PgPool directly
    let mut _handle = Rexecuter::new(backend.clone())
        .with_executor::<BasicJob>()
        .with_executor::<TimeoutJob>()
        .with_executor::<FlakyJob>();

    let job_id = BasicJob::builder()
        .with_max_attempts(3)
        .with_tags(vec!["initial_job"])
        .with_data("First job".into())
        .schedule_in(TimeDelta::seconds(2))
        // Might be nice to make this be able to take a reference to a PgPool directly
        .enqueue(&backend)
        .await
        .unwrap();
    println!("Inserted job {job_id}");

    let job_id = BasicJob::builder()
        .with_max_attempts(2)
        .with_tags(vec!["second_job"])
        .with_data("Second job".into())
        .enqueue(&backend)
        .await
        .unwrap();
    println!("Inserted job {job_id}");

    let job_id = FlakyJob::builder()
        .with_max_attempts(3)
        .with_tags(vec!["flaky_job"])
        .with_data("Flaky job".into())
        .schedule_in(TimeDelta::seconds(1))
        .enqueue(&backend)
        .await
        .unwrap();
    println!("Inserted job {job_id}");

    let job_id = FlakyJob::builder()
        .with_data("Discarded job".into())
        .enqueue(&backend)
        .await
        .unwrap();
    println!("Inserted job {job_id}");

    let job_id = TimeoutJob::builder()
        .with_data("Timeout job".into())
        .enqueue(&backend)
        .await
        .unwrap();
    println!("Inserted job {job_id}");

    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
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
    type Data = String;
    const NAME: &'static str = "timeout_job";
    const MAX_ATTEMPTS: u16 = 3;
    async fn execute(job: Job<Self::Data>) -> ExecutionResult {
        println!("{} running, with args: {}", Self::NAME, job.data);
        tokio::time::sleep(Duration::from_millis(15)).await;
        ExecutionResult::Done
    }

    fn backoff(_job: &Job<Self::Data>) -> TimeDelta {
        TimeDelta::seconds(1)
    }

    fn timeout(job: &Job<Self::Data>) -> Duration {
        Duration::from_millis(10 * job.attempt as u64)
    }
}
