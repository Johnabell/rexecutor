use async_trait::async_trait;
use chrono::Duration;
use rexecutor::{
    executor::{ExecutionResult, Executor},
    job::{builder::JobBuilder, Job},
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
    let backend: RexecutorPgBackend = pool.into();
    // Might be nice to make this be able to take a PgPool directly
    let mut _handle = Rexecuter::new(backend.clone())
        .with_executor::<BasicJob>()
        .await
        .spawn();

    let job_id = JobBuilder::<BasicJob>::default()
        .with_max_attempts(2)
        .with_tags(vec!["initial_job"])
        .with_data("First job".into())
        .schedule_in(Duration::seconds(2))
        // Might be nice to make this be able to take a reference to a PgPool directly
        .enqueue(&backend)
        .await
        .unwrap();
    println!("Inserted job {job_id}");

    let job_id = JobBuilder::<BasicJob>::default()
        .with_max_attempts(2)
        .with_tags(vec!["second_job"])
        .with_data("Second job".into())
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
