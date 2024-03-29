use async_trait::async_trait;
use chrono::Duration;
use std::{error::Error, fmt::Display};

use crate::job::Job;

#[async_trait]
pub trait Executor {
    type Data;
    const NAME: &'static str;
    const MAX_ATTEMPTS: u32 = 5;
    async fn execute(job: Job<Self::Data>) -> ExecutionResult;

    fn backoff(job: Job<Self::Data>) -> Duration {
        Duration::seconds(10 * job.attempt as i64 + 1)
    }

    fn timeout(_job: Job<Self::Data>) -> Duration {
        Duration::minutes(5)
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct ExecutorIdentifier(String);

pub enum ExecutionResult {
    Done,
    Cancelled { reason: Box<dyn Display> },
    Snooze { delay: Duration },
    Error { error: Box<dyn ExecutionError> },
}

pub trait ExecutionError: Error {
    fn error_type(&self) -> &'static str;
}

#[cfg(test)]
pub(crate) mod test {
    use async_trait::async_trait;

    use crate::job::Job;

    use super::*;

    pub(crate) struct SimpleExecutor;

    #[async_trait]
    impl Executor for SimpleExecutor {
        type Data = String;
        const NAME: &'static str = "simple_executor";
        const MAX_ATTEMPTS: u32 = 2;
        async fn execute(_job: Job<Self::Data>) -> ExecutionResult {
            ExecutionResult::Done
        }
    }
}
