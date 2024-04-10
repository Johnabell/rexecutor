use async_trait::async_trait;
use chrono::Duration;
use serde::{de::DeserializeOwned, Serialize};
use std::{error::Error, fmt::Display};

use crate::job::{builder::JobBuilder, Job};

#[async_trait]
pub trait Executor {
    type Data;
    const NAME: &'static str;
    const MAX_ATTEMPTS: u16 = 5;
    const BLOCKING: bool = false;

    async fn execute(job: Job<Self::Data>) -> ExecutionResult;

    // TODO: make module for different backoff strategies
    fn backoff(job: &Job<Self::Data>) -> Duration {
        Duration::seconds(10 * job.attempt as i64 + 1)
    }

    // TODO: make Option
    fn timeout(_job: &Job<Self::Data>) -> std::time::Duration {
        std::time::Duration::from_secs(3000)
    }

    fn builder() -> JobBuilder<Self>
    where
        Self: Sized,
        Self::Data: Serialize + DeserializeOwned,
    {
        Default::default()
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub struct ExecutorIdentifier(String);

impl From<String> for ExecutorIdentifier {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl std::ops::Deref for ExecutorIdentifier {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub enum ExecutionResult {
    Done,
    Cancelled { reason: Box<dyn CancellationReason> },
    Snooze { delay: Duration },
    Error { error: Box<dyn ExecutionError> },
}

impl<T> From<T> for ExecutionResult
where
    T: ExecutionError + 'static,
{
    fn from(value: T) -> Self {
        Self::Error {
            error: Box::new(value),
        }
    }
}

pub trait ExecutionError: Error + Send {
    fn error_type(&self) -> &'static str;
}

pub trait CancellationReason: Display + Send {}

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
        const MAX_ATTEMPTS: u16 = 2;
        async fn execute(_job: Job<Self::Data>) -> ExecutionResult {
            ExecutionResult::Done
        }
    }
}
