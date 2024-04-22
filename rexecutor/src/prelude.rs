//! The purpose of this module is to alleviate the need to import many of the `[rexecutor]` types.
//!
//! ```
//! # #![allow(unused_imports)]
//! use rexecutor::prelude::*;
//! ```
pub use crate::backoff::BackoffStrategy;
pub use crate::backoff::Jitter;
pub use crate::backoff::Strategy;
pub use crate::executor::{ExecutionError, ExecutionResult, Executor};
pub use crate::job::query::Where;
pub use crate::job::uniqueness_criteria::{Replace, UniquenessCriteria};
pub use crate::job::{Job, JobStatus};
pub use crate::pruner::Pruner;
pub use crate::pruner::PrunerConfig;
pub use crate::Rexecutor;
