//! Common backoff strategies for use as part of [`crate::Executor::backoff`].
//!
//! This module provides four main backoff strategies:
//!
//! 1. Constant
//! 2. Linear
//! 3. Polynomial
//! 4. Exponential
//!
//! each which can be optionally modified by applying different types of jitter.
//!
//! All of the constructors and configuration functions are `const`.
//!
//! # Example
//!
//! ```
//! # use rexecutor::prelude::*;
//! # use chrono::TimeDelta;
//! let strategy = BackoffStrategy::linear(TimeDelta::seconds(20))
//!     .with_max(TimeDelta::seconds(60))
//!     .with_jitter(Jitter::Absolute(TimeDelta::seconds(10)));
//!
//! assert!(strategy.backoff(1) >= TimeDelta::seconds(10));
//! assert!(strategy.backoff(1) <= TimeDelta::seconds(30));
//! assert!(strategy.backoff(2) >= TimeDelta::seconds(30));
//! assert!(strategy.backoff(2) <= TimeDelta::seconds(50));
//! assert!(strategy.backoff(3) >= TimeDelta::seconds(50));
//! // Note the max here is the max plus max jitter
//! assert!(strategy.backoff(3) <= TimeDelta::seconds(70));
//! assert!(strategy.backoff(10) >= TimeDelta::seconds(50));
//! assert!(strategy.backoff(10) <= TimeDelta::seconds(70));
//! ```

use chrono::TimeDelta;
use rand::Rng;

/// Type that can be used to implement a backoff strategy.
pub trait Strategy {
    /// Given a job attempt as a number returns the [`TimeDelta`] to wait before the job should be
    /// retried.
    fn backoff(&self, attempt: u16) -> TimeDelta;
}

/// Constant backoff strategy.
///
/// Always returns the same value no matter what the attempt is.
///
/// __Note:__ This type cannot be constructed directly, instead [`BackoffStrategy::constant`]
/// should be used.
///
/// # Example
///
/// ```
/// # use rexecutor::prelude::*;
/// # use chrono::TimeDelta;
///
/// let strategy = BackoffStrategy::constant(TimeDelta::seconds(10));
///
/// assert_eq!(strategy.backoff(1), TimeDelta::seconds(10));
/// assert_eq!(strategy.backoff(2), TimeDelta::seconds(10));
/// assert_eq!(strategy.backoff(3), TimeDelta::seconds(10));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Constant {
    delay: TimeDelta,
}

impl Strategy for Constant {
    fn backoff(&self, _attempt: u16) -> TimeDelta {
        self.delay
    }
}

/// Exponential backoff strategy.
///
/// Grows exponentially with each attempt. It is also possible, and advisable, to set the maximum
/// backoff using [`BackoffStrategy::with_max`].
///
/// __Note:__ This type cannot be constructed directly, instead [`BackoffStrategy::exponential`]
/// should be used.
///
/// # Example
///
/// ```
/// # use rexecutor::prelude::*;
/// # use chrono::TimeDelta;
///
/// let strategy =
///     BackoffStrategy::exponential(TimeDelta::seconds(2)).with_max(TimeDelta::seconds(30));
///
/// assert_eq!(strategy.backoff(1), TimeDelta::seconds(2));
/// assert_eq!(strategy.backoff(2), TimeDelta::seconds(4));
/// assert_eq!(strategy.backoff(3), TimeDelta::seconds(8));
/// assert_eq!(strategy.backoff(4), TimeDelta::seconds(16));
/// assert_eq!(strategy.backoff(5), TimeDelta::seconds(30));
/// assert_eq!(strategy.backoff(6), TimeDelta::seconds(30));
/// ```
pub struct Exponential {
    base: TimeDelta,
    max: Option<TimeDelta>,
}

impl Strategy for Exponential {
    fn backoff(&self, attempt: u16) -> TimeDelta {
        let mut seconds = self
            .base
            .num_seconds()
            .checked_pow(attempt.into())
            .unwrap_or(i64::MAX);
        if let Some(max) = self.max {
            seconds = seconds.min(max.num_seconds());
        }
        TimeDelta::seconds(seconds)
    }
}

/// Linear backoff strategy.
///
/// Grows linear with each attempt. It is also possible to set the maximum backoff using
/// [`BackoffStrategy::with_max`].
///
/// __Note:__ This type cannot be constructed directly, instead [`BackoffStrategy::linear`]
/// should be used.
///
/// # Example
///
/// ```
/// # use rexecutor::prelude::*;
/// # use chrono::TimeDelta;
///
/// let strategy = BackoffStrategy::linear(TimeDelta::seconds(10)).with_max(TimeDelta::seconds(40));
///
/// assert_eq!(strategy.backoff(1), TimeDelta::seconds(10));
/// assert_eq!(strategy.backoff(2), TimeDelta::seconds(20));
/// assert_eq!(strategy.backoff(3), TimeDelta::seconds(30));
/// assert_eq!(strategy.backoff(4), TimeDelta::seconds(40));
/// assert_eq!(strategy.backoff(5), TimeDelta::seconds(40));
/// ```
pub struct Linear {
    factor: TimeDelta,
    max: Option<TimeDelta>,
}

impl Strategy for Linear {
    fn backoff(&self, attempt: u16) -> TimeDelta {
        let mut backoff = self.factor * attempt.into();
        if let Some(max) = self.max {
            backoff = backoff.min(max);
        }
        backoff
    }
}

/// Polynomial backoff strategy.
///
/// Grows in a polynomial manor with each attempt. It is also possible to set the maximum backoff using
/// [`BackoffStrategy::with_max`].
///
/// __Note:__ This type cannot be constructed directly, instead [`BackoffStrategy::polynomial`]
/// should be used.
///
/// # Example
///
/// ```
/// # use rexecutor::prelude::*;
/// # use chrono::TimeDelta;
///
/// let strategy =
///     BackoffStrategy::polynomial(TimeDelta::seconds(10), 2).with_max(TimeDelta::seconds(200));
///
/// assert_eq!(strategy.backoff(1), TimeDelta::seconds(10));
/// assert_eq!(strategy.backoff(2), TimeDelta::seconds(40));
/// assert_eq!(strategy.backoff(3), TimeDelta::seconds(90));
/// assert_eq!(strategy.backoff(4), TimeDelta::seconds(160));
/// assert_eq!(strategy.backoff(5), TimeDelta::seconds(200));
/// assert_eq!(strategy.backoff(6), TimeDelta::seconds(200));
/// ```
pub struct Polynomial {
    factor: TimeDelta,
    power: u32,
    max: Option<TimeDelta>,
}

impl Strategy for Polynomial {
    fn backoff(&self, attempt: u16) -> TimeDelta {
        let mut backoff = self.factor * (attempt as i32).pow(self.power);
        if let Some(max) = self.max {
            backoff = backoff.min(max);
        }
        backoff
    }
}

/// A random jitter to be applied to a given backoff.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Jitter {
    /// A random jitter to be added to the backoff in the range `-delta =< jitter =< delta`.
    Absolute(TimeDelta),
    /// A random jitter to be added as a proportion of the current backoff.
    Relative(f64),
}

impl Jitter {
    fn apply_jitter(&self, value: TimeDelta) -> TimeDelta {
        let milliseconds = match self {
            Self::Absolute(delta) => delta.num_milliseconds(),
            Self::Relative(ratio) => (value.num_milliseconds() as f64 * ratio).round() as i64,
        };
        let rand_jitter_seconds = rand::thread_rng().gen_range(-milliseconds..=milliseconds);
        value + TimeDelta::milliseconds(rand_jitter_seconds)
    }
}

/// Common backoff strategies for use as part of [`crate::Executor::backoff`].
///
/// This type provides four main backoff strategies:
///
/// 1. Constant
/// 2. Linear
/// 3. Polynomial
/// 4. Exponential
///
/// each which can be optionally modified by applying different types of jitter.
///
/// All of the constructors and configuration functions are `const`.
///
/// # Example
///
/// ```
/// # use rexecutor::prelude::*;
/// # use chrono::TimeDelta;
/// let strategy = BackoffStrategy::linear(TimeDelta::seconds(20))
///     .with_max(TimeDelta::seconds(60))
///     .with_jitter(Jitter::Absolute(TimeDelta::seconds(10)));
///
/// assert!(strategy.backoff(1) >= TimeDelta::seconds(10));
/// assert!(strategy.backoff(1) <= TimeDelta::seconds(30));
/// assert!(strategy.backoff(2) >= TimeDelta::seconds(30));
/// assert!(strategy.backoff(2) <= TimeDelta::seconds(50));
/// assert!(strategy.backoff(3) >= TimeDelta::seconds(50));
/// // Note the max here is the max plus max jitter
/// assert!(strategy.backoff(3) <= TimeDelta::seconds(70));
/// assert!(strategy.backoff(10) >= TimeDelta::seconds(50));
/// assert!(strategy.backoff(10) <= TimeDelta::seconds(70));
/// ```
pub struct BackoffStrategy<T: Strategy> {
    strategy: T,
    jitter: Option<Jitter>,
    additional_offset: Option<TimeDelta>,
    min: TimeDelta,
}

impl BackoffStrategy<Constant> {
    /// Creates a [`BackoffStrategy`] with a constant backoff strategy.
    ///
    /// This will always return the same value no matter what the attempt number is.
    ///
    /// # Example
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    /// # use chrono::TimeDelta;
    ///
    /// let strategy = BackoffStrategy::constant(TimeDelta::seconds(10));
    ///
    /// assert_eq!(strategy.backoff(1), TimeDelta::seconds(10));
    /// assert_eq!(strategy.backoff(2), TimeDelta::seconds(10));
    /// assert_eq!(strategy.backoff(3), TimeDelta::seconds(10));
    /// ```
    pub const fn constant(delay: TimeDelta) -> Self {
        Self::new(Constant { delay })
    }
}

impl BackoffStrategy<Exponential> {
    /// Creates a [`BackoffStrategy`] with a exponential backoff strategy.
    ///
    /// Grows exponentially with each attempt. It is also possible, and advisable, to set the maximum
    /// backoff using [`BackoffStrategy::with_max`].
    ///
    /// # Example
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    /// # use chrono::TimeDelta;
    ///
    /// let strategy =
    ///     BackoffStrategy::exponential(TimeDelta::seconds(2)).with_max(TimeDelta::seconds(30));
    ///
    /// assert_eq!(strategy.backoff(1), TimeDelta::seconds(2));
    /// assert_eq!(strategy.backoff(2), TimeDelta::seconds(4));
    /// assert_eq!(strategy.backoff(3), TimeDelta::seconds(8));
    /// assert_eq!(strategy.backoff(4), TimeDelta::seconds(16));
    /// assert_eq!(strategy.backoff(5), TimeDelta::seconds(30));
    /// assert_eq!(strategy.backoff(6), TimeDelta::seconds(30));
    /// ```
    pub const fn exponential(base: TimeDelta) -> Self {
        Self::new(Exponential { base, max: None })
    }

    /// Clamps the maximum value to be returned by [`Strategy::backoff`] to `max_delay`.
    pub const fn with_max(mut self, max_delay: TimeDelta) -> Self {
        self.strategy.max = Some(max_delay);
        self
    }
}

impl BackoffStrategy<Linear> {
    /// Creates a [`BackoffStrategy`] with a linear backoff strategy.
    ///
    /// Grows linear with each attempt. It is also possible to set the maximum backoff using
    /// [`BackoffStrategy::with_max`].
    ///
    /// # Example
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    /// # use chrono::TimeDelta;
    ///
    /// let strategy = BackoffStrategy::linear(TimeDelta::seconds(10)).with_max(TimeDelta::seconds(40));
    ///
    /// assert_eq!(strategy.backoff(1), TimeDelta::seconds(10));
    /// assert_eq!(strategy.backoff(2), TimeDelta::seconds(20));
    /// assert_eq!(strategy.backoff(3), TimeDelta::seconds(30));
    /// assert_eq!(strategy.backoff(4), TimeDelta::seconds(40));
    /// assert_eq!(strategy.backoff(5), TimeDelta::seconds(40));
    /// ```
    pub const fn linear(factor: TimeDelta) -> Self {
        Self::new(Linear { factor, max: None })
    }

    /// Clamps the maximum value to be returned by [`Strategy::backoff`] to `max_delay`.
    pub const fn with_max(mut self, max_delay: TimeDelta) -> Self {
        self.strategy.max = Some(max_delay);
        self
    }
}

impl BackoffStrategy<Polynomial> {
    /// Creates a [`BackoffStrategy`] with a polynomial backoff strategy.
    ///
    /// Grows in a polynomial manor with each attempt. It is also possible to set the maximum backoff using
    /// [`BackoffStrategy::with_max`].
    ///
    /// # Example
    ///
    /// ```
    /// # use rexecutor::prelude::*;
    /// # use chrono::TimeDelta;
    ///
    /// let strategy =
    ///     BackoffStrategy::polynomial(TimeDelta::seconds(10), 2).with_max(TimeDelta::seconds(200));
    ///
    /// assert_eq!(strategy.backoff(1), TimeDelta::seconds(10));
    /// assert_eq!(strategy.backoff(2), TimeDelta::seconds(40));
    /// assert_eq!(strategy.backoff(3), TimeDelta::seconds(90));
    /// assert_eq!(strategy.backoff(4), TimeDelta::seconds(160));
    /// assert_eq!(strategy.backoff(5), TimeDelta::seconds(200));
    /// assert_eq!(strategy.backoff(6), TimeDelta::seconds(200));
    /// ```
    pub const fn polynomial(factor: TimeDelta, power: u32) -> Self {
        Self::new(Polynomial {
            factor,
            power,
            max: None,
        })
    }

    /// Clamps the maximum value to be returned by [`Strategy::backoff`] to `max_delay`.
    pub const fn with_max(mut self, max_delay: TimeDelta) -> Self {
        self.strategy.max = Some(max_delay);
        self
    }
}

impl<T> BackoffStrategy<T>
where
    T: Strategy,
{
    /// Creates a [`BackoffStrategy`] with a the given backoff strategy.
    ///
    /// Generally this function will only be used if you have implemented your own custom
    /// [`Strategy`]. More commonly [`BackoffStrategy`] is constructed via the strategy specific
    /// construct functions:
    ///
    /// - [`BackoffStrategy::constant`]
    /// - [`BackoffStrategy::linear`]
    /// - [`BackoffStrategy::polynomial`]
    /// - [`BackoffStrategy::exponential`]
    pub const fn new(strategy: T) -> Self {
        Self {
            strategy,
            jitter: None,
            additional_offset: None,
            min: TimeDelta::zero(),
        }
    }
    /// Add a jitter to the backoff strategy see [`Jitter`] for more information about how this
    /// affects the strategy.
    pub const fn with_jitter(mut self, jitter: Jitter) -> Self {
        self.jitter = Some(jitter);
        self
    }
    /// Add a minimum value. This can be useful when you have a particularly large jitter and would
    /// like to avoid a delay of less than a given amount.
    pub const fn with_min(mut self, min: TimeDelta) -> Self {
        self.min = min;
        self
    }

    // TODO: do we want this?
    #[doc(hidden)]
    pub const fn with_offset(mut self, offset: TimeDelta) -> Self {
        self.additional_offset = Some(offset);
        self
    }
}

impl<T> Strategy for BackoffStrategy<T>
where
    T: Strategy,
{
    fn backoff(&self, attempt: u16) -> TimeDelta {
        let mut backoff = self.strategy.backoff(attempt);

        if let Some(additional_offset) = self.additional_offset {
            backoff += additional_offset;
        }

        if let Some(jitter) = self.jitter {
            backoff = jitter.apply_jitter(backoff);
        }

        backoff.max(self.min)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn constant_backoff() {
        let delay = TimeDelta::minutes(1);
        let strategy = BackoffStrategy::constant(delay);

        for i in 1..100 {
            assert_eq!(strategy.backoff(i), delay);
        }
    }

    #[test]
    fn constant_backoff_with_absolute_jitter() {
        let delay = TimeDelta::minutes(1);
        let jitter = TimeDelta::seconds(10);
        let strategy = BackoffStrategy::constant(delay).with_jitter(Jitter::Absolute(jitter));

        for i in 1..100 {
            let backoff = strategy.backoff(i);
            assert!(backoff >= delay - jitter);
            assert!(backoff <= delay + jitter);
        }
    }

    #[test]
    fn constant_backoff_with_relative_jitter() {
        let delay = TimeDelta::minutes(1);
        let strategy = BackoffStrategy::constant(delay).with_jitter(Jitter::Relative(0.1));

        for i in 1..100 {
            let jitter = TimeDelta::seconds(10);
            let backoff = strategy.backoff(i);
            assert!(backoff >= delay - jitter);
            assert!(backoff <= delay + jitter);
        }
    }

    #[test]
    fn constant_backoff_with_jitter_min() {
        let delay = TimeDelta::seconds(20);
        let jitter = TimeDelta::seconds(20);
        let min = TimeDelta::seconds(5);
        let strategy = BackoffStrategy::constant(delay)
            .with_jitter(Jitter::Absolute(jitter))
            .with_min(min);

        for i in 1..100 {
            let backoff = strategy.backoff(i);
            assert!(backoff >= min);
            assert!(backoff <= delay + jitter);
        }
    }

    #[test]
    fn polynomial_backoff() {
        let delay = TimeDelta::minutes(1);
        let strategy = BackoffStrategy::polynomial(delay, 2);

        for i in 1..100 {
            assert_eq!(strategy.backoff(i), delay * i.pow(2) as _);
        }
    }

    #[test]
    fn polynomial_backoff_with_max() {
        let delay = TimeDelta::minutes(1);
        let max = TimeDelta::minutes(10);
        let strategy = BackoffStrategy::polynomial(delay, 2).with_max(max);

        for i in 1..100 {
            let backoff = strategy.backoff(i);
            assert!(backoff <= max);
        }
    }

    #[test]
    fn linear_backoff() {
        let delay = TimeDelta::minutes(1);
        let strategy = BackoffStrategy::linear(delay);

        for i in 1..100 {
            assert_eq!(strategy.backoff(i), delay * i as _);
        }
    }

    #[test]
    fn linear_backoff_with_max() {
        let delay = TimeDelta::minutes(1);
        let max = TimeDelta::minutes(10);
        let strategy = BackoffStrategy::linear(delay).with_max(max);

        for i in 1..100 {
            let backoff = strategy.backoff(i);
            assert!(backoff <= max);
        }
    }

    #[test]
    fn linear_backoff_with_absolute_jitter() {
        let delay = TimeDelta::minutes(1);
        let jitter = TimeDelta::seconds(10);
        let strategy = BackoffStrategy::linear(delay).with_jitter(Jitter::Absolute(jitter));

        for i in 1..100 {
            let backoff = strategy.backoff(i);
            assert!(backoff >= delay * i as _ - jitter);
            assert!(backoff <= delay * i as _ + jitter);
        }
    }

    #[test]
    fn linear_backoff_with_relative_jitter() {
        let delay = TimeDelta::minutes(1);
        let strategy = BackoffStrategy::linear(delay).with_jitter(Jitter::Relative(0.1));

        for i in 1..100 {
            let backoff = strategy.backoff(i);
            let jitter = TimeDelta::seconds(10) * i as _;
            assert!(backoff >= delay * i as _ - jitter);
            assert!(backoff <= delay * i as _ + jitter);
        }
    }

    #[test]
    fn linear_backoff_with_jitter_min() {
        let delay = TimeDelta::seconds(20);
        let jitter = TimeDelta::seconds(20);
        let min = TimeDelta::seconds(5);
        let strategy = BackoffStrategy::linear(delay)
            .with_jitter(Jitter::Absolute(jitter))
            .with_min(min);

        for i in 1..100 {
            let backoff = strategy.backoff(i);
            assert!(backoff >= min);
            assert!(backoff <= delay * i as _ + jitter);
        }
    }

    #[test]
    fn exponential_backoff() {
        let delay = TimeDelta::seconds(1);
        let strategy = BackoffStrategy::exponential(delay);

        for i in 1..10 {
            assert_eq!(
                strategy.backoff(i).num_seconds(),
                delay.num_seconds().pow(i as _)
            );
        }
    }

    #[test]
    fn exponential_backoff_with_max() {
        let delay = TimeDelta::minutes(1);
        let max = TimeDelta::minutes(10);
        let strategy = BackoffStrategy::exponential(delay).with_max(max);

        for i in 1..100 {
            let backoff = strategy.backoff(i);
            assert!(backoff <= max);
        }
    }

    #[test]
    fn exponential_backoff_with_absolute_jitter() {
        let delay = TimeDelta::minutes(1);
        let jitter = TimeDelta::seconds(10);
        let strategy = BackoffStrategy::exponential(delay).with_jitter(Jitter::Absolute(jitter));

        for i in 1..5 {
            let backoff = strategy.backoff(i);
            assert!(
                backoff.num_seconds() >= delay.num_seconds().pow(i as _) - jitter.num_seconds()
            );
            assert!(
                backoff.num_seconds() <= delay.num_seconds().pow(i as _) + jitter.num_seconds()
            );
        }
    }

    #[test]
    fn exponential_backoff_with_relative_jitter() {
        let delay = TimeDelta::minutes(1);
        let strategy = BackoffStrategy::exponential(delay).with_jitter(Jitter::Relative(0.1));

        for i in 1..5 {
            let backoff = strategy.backoff(i);
            assert!(backoff.num_seconds() as f64 >= delay.num_seconds().pow(i as _) as f64 * 0.9);
            assert!(backoff.num_seconds() as f64 <= delay.num_seconds().pow(i as _) as f64 * 1.1);
        }
    }

    #[test]
    fn exponential_backoff_with_jitter_min() {
        let delay = TimeDelta::seconds(1);
        let jitter = TimeDelta::seconds(10);
        let min = TimeDelta::seconds(5);
        let strategy = BackoffStrategy::exponential(delay)
            .with_jitter(Jitter::Absolute(jitter))
            .with_min(min);

        for i in 1..5 {
            let backoff = strategy.backoff(i);
            assert!(backoff >= min);
        }
    }
}
