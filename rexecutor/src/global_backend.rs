//! The global backend.
//!
//! Generally this will not be used directly. However, there could be some use cases for getting a
//! reference to the global backend via [`GlobalBackend::as_ref`]. However, it is generally
//! encouraged to use the provided APIs in the library.
//!
//! Although [`GlobalBackend::as_ref`] is public, the [`GlobalBackend`] APIs should be considered
//! unstable.
//!
//! Setting the global backed should be done via [`crate::Rexecutor::set_global_backend`].
use std::sync::Arc;
use tokio::sync::OnceCell;

use crate::{backend::Backend, RexecutorError};

/// The global backend.
///
/// If this has been set using [`crate::Rexecutor::set_global_backend`], then it is possible to get
/// a reference to the global backend via [`GlobalBackend::as_ref`].
pub struct GlobalBackend;

static GLOBAL_BACKEND: OnceCell<Arc<dyn Backend + 'static + Sync + Send>> = OnceCell::const_new();

impl GlobalBackend {
    /// Sets the global backend to the backend associated with the current instance of
    ///
    /// This should only be called once. If called a second time it will return
    /// [`RexecutorError::GlobalBackend`].
    ///
    /// Calling this makes is possible to enqueue jobs without maintaining a reference to the
    /// backend throughout the codebase and enables the use of
    /// [`job::builder::JobBuilder::enqueue`].
    pub(crate) fn set(backend: impl Backend + Send + Sync + 'static) -> Result<(), RexecutorError> {
        GLOBAL_BACKEND.set(Arc::new(backend)).map_err(|err| {
            tracing::error!(%err, "Couldn't set global backend {err}");
            RexecutorError::GlobalBackend
        })?;
        Ok(())
    }

    /// Get a reference to the global backend.
    ///
    /// Example
    ///
    /// Calling [`GlobalBackend::as_ref`] before setting the backend returns an error
    ///
    /// ```
    /// use rexecutor::global_backend::GlobalBackend;
    /// use rexecutor::RexecutorError;
    ///
    /// let result = GlobalBackend::as_ref();
    ///
    /// assert!(matches!(result, Err(RexecutorError::GlobalBackend)));
    /// ```
    #[doc(hidden)]
    pub fn as_ref() -> Result<&'static (dyn Backend + Send + Sync), RexecutorError> {
        Ok(GLOBAL_BACKEND
            .get()
            .ok_or(RexecutorError::GlobalBackend)?
            .as_ref())
    }
}
