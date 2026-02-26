//! Core types

use polars::prelude::*;

/// DataFrame wrapper around Polars DataFrame
#[derive(Debug, Clone)]
pub struct DataFrame {
    inner: polars::frame::DataFrame,
}

impl DataFrame {
    /// Create a new DataFrame
    pub fn new(df: polars::frame::DataFrame) -> Self {
        Self { inner: df }
    }

    /// Get reference to inner Polars DataFrame
    pub fn inner(&self) -> &polars::frame::DataFrame {
        &self.inner
    }

    /// Consume and return inner Polars DataFrame
    pub fn into_inner(self) -> polars::frame::DataFrame {
        self.inner
    }
}

impl From<polars::frame::DataFrame> for DataFrame {
    fn from(df: polars::frame::DataFrame) -> Self {
        Self::new(df)
    }
}

impl std::fmt::Display for DataFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}
