//! Core types

/// DataFrame wrapper around Polars DataFrame
#[derive(Debug, Clone)]
pub struct DataFrame(pub(crate) polars::frame::DataFrame);

impl DataFrame {
    /// Create a new DataFrame
    pub fn new(df: polars::frame::DataFrame) -> Self {
        Self(df)
    }

    /// Get reference to inner Polars DataFrame
    pub fn inner(&self) -> &polars::frame::DataFrame {
        &self.0
    }

    /// Consume and return inner Polars DataFrame
    pub fn into_inner(self) -> polars::frame::DataFrame {
        self.0
    }
}

impl From<polars::frame::DataFrame> for DataFrame {
    fn from(df: polars::frame::DataFrame) -> Self {
        Self::new(df)
    }
}

impl std::fmt::Display for DataFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // Import only the df! macro — avoid wildcard that pulls in polars::DataFrame
    // and creates an ambiguity with the wrapper DataFrame defined in super.
    use polars::prelude::df;

    #[test]
    fn test_dataframe_new() {
        let polars_df = df! {
            "id" => &[1, 2, 3],
            "name" => &["A", "B", "C"],
        }
        .unwrap();

        let df = DataFrame::new(polars_df.clone());
        assert_eq!(df.inner().height(), 3);
        assert_eq!(df.inner().width(), 2);
    }

    #[test]
    fn test_dataframe_from() {
        let polars_df = df! {
            "x" => &[1, 2, 3],
        }
        .unwrap();

        let df: DataFrame = polars_df.into();
        assert_eq!(df.inner().height(), 3);
    }

    #[test]
    fn test_dataframe_inner() {
        let polars_df = df! {
            "col" => &[1, 2],
        }
        .unwrap();

        let df = DataFrame::new(polars_df.clone());
        let inner = df.inner();
        assert_eq!(inner.height(), 2);
    }

    #[test]
    fn test_dataframe_into_inner() {
        let polars_df = df! {
            "col" => &[1, 2],
        }
        .unwrap();

        let df = DataFrame::new(polars_df);
        let inner = df.into_inner();
        assert_eq!(inner.height(), 2);
    }

    #[test]
    fn test_dataframe_display() {
        let polars_df = df! {
            "id" => &[1],
        }
        .unwrap();

        let df = DataFrame::new(polars_df);
        let display_str = format!("{}", df);
        assert!(display_str.contains("id"));
    }

    #[test]
    fn test_dataframe_clone() {
        let polars_df = df! {
            "data" => &[1, 2, 3],
        }
        .unwrap();

        let df1 = DataFrame::new(polars_df);
        let df2 = df1.clone();
        assert_eq!(df1.inner().height(), df2.inner().height());
    }

    #[test]
    fn test_empty_dataframe() {
        let polars_df = polars::frame::DataFrame::default();
        let df = DataFrame::new(polars_df);
        assert_eq!(df.inner().height(), 0);
        assert_eq!(df.inner().width(), 0);
    }
}
