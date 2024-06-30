//! This module handles the transformation of data within DataFrames.
//!
//! It provides functions for cleaning, normalizing, and validating data.

use anyhow::Result;
use polars::prelude::*;

/// Transforms the input DataFrame by cleaning, normalizing, and validating the data.
///
/// # Arguments
///
/// * `df` - A DataFrame containing the data to be transformed.
///
/// # Returns
///
/// * `Result<DataFrame>` - A result containing the transformed DataFrame if successful, or an error if the transformation fails.
///
/// # Example
///
/// ```
/// let df = DataFrame::new(vec![
///     Series::new("Name", &["Alice", "Bob"]),
///     Series::new("Gender", &["F", "M"]),
///     // other columns...
/// ]).unwrap();
///
/// let transformed_df = transform_data(df).expect("Data transformation failed");
/// ```
pub fn transform_data(df: DataFrame) -> Result<DataFrame> {
    // let df = df.head(Some(1000));
    let df = clean_data(df)?;
    let df = normalize_data(df)?;
    let df = validate_data(df)?;
    Ok(df)
}

/// Cleans the data by converting specified columns to lowercase.
///
/// # Arguments
///
/// * `df` - A DataFrame containing the data to be cleaned.
///
/// # Returns
///
/// * `Result<DataFrame>` - A result containing the cleaned DataFrame if successful, or an error if the cleaning fails.
fn clean_data(df: DataFrame) -> Result<DataFrame> {
    let df = df
        .lazy()
        .with_column(col("Name").str().to_lowercase().alias("Name"))
        .with_column(col("Gender").str().to_lowercase().alias("Gender"))
        .collect()?;
    Ok(df)
}

/// Normalizes the data by filtering, dropping nulls, and processing specific columns.
///
/// # Arguments
///
/// * `df` - A DataFrame containing the data to be normalized.
///
/// # Returns
///
/// * `Result<DataFrame>` - A result containing the normalized DataFrame if successful, or an error if the normalization fails.
fn normalize_data(df: DataFrame) -> Result<DataFrame> {
    let df = df
        .lazy()
        .filter(col("Age").gt_eq(lit(0)).and(col("Age").lt_eq(lit(120))))
        .drop_nulls(None)
        .select(&[
            col("Age"),
            col("Gender"),
            col("Blood Type"),
            col("Medical Condition"),
            col("Billing Amount"),
            col("Medication"),
            col("Test Results"),
            col("Date of Admission"),
            col("Admission Type"),
        ])
        .with_column((col("Billing Amount") / lit(1000)).alias("Billing Amount"))
        .with_column(
            col("Date of Admission")
                .str()
                .strptime(
                    DataType::Date,
                    StrptimeOptions {
                        format: Some("%Y-%m-%d".into()),
                        ..Default::default()
                    },
                    col("Date of Admission"),
                )
                .alias("Date of Admission"),
        )
        .with_column(col("Age").cast(DataType::Int32).alias("Age"))
        .collect()?;
    Ok(df)
}

/// Validates the data by ensuring all dates in the "Date of Admission" column are valid.
///
/// # Arguments
///
/// * `df` - A DataFrame containing the data to be validated.
///
/// # Returns
///
/// * `Result<DataFrame>` - A result containing the validated DataFrame if successful, or an error if the validation fails.
fn validate_data(df: DataFrame) -> Result<DataFrame> {
    let valid_dates = df
        .column("Date of Admission")?
        .date()?
        .into_iter()
        .map(|date| date.is_some())
        .collect::<BooleanChunked>();

    let df = df.filter(&valid_dates)?;
    Ok(df)
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::df;

    fn create_test_dataframe() -> DataFrame {
        df!(
            "Name" => &["Alice", "BOB", "Charlie"],
            "Gender" => &["F", "M", "M"],
            "Age" => &[25, 30, 150],
            "Blood Type" => &["A+", "B-", "O+"],
            "Medical Condition" => &["Healthy", "Flu", "Headache"],
            "Billing Amount" => &[1000.0, 2000.0, 3000.0],
            "Medication" => &["None", "Aspirin", "Ibuprofen"],
            "Test Results" => &["Normal", "Elevated", "Normal"],
            "Date of Admission" => &["2023-01-01", "2023-02-15", "2023-03-30"],
            "Admission Type" => &["Emergency", "Scheduled", "Walk-in"]
        )
        .unwrap()
    }

    #[test]
    fn test_transform_data() {
        let df = create_test_dataframe();
        let result = transform_data(df);
        assert!(result.is_ok());

        let transformed_df = result.unwrap();
        assert_eq!(transformed_df.height(), 2); // Only 2 rows should remain after filtering
        assert_eq!(transformed_df.width(), 9); // 9 columns after normalization
    }

    #[test]
    fn test_clean_data() {
        let df = df!(
            "Name" => &["Alice", "BOB", "Charlie"],
            "Gender" => &["F", "M", "M"]
        )
        .unwrap();
        let cleaned_df = clean_data(df).unwrap();

        let name_col = cleaned_df.column("Name").unwrap().str().unwrap();
        assert_eq!(name_col.get(0), Some("alice"));
        assert_eq!(name_col.get(1), Some("bob"));
        assert_eq!(name_col.get(2), Some("charlie"));

        let gender_col = cleaned_df.column("Gender").unwrap().str().unwrap();
        assert_eq!(gender_col.get(0), Some("f"));
        assert_eq!(gender_col.get(1), Some("m"));
        assert_eq!(gender_col.get(2), Some("m"));
    }

    #[test]
    fn test_normalize_data() {
        let df = create_test_dataframe();
        let normalized_df = normalize_data(df).unwrap();

        assert_eq!(normalized_df.height(), 2); // 150 age should be filtered out
        assert_eq!(normalized_df.width(), 9);

        let age_col = normalized_df.column("Age").unwrap();
        assert!(age_col.dtype() == &DataType::Int32);

        let billing_col = normalized_df.column("Billing Amount").unwrap();
        let billing_values: Vec<f64> = billing_col.f64().unwrap().into_no_null_iter().collect();
        assert_eq!(billing_values, vec![1.0, 2.0]);

        let date_col = normalized_df.column("Date of Admission").unwrap();
        assert!(date_col.dtype() == &DataType::Date);
    }
}
