//! This module handles the ingestion of CSV data files into DataFrames.
//!
//! It provides functions for reading CSV files and retrying the ingestion process.

use anyhow::{Context, Result};
use polars::prelude::*;

/// Ingests a CSV file and returns a DataFrame.
///
/// # Arguments
///
/// * `file_path` - A string slice that holds the path to the CSV file.
///
/// # Returns
///
/// * `Result<DataFrame>` - A result containing the DataFrame if successful, or an error if the ingestion fails.
///
/// # Example
///
/// ```
/// let df = ingest_csv("data.csv").expect("CSV ingestion failed");
/// ```
pub fn ingest_csv(file_path: &str) -> Result<DataFrame> {
    println!("Starting data ingestion from CSV file: {}", file_path);

    let df = CsvReadOptions::default()
        .with_has_header(true)
        .try_into_reader_with_file_path(Some(file_path.into()))?
        .finish()
        .context("Failed to read CSV file")?;

    println!("Successfully ingested {} rows", df.height());
    println!("Columns: {:?}", df.get_column_names());

    Ok(df)
}

/// Retries the ingestion of a CSV file up to a specified number of attempts.
///
/// # Arguments
///
/// * `file_path` - A string slice that holds the path to the CSV file.
/// * `max_attempts` - The maximum number of attempts to retry ingestion.
///
/// # Returns
///
/// * `Result<DataFrame>` - A result containing the DataFrame if successful, or an error if the maximum attempts are reached.
///
/// # Example
///
/// ```
/// let df = retry_ingest("data.csv", 3).expect("CSV ingestion failed after 3 attempts");
/// ```
pub fn retry_ingest(file_path: &str, max_attempts: usize) -> Result<DataFrame> {
    let mut attempts = 0;
    loop {
        match ingest_csv(file_path) {
            Ok(df) => return Ok(df),
            Err(e) => {
                attempts += 1;
                if attempts >= max_attempts {
                    return Err(e).context("Max retry attempts reached");
                }
                println!("Attempt {} failed, retrying...", attempts);
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;

    fn create_temp_csv(content: &str) -> String {
        let file_path = "temp_test.csv";
        let mut file = File::create(file_path).unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file_path.to_string()
    }

    #[test]
    fn test_ingest_csv() {
        let csv_content = "name,age\nAlice,30\nBob,25";
        let file_path = create_temp_csv(csv_content);

        let df = ingest_csv(&file_path).expect("CSV ingestion failed");

        assert_eq!(df.shape(), (2, 2)); // 2 rows, 2 columns
        assert_eq!(
            df.column("name").unwrap().str().unwrap().get(0),
            Some("Alice")
        );
        assert_eq!(df.column("age").unwrap().i64().unwrap().get(1), Some(25));
    }

    #[test]
    fn test_retry_ingest() {
        let csv_content = "name,age\nAlice,30\nBob,25";
        let file_path = create_temp_csv(csv_content);

        let df = retry_ingest(&file_path, 3).expect("CSV ingestion failed after 3 attempts");

        assert_eq!(df.shape(), (2, 2)); // 2 rows, 2 columns
        assert_eq!(
            df.column("name").unwrap().str().unwrap().get(0),
            Some("Alice")
        );
        assert_eq!(df.column("age").unwrap().i64().unwrap().get(1), Some(25));
    }

    #[test]
    fn test_retry_ingest_fail() {
        let file_path = "non_existent_file.csv";

        let result = retry_ingest(&file_path, 3);
        assert!(result.is_err());
    }
}
