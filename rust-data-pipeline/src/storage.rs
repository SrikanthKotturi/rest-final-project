//! This module handles the storage of data into a PostgreSQL database.
//!
//! It provides functions for creating a connection pool, storing data from DataFrames, and retrieving data from the database.

use anyhow::{Context, Result};
use chrono::NaiveDate;
use futures::future::try_join_all;
use polars::prelude::*;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::types::BigDecimal;
use sqlx::Row;
use std::str::FromStr;

/// Creates a connection pool to the PostgreSQL database.
///
/// # Returns
///
/// * `Result<PgPool>` - A result containing the PostgreSQL connection pool if successful, or an error if the connection setup fails.
///
/// # Example
///
/// ```
/// let pool = create_connection_pool().await.expect("Failed to create connection pool");
/// ```
pub async fn create_connection_pool() -> Result<PgPool> {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    Ok(pool)
}

/// Stores data from a DataFrame into the PostgreSQL database.
///
/// # Arguments
///
/// * `pool` - A reference to the PostgreSQL connection pool.
/// * `df` - A reference to the DataFrame containing the data to be stored.
///
/// # Returns
///
/// * `Result<()>` - A result indicating success or failure of the data storage operation.
///
/// # Example
///
/// ```
/// let df = DataFrame::new(vec![
///     Series::new("Age", &vec![30, 40]),
///     Series::new("Gender", &vec!["M", "F"]),
///     // other columns...
/// ]).unwrap();
///
/// store_data(&pool, &df).await.expect("Failed to store data");
/// ```
pub async fn store_data(pool: &PgPool, df: &DataFrame) -> Result<()> {
    let age_series = df.column("Age")?.i32()?;
    let gender_series = df.column("Gender")?.str()?;
    let blood_type_series = df.column("Blood Type")?.str()?;
    let medical_condition_series = df.column("Medical Condition")?.str()?;
    let billing_amount_series = df.column("Billing Amount")?.f64()?;
    let medication_series = df.column("Medication")?.str()?;
    let test_results_series = df.column("Test Results")?.str()?;
    let date_of_admission_series = df.column("Date of Admission")?.date()?;
    let admission_type_series = df.column("Admission Type")?.str()?;

    let mut tasks = vec![];

    for i in 0..df.height() {
        let age = age_series.get(i).context("Failed to get Age")?;
        let gender = gender_series
            .get(i)
            .context("Failed to get Gender")?
            .to_string();
        let blood_type = blood_type_series
            .get(i)
            .context("Failed to get Blood Type")?
            .to_string();
        let medical_condition = medical_condition_series
            .get(i)
            .context("Failed to get Medical Condition")?
            .to_string();
        let billing_amount = BigDecimal::from_str(
            &billing_amount_series
                .get(i)
                .context("Failed to get Billing Amount")?
                .to_string(),
        )
        .context("Failed to convert billing amount to BigDecimal")?;
        let medication = medication_series
            .get(i)
            .context("Failed to get Medication")?
            .to_string();
        let test_results = test_results_series
            .get(i)
            .context("Failed to get Test Results")?
            .to_string();
        let date_of_admission = NaiveDate::from_num_days_from_ce_opt(
            date_of_admission_series
                .get(i)
                .context("Failed to get Date of Admission")?,
        )
        .context("Failed to convert date of admission")?;
        let admission_type = admission_type_series
            .get(i)
            .context("Failed to get Admission Type")?
            .to_string();

        let pool = pool.clone();
        let task = tokio::spawn(async move {
            let result = sqlx::query!(
                r#"
                INSERT INTO patients (age, gender, blood_type, medical_condition, billing_amount, medication, test_results, date_of_admission, admission_type)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                "#,
                age,
                gender,
                blood_type,
                medical_condition,
                billing_amount,
                medication,
                test_results,
                date_of_admission,
                admission_type
            )
            .execute(&pool)
            .await;

            if let Err(e) = &result {
                eprintln!("Failed to insert row {}: {:?}", i, e);
            }

            result.context("Failed to insert data into the database")
        });

        tasks.push(task);
    }

    try_join_all(tasks).await?;
    Ok(())
}

/// Fetches and prints the first 5 rows from the patients table in the PostgreSQL database.
///
/// # Arguments
///
/// * `pool` - A reference to the PostgreSQL connection pool.
///
/// # Returns
///
/// * `Result<()>` - A result indicating success or failure of the data retrieval operation.
///
/// # Example
///
/// ```
/// get_first_5_rows(&pool).await.expect("Failed to fetch first 5 rows");
/// ```
pub async fn get_first_5_rows(pool: &PgPool) -> Result<()> {
    let rows = sqlx::query("SELECT * FROM patients LIMIT 5")
        .fetch_all(pool)
        .await
        .context("Failed to fetch rows from the database")?;

    for row in rows {
        let id: i32 = row.try_get("id")?;
        let age: i32 = row.try_get("age")?;
        let gender: String = row.try_get("gender")?;
        let blood_type: String = row.try_get("blood_type")?;
        let medical_condition: String = row.try_get("medical_condition")?;
        let billing_amount: BigDecimal = row.try_get("billing_amount")?;
        let medication: String = row.try_get("medication")?;
        let test_results: String = row.try_get("test_results")?;
        let date_of_admission: NaiveDate = row.try_get("date_of_admission")?;
        let admission_type: String = row.try_get("admission_type")?;

        println!(
            "ID: {}, Age: {}, Gender: {}, Blood Type: {}, Medical Condition: {}, Billing Amount: {}, Medication: {}, Test Results: {}, Date of Admission: {}, Admission Type: {}",
            id, age, gender, blood_type, medical_condition, billing_amount, medication, test_results, date_of_admission, admission_type
        );
    }

    Ok(())
}
