//! This module handles the initial setup of the database.
//!
//! It provides a function to create the necessary tables and schema in the database.

use crate::storage;
use anyhow::Result;

/// Sets up the database by creating the connection pool and initializing the `patients` table.
///
/// # Returns
///
/// * `Result<()>` - A result indicating success or failure of the database setup.
///
/// # Example
///
/// ```
/// run_db_setup().await.expect("Failed to set up the database");
/// ```
pub async fn run_db_setup() -> Result<()> {
    dotenv::dotenv().ok();
    let pool = storage::create_connection_pool().await?;

    // Drop the table if it exists
    let drop_table_sql = "DROP TABLE IF EXISTS patients CASCADE;";
    sqlx::query(drop_table_sql).execute(&pool).await?;

    // Create the table
    let create_table_sql = r#"
    CREATE TABLE IF NOT EXISTS patients (
        id SERIAL PRIMARY KEY,
        age INTEGER NOT NULL,
        gender VARCHAR(10) NOT NULL,
        blood_type VARCHAR(5) NOT NULL,
        medical_condition VARCHAR(100) NOT NULL,
        date_of_admission DATE NOT NULL,
        billing_amount DECIMAL(10, 2) NOT NULL,
        medication VARCHAR(100) NOT NULL,
        test_results VARCHAR(20) NOT NULL,
        admission_type VARCHAR(20) NOT NULL
    );
    "#;
    sqlx::query(create_table_sql).execute(&pool).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use sqlx::{Pool, Postgres};

    async fn create_temp_table(pool: &Pool<Postgres>) -> Result<()> {
        let create_temp_table_sql = r#"
        CREATE TEMP TABLE temp_patients (
            id SERIAL PRIMARY KEY,
            age INTEGER NOT NULL,
            gender VARCHAR(10) NOT NULL,
            blood_type VARCHAR(5) NOT NULL,
            medical_condition VARCHAR(100) NOT NULL,
            date_of_admission DATE NOT NULL,
            billing_amount DECIMAL(10, 2) NOT NULL,
            medication VARCHAR(100) NOT NULL,
            test_results VARCHAR(20) NOT NULL,
            admission_type VARCHAR(20) NOT NULL
        );
        "#;
        sqlx::query(create_temp_table_sql).execute(pool).await?;
        Ok(())
    }

    async fn drop_temp_table(pool: &Pool<Postgres>) -> Result<()> {
        let drop_temp_table_sql = "DROP TABLE IF EXISTS temp_patients;";
        sqlx::query(drop_temp_table_sql).execute(pool).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_run_db_setup() -> Result<()> {
        dotenv::dotenv().ok();
        let pool = storage::create_connection_pool().await?;

        // Create a temporary table for testing
        create_temp_table(&pool).await?;

        // Run the database setup function
        run_db_setup().await?;

        // Check if the table was created
        let table_exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'patients');",
        )
        .fetch_one(&pool)
        .await?;

        assert!(table_exists);

        // Clean up the temporary table
        drop_temp_table(&pool).await?;

        Ok(())
    }
}
