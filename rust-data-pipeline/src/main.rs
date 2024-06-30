//! This is the main module that drives the entire data pipeline application.
//!
//! It coordinates the ingestion, transformation, and storage of data.

use anyhow::Result;
use dotenv::dotenv;

mod ingestion;
mod seed;
mod storage;
mod transformation;

/// The main entry point for the data pipeline application.
///
/// # Returns
///
/// * `Result<()>` - A result indicating success or failure of the data pipeline execution.
///
/// # Example
///
/// ```
/// main().await.expect("Data pipeline execution failed");
/// ```
#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables from .env file
    dotenv().ok();

    // Uncomment to run database setup (run once, then comment out)
    seed::run_db_setup().await?;

    println!("Starting data pipeline...");

    // Ingest data
    let df = ingestion::retry_ingest("data/dataset.csv", 3)?;
    println!("Data ingestion complete. DataFrame shape: {:?}", df.shape());
    println!("DataFrame: {:?}", df);

    // Transform data
    let transformed_df = transformation::transform_data(df)?;
    println!("DataFrame: {:?}", transformed_df);
    println!(
        "Data transformation complete. Transformed DataFrame shape: {:?}",
        transformed_df.shape()
    );
    println!("DataFrame dtypes: {:?}", transformed_df.dtypes());

    // Store data
    let pool = storage::create_connection_pool().await?;
    storage::store_data(&pool, &transformed_df).await?;
    println!("Data storage complete.");

    // Retrieve and print first 5 rows
    storage::get_first_5_rows(&pool).await?;
    println!("Data retrieved and printed successfully.");

    println!("Data pipeline finished successfully.");

    Ok(())
}
