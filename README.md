# Rust Data Engineering Pipeline

## Project Overview

This project implements a scalable and efficient data engineering pipeline using
Rust. It demonstrates Rust's capabilities in data processing, ETL (Extract,
Transform, Load), and data analysis.

## Components

1. Data Ingestion (ETL)
2. Data Processing/Transformation
3. Storage & Visualization

## Features

- Data ingestion from CSV files
- Data cleaning and normalization
- Data validation
- Efficient data manipulation using Polars
- Data storage in PostgreSQL database
- Error handling using Result enum
- Unit testing
- Comprehensive documentation

## Prerequisites

- Rust (latest stable version)
- PostgreSQL
- Cargo (Rust's package manager)

## Setup

1. Clone the repository:

   ```bash
   git clone https://github.com/SrikanthKotturi/rust-final-project
   cd rust-final-project
   ```

2. Set up the PostgreSQL database and update the `DATABASE_URL` in a `.env`
   file:

   ```bash
   DATABASE_URL=postgres://username:password@localhost/database_name
   ```

3. Build the project:

```bash
cargo build
```

## Usage

### Run the main data pipeline

```bash
cargo run
```

### Generate Documentation

```bash
cargo doc --no-deps --document-private-items --open --target-dir ./docs
```

## Project Structure

- `src/main.rs`: Main entry point of the application
- `src/ingestion.rs`: Handles data ingestion from CSV files
- `src/transformation.rs`: Implements data cleaning, normalization, and
  validation
- `src/storage.rs`: Manages data storage in PostgreSQL
- `data/`: Directory for input CSV files

## Testing

Run the unit tests with:

```bash
cargo test
```
