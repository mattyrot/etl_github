
# Scytale GitHub ETL

## Overview

This project is an Airflow-based ETL pipeline that monitors GitHub pull requests for compliance.
It consists of three main stages:
- Extract: Asynchronously calls the GitHub REST API to fetch recent PRs from a target repository, including
  reviews, status checks, and commit metadata, and stores the enriched data as JSON in the data directory.
- Transform: Validates the JSON against an input JSON Schema, then uses Polars to compute compliance flags
  (code review passed, status checks passed, overall compliance) and writes the result to Parquet/CSV files.
- Load: Loads the transformed Parquet data into a DuckDB database (scytale.duckdb), appending to a
  compliance_data table and archiving the Parquet files in the shared data directory.

## Key libraries

- Airflow: Runs the extract, transform, and load steps as a DAG.
- httpx: Talks to the GitHub API using async HTTP requests (chosen over requests for built-in async support
	and better performance with many calls).
- Polars: Rust-based DataFrame library that applies the compliance rules (chosen over pandas for faster
	performance on larger datasets and a more SQL-like expression API).
- DuckDB: Stores the final compliance data for querying.
- uv: Installs Python dependencies inside the Docker image (chosen over pip for faster, reproducible
	dependency resolution).

## Build & run

Prerequisites
- Docker installed.
- A GitHub token with access to this repository.

### Quick start

- Build the Docker image (replace the token value):

```bash
docker build \
  --build-arg AIRFLOW_VAR_GITHUB_TOKEN=your_token_here \
  -t scytale-airflow-fixed .
```

- Start Airflow with docker compose (from the project root):

```bash
docker compose up -d
```

The token can also be set later using the Airflow UI at:
- http://localhost:8080/variable/list/

Access the UI
- Open http://localhost:8080 in your browser.
- Default Airflow credentials created by docker-compose:
	- Username: scytale
	- Password: scytale

