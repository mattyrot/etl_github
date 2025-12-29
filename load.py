import os
import shutil
import logging
from datetime import datetime
import duckdb

# --- Configuration ---
# Use Airflow logger to see output in Airflow UI
logger = logging.getLogger(__name__)

# Base paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SHARED_DATA_DIR = os.path.join(BASE_DIR, "data")
DB_FILE = os.path.join(SHARED_DATA_DIR, "scytale.duckdb")

def run_load(input_file: str):
    """
    Loads the Parquet file into DuckDB and archives it.
    """
    # Validation
    if not input_file or not os.path.exists(input_file):
        logger.error(f"Input file not found: {input_file}")
        return

    # Ensure shared data directory exists
    os.makedirs(SHARED_DATA_DIR, exist_ok=True)

    # Archive Parquet ---
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest_path = os.path.join(SHARED_DATA_DIR, f"compliance_{timestamp}.parquet")

    try:
        shutil.copy(input_file, dest_path)
        logger.info(f"Archived Parquet to: {dest_path}")
    except IOError as e:
        logger.error(f"Failed to archive Parquet: {e}")

    # Load into DuckDB ---
    logger.info(f"Loading data into DuckDB: {DB_FILE}...")

    # Fix path for SQL (DuckDB prefers forward slashes even on Windows/WSL)
    input_path_sql = input_file.replace("\\", "/")

    try:
        con = duckdb.connect(DB_FILE)

        # Create the table
        create_query = f"CREATE OR REPLACE TABLE compliance_data AS SELECT * FROM '{input_path_sql}'"
        con.execute(create_query)

        # We check if the result is None before trying to access
        res = con.execute("SELECT COUNT(*) FROM compliance_data").fetchone()

        if res is None:
            logger.warning("Query returned no rows (unexpected for COUNT).")
        else:
            logger.info(f"Successfully loaded {res[0]} rows into table 'compliance_data'.")

        # Show a sample
        logger.info("\nSample Data:")
        # .pl() converts result to a clean Polars/Pandas-style view if available
        # If running in a slim container without Polars, this might need .fetchall()
        try:
            logger.info(con.execute("SELECT * FROM compliance_data LIMIT 10").pl())
        except Exception:
             logger.info(con.execute("SELECT * FROM compliance_data LIMIT 10").fetchall())

        con.close()

    except Exception as e:
        logger.error(f"Database Error: {e}")
        raise # Raise so Airflow marks the task as failed

if __name__ == "__main__":
    # Fallback for manual running (defaults to expected location)
    default_input = os.path.join(BASE_DIR, "output", "compliance_report.parquet")
    run_load(default_input)