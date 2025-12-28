import os
import shutil
from datetime import datetime

import duckdb

# --- Configuration ---
# Get absolute paths to ensure scripts work regardless of where you run them from
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.join(BASE_DIR, "output", "compliance_report.parquet")
SHARED_DATA_DIR = os.path.join(BASE_DIR, "data")
DB_FILE = os.path.join(SHARED_DATA_DIR, "scytale.duckdb")


def run_load():
    # Validation
    if not os.path.exists(INPUT_FILE):
        print(f"Input file not found: {INPUT_FILE}")
        return

    # Ensure shared data directory exists
    os.makedirs(SHARED_DATA_DIR, exist_ok=True)

    # Archive Parquet ---
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest_path = os.path.join(SHARED_DATA_DIR, f"compliance_{timestamp}.parquet")

    try:
        shutil.copy(INPUT_FILE, dest_path)
        print(f"Archived Parquet to: {dest_path}")
    except IOError as e:
        print(f"Failed to archive Parquet: {e}")

    # Load into DuckDB ---
    print(f"Loading data into DuckDB: {DB_FILE}...")

    input_path_sql = INPUT_FILE.replace("\\", "/")

    try:
        con = duckdb.connect(DB_FILE)

        # Create the table
        create_query = f"CREATE OR REPLACE TABLE compliance_data AS SELECT * FROM '{input_path_sql}'"
        con.execute(create_query)

        # We check if the result is None before trying to access
        res = con.execute("SELECT COUNT(*) FROM compliance_data").fetchone()

        if res is None:
            print("Warning: Query returned no rows (unexpected for COUNT).")
        else:
            print(f"Successfully loaded {res[0]} rows into table 'compliance_data'.")

        # Show a sample
        print("\nSample Data:")
        # .df() converts result to a clean Pandas-style view for printing
        print(con.execute("SELECT * FROM compliance_data LIMIT 3").df())

        con.close()

    except Exception as e:
        print(f"Database Error: {e}")


if __name__ == "__main__":
    run_load()
