import json
import os
import logging
import polars as pl
from jsonschema import ValidationError, validate

# --- Configuration ---
# Use Airflow's standard logger
logger = logging.getLogger(__name__)

# Load schema relative to this script
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "input_schema.json")

# Fail early if schema is missing
if not os.path.exists(SCHEMA_PATH):
    raise FileNotFoundError(f"Schema file not found at {SCHEMA_PATH}")

with open(SCHEMA_PATH, "r", encoding="utf-8") as schema_file:
    INPUT_SCHEMA = json.load(schema_file)


def load_and_validate_file(file_path: str) -> list:
    """Reads a single JSON file, validates schema, and returns the list."""
    if not os.path.exists(file_path):
        logger.error(f"Input file not found: {file_path}")
        raise FileNotFoundError(f"File missing: {file_path}")

    logger.info(f"Validating and loading: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
            
            # Validates the entire JSON object against the schema
            validate(instance=data, schema=INPUT_SCHEMA)
            
            return data

        except ValidationError as e:
            logger.error(f"Schema Validation Failed for {file_path}: {e.message}")
            raise # Stop the pipeline if data is bad
        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")
            raise


def run_transform(input_path: str, output_dir: str = "data") -> str:
    """
    Transforms the specific input file and returns the path to the output Parquet file.
    """
    # Load Data 
    raw_data = load_and_validate_file(input_path)
    
    if not raw_data:
        raise ValueError("No data found in input file.")

    # Polars automatically infers nested structures (Lists/Structs)
    df = pl.DataFrame(raw_data)

    # Apply Compliance Logic
    transformed_df = df.select(
        [
            pl.col("number").alias("pr_number"),
            pl.col("title").alias("pr_title"),
            pl.col("user_login").alias("author"),
            pl.lit("home-assistant/core").alias("repository"),
            # Added time_zone="UTC" to handle the Z 
            pl.col("merged_at").str.to_datetime(time_zone="UTC"),
            # "Approved review
            pl.col("reviews")
            .list.eval(pl.element().struct.field("state") == "APPROVED")
            .list.any()
            .fill_null(False)
            .alias("code_review_passed"),
            # "Required checks successful"
            pl.col("status_checks")
            .list.eval(pl.element().struct.field("conclusion") == "success")
            .list.all()
            .fill_null(True)
            .alias("status_checks_passed"),
        ]
    ).with_columns(
        # "both review and checks passed"
        (pl.col("code_review_passed") & pl.col("status_checks_passed")).alias("is_compliant")
    )

    # Summary Statistics
    total_prs = transformed_df.height
    compliant_prs = transformed_df.filter(pl.col("is_compliant")).height
    compliance_rate = (compliant_prs / total_prs * 100) if total_prs > 0 else 0.0

    logger.info("=" * 30)
    logger.info(" SUMMARY STATISTICS")
    logger.info("=" * 30)
    logger.info(f"Total PRs:          {total_prs}")
    logger.info(f"Compliant PRs:      {compliant_prs}")
    logger.info(f"Compliance Rate:    {compliance_rate:.2f}%")
    logger.info("-" * 30)

    # Violations by Repository
    violations = (
        transformed_df.filter(~pl.col("is_compliant"))
        .group_by("repository")
        .agg(pl.len().alias("violation_count"))
    )
    logger.info(f"Violations:\n{violations}")

    # Save Output
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Generate a specific filename based on the input
    base_name = os.path.basename(input_path).replace(".json", "")
    parquet_filename = f"transformed_{base_name}.parquet"
    parquet_path = os.path.join(output_dir, parquet_filename)
    
    transformed_df.write_parquet(parquet_path)
    logger.info(f"Transformed data saved to: {parquet_path}")

    # Optional - Save as CSV
    csv_path = os.path.join(output_dir, f"transformed_{base_name}.csv")
    transformed_df.write_csv(csv_path)

    # RETURN the path so the DAG can pass it to the 'Load' task
    return parquet_path


if __name__ == "__main__":
    # Local tests
    import sys
    if len(sys.argv) > 1:
        run_transform(sys.argv[1])
    else:
        print("Usage: python transform.py path_to_json_file")