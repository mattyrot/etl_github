import polars as pl
import json
import glob
import os
from jsonschema import validate, ValidationError

# --- Configuration ---
INPUT_DIR = "data"
OUTPUT_DIR = "output"
INPUT_PATTERN = os.path.join(INPUT_DIR, "prs_extract_*.json") 

# --- 1. Predefine Input Schema (JSON Schema) ---
INPUT_SCHEMA = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "number": {"type": "integer"},
            "title": {"type": "string"},
            "user_login": {"type": "string"},
            "merged_at": {"type": ["string", "null"]},
            "reviews": {
                "type": "array", 
                "items": {
                    "type": "object", 
                    "properties": {"state": {"type": "string"}}
                }
            },
            "status_checks": {
                "type": "array",
                "items": {
                    "type": "object", 
                    "properties": {"conclusion": {"type": "string"}}
                }
            }
        },
        "required": ["number", "title", "user_login"]
    }
}

def load_and_validate_data():
    """Reads JSON files, validates schema, and returns a raw list."""
    json_files = glob.glob(INPUT_PATTERN)
    if not json_files:
        print("No input files found!")
        return []

    merged_data = []
    print(f"Found {len(json_files)} files. Validating and loading...")

    for file_path in json_files:
        with open(file_path, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
                validate(instance=data, schema=INPUT_SCHEMA)
                merged_data.extend(data)
            except ValidationError as e:
                print(f"❌ Schema Validation Failed for {file_path}: {e.message}")
            except Exception as e:
                print(f"❌ Error reading {file_path}: {e}")
                
    return merged_data

def run_transform():
    # 1. Load Data
    raw_data = load_and_validate_data()
    if not raw_data:
        return

    # Polars automatically infers nested structures (Lists/Structs)
    df = pl.DataFrame(raw_data)

    # 2. Apply Compliance Logic
    transformed_df = df.select([
        pl.col("number").alias("pr_number"),
        pl.col("title").alias("pr_title"),
        pl.col("user_login").alias("author"),
        pl.lit("home-assistant/core").alias("repository"),

        # --- FIX IS HERE ---
        # Added time_zone="UTC" to handle the 'Z' suffix in your data
        pl.col("merged_at").str.to_datetime(time_zone="UTC"),

        # "at least one approved review"
        pl.col("reviews").list.eval(
            pl.element().struct.field("state") == "APPROVED"
        ).list.any().fill_null(False).alias("code_review_passed"),

        # "all required checks successful"
        pl.col("status_checks").list.eval(
            pl.element().struct.field("conclusion") == "success"
        ).list.all().fill_null(True).alias("status_checks_passed")
    ]).with_columns(
        # "both review and checks passed"
        (pl.col("code_review_passed") & pl.col("status_checks_passed")).alias("is_compliant")
    )

    # 3. Summary Statistics
    total_prs = transformed_df.height
    compliant_prs = transformed_df.filter(pl.col("is_compliant")).height
    compliance_rate = (compliant_prs / total_prs * 100) if total_prs > 0 else 0.0

    print("\n" + "="*30)
    print(" SUMMARY STATISTICS")
    print("="*30)
    print(f"Total PRs:           {total_prs}")
    print(f"Compliant PRs:       {compliant_prs}")
    print(f"Compliance Rate:     {compliance_rate:.2f}%")
    print("-" * 30)
    
    # Violations by Repository
    print("Violations by Repository:")
    violations = (
        transformed_df.filter(~pl.col("is_compliant"))
        .group_by("repository")
        .agg(pl.len().alias("violation_count"))
    )
    print(violations)

    # 4. Save Output
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        
    parquet_path = os.path.join(OUTPUT_DIR, "compliance_report.parquet")
    transformed_df.write_parquet(parquet_path)
    print(f"\n Transformed data saved to: {parquet_path}")
    
    # Optional: Save as CSV
    transformed_df.write_csv(os.path.join(OUTPUT_DIR, "compliance_report.csv"))

if __name__ == "__main__":
    run_transform()