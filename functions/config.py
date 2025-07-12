from pathlib import Path

# Root of the project on the local filesystem
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Map short ``job_type`` names to ingest function combinations.
JOB_TYPE_MAP = {
    "bronze_standard_streaming": {
        "read_function": "functions.read.stream_read_cloudfiles",
        "transform_function": "functions.transform.bronze_standard_transform",
        "write_function": "functions.write.stream_write_table",
    },
    "silver_scd2_streaming": {
        "read_function": "functions.read.stream_read_table",
        "transform_function": "functions.transform.silver_scd2_transform",
        "write_function": "functions.write.stream_upsert_table",
        "upsert_function": "functions.write.microbatch_upsert_scd2_fn",
    },
    "silver_upsert_streaming": {
        "read_function": "functions.read.stream_read_table",
        "transform_function": "functions.transform.silver_standard_transform",
        "write_function": "functions.write.stream_upsert_table",
        "upsert_function": "functions.write.microbatch_upsert_fn",
    },
    "silver_standard_streaming": {
        "read_function": "functions.read.stream_read_table",
        "transform_function": "functions.transform.silver_standard_transform",
        "write_function": "functions.write.stream_write_table",
    },
    "silver_scd2_batch": {
        "read_function": "functions.read.read_table",
        "transform_function": "functions.transform.silver_scd2_transform",
        "write_function": "functions.write.batch_upsert_scd2",
    },
    "silver_standard_batch": {
        "read_function": "functions.read.read_table",
        "transform_function": "functions.transform.silver_standard_transform",
        "write_function": "functions.write.write_upsert_snapshot",
    },
}

# Base S3 paths for external volumes
S3_ROOT_LANDING = "s3://edsm/landing/"
S3_ROOT_UTILITY = "s3://edsm/utility/"

# Allowed Databricks workspace host names
ALLOWED_HOST_NAMES = {"dbc-bde2b6e3-4903", "dev", "staging", "prod"}

