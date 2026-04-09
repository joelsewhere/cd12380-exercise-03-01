# # dags/sales_etl_solution.py
# #
# # Airflow 3.1 Exercise — AWS ETL Pipeline (SOLUTION)
# #
# # Scenario
# # --------
# # Every night the sales platform drops a JSON file of the day's transactions
# # into S3 under a landing prefix. This DAG must:
# #
# #   1. Extract   — move the file from landing to raw, validating it is
# #                  readable JSON in the process
# #   2. Transform — read the raw JSON, clean and reshape the records, then
# #                  write the result back to S3 as CSV under a processed prefix
# #   3. Load      — use S3ToRedshiftOperator to upsert the processed CSV
# #                  into the Redshift production table
# #
# # Using S3 as the intermediate store between every stage is the standard
# # production pattern for Redshift pipelines. It avoids hitting Redshift
# # row-by-row INSERT limits and makes each stage independently replayable —
# # if the load fails, the processed CSV is already in S3 and transform does
# # not need to re-run. If transform fails, the raw JSON is already in S3 and
# # extract does not need to re-run.
# #
# # S3 layout:
# #   landing/transactions/{date}/orders.json    ← written by upstream system
# #   raw/transactions/{date}/orders.json        ← written by extract task
# #   processed/transactions/{date}/orders.csv   ← written by transform task
# #
# # Redshift table schema:
# #
# #   sales.daily_transactions
# #   ┌──────────────┬─────────┬──────────┬────────────┬───────────────┐
# #   │ order_id     │ sku     │ quantity │ unit_price │ total_revenue │
# #   │ VARCHAR PK   │ VARCHAR │ INTEGER  │ NUMERIC    │ NUMERIC       │
# #   └──────────────┴─────────┴──────────┴────────────┴───────────────┘
# #
# # Read README.md before editing this file.

# from __future__ import annotations

# import json
# from datetime import datetime

# from airflow.sdk import DAG, task
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator


# # ---------------------------------------------------------------------------
# # Constants (do not modify)
# # ---------------------------------------------------------------------------

# S3_CONN_ID       = "aws_default"
# REDSHIFT_CONN_ID = "redshift_default"
# S3_BUCKET        = "my-sales-bucket"
# REDSHIFT_SCHEMA  = "sales"
# REDSHIFT_TABLE   = "daily_transactions"
# S3_KEYS = {
#     'landing':   "landing/transactions/{{ ds }}/orders.json",
#     'extract':   "raw/transactions/{{ ds }}/orders.json",
#     'transform': "processed/transactions/{{ ds }}/orders.csv",
# }


# # ---------------------------------------------------------------------------
# # DAG definition
# # ---------------------------------------------------------------------------

# with DAG(
#     dag_id="sales_etl_solution",
#     schedule="@daily",
#     start_date=datetime(2026, 1, 1),
#     end_date=datetime(2026, 1, 7),
#     catchup=False,
# ):

#     # -----------------------------------------------------------------------
#     # TASK 1 — extract
#     #
#     # Move the raw JSON file from the landing prefix to the raw prefix.
#     # The landing prefix is written by the upstream system and should be
#     # treated as immutable — extract copies the file to raw so that
#     # downstream stages have a stable, pipeline-owned copy to work from.
#     #
#     # Steps:
#     #   - Instantiate S3Hook(aws_conn_id=S3_CONN_ID)
#     #   - Read the file from source_s3_key with hook.read_key()
#     #   - Validate the content is parseable JSON with json.loads()
#     #   - Write the content to dest_s3_key with hook.load_string()
#     #
#     # Returning nothing — transform reads directly from S3 so no XCom
#     # payload is needed between these two tasks.
#     # -----------------------------------------------------------------------

#     @task
#     def extract(source_s3_key, dest_s3_key):
#         hook = S3Hook(aws_conn_id=S3_CONN_ID)

#         content = hook.read_key(key=source_s3_key, bucket_name=S3_BUCKET)

#         # Validate the file is parseable before promoting it to raw
#         records = json.loads(content)
#         print(f"Validated {len(records)} records from {source_s3_key}")

#         hook.load_string(
#             string_data=content,
#             key=dest_s3_key,
#             bucket_name=S3_BUCKET,
#             replace=True,
#         )
#         print(f"Moved {source_s3_key} → {dest_s3_key}")

#     # -----------------------------------------------------------------------
#     # TASK 2 — transform
#     #
#     # Read the raw JSON from S3, clean and reshape the records, then write
#     # the result to S3 as CSV under the processed prefix.
#     #
#     # Each raw record looks like:
#     #   {
#     #       "order_id":   "ORD-001",
#     #       "sku":        "WGT-A",
#     #       "quantity":   "10",      ← string, cast to int
#     #       "unit_price": "9.99",    ← string, cast to float
#     #       "notes":      "fragile"  ← drop this field
#     #   }
#     #
#     # For each record:
#     #   - Cast quantity to int and unit_price to float
#     #   - Compute total_revenue = round(quantity * unit_price, 2)
#     #   - Drop the "notes" field
#     #
#     # Then write a CSV (with header row) to S3:
#     #   - Build the CSV string using pandas DataFrame.to_csv()
#     #   - Upload to dest_s3_key with hook.load_string()
#     # -----------------------------------------------------------------------

#     @task
#     def transform(source_s3_key, dest_s3_key):
#         import pandas as pd

#         hook = S3Hook(aws_conn_id=S3_CONN_ID)
#         content = hook.read_key(key=source_s3_key, bucket_name=S3_BUCKET)
#         records = json.loads(content)

#         cleaned = []
#         for r in records:
#             quantity   = int(r["quantity"])
#             unit_price = float(r["unit_price"])
#             cleaned.append({
#                 "order_id":      r["order_id"],
#                 "sku":           r["sku"],
#                 "quantity":      quantity,
#                 "unit_price":    unit_price,
#                 "total_revenue": round(quantity * unit_price, 2),
#             })

#         csv = pd.DataFrame(cleaned).to_csv(index=False)

#         hook.load_string(
#             string_data=csv,
#             key=dest_s3_key,
#             bucket_name=S3_BUCKET,
#             replace=True,
#         )
#         print(f"Wrote {len(cleaned)} records to {dest_s3_key}")

#     # -----------------------------------------------------------------------
#     # TASK 3 — load
#     #
#     # Read the processed CSV from S3 and upsert it into Redshift.
#     # S3ToRedshiftOperator issues a Redshift COPY command under the hood —
#     # no SQL required. method="UPSERT" with upsert_keys=["order_id"] deletes
#     # any existing rows matching on order_id before inserting, making the
#     # load idempotent so backfills and retries are safe.
#     # -----------------------------------------------------------------------

#     load = S3ToRedshiftOperator(
#         task_id="load",
#         s3_bucket=S3_BUCKET,
#         s3_key=S3_KEYS['transform'],
#         schema=REDSHIFT_SCHEMA,
#         table=REDSHIFT_TABLE,
#         aws_conn_id=S3_CONN_ID,
#         redshift_conn_id=REDSHIFT_CONN_ID,
#         method="UPSERT",
#         upsert_keys=["order_id"],
#         copy_options=["CSV", "IGNOREHEADER 1"],
#     )

#     # -----------------------------------------------------------------------
#     # WIRING
#     #
#     # Each task reads and writes directly from S3, so dependencies are set
#     # with >> rather than passing return values between tasks. This makes
#     # every stage independently replayable from its S3 input.
#     # -----------------------------------------------------------------------

#     extract(S3_KEYS['landing'], S3_KEYS['extract']) >> \
#     transform(S3_KEYS['extract'], S3_KEYS['transform']) >> \
#     load