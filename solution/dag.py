# # dags/sales_etl_solution.py
# #
# # Airflow 3.1 Exercise — AWS ETL Pipeline (SOLUTION)
# #
# # Scenario
# # --------
# # Every night the sales platform drops a JSON file of the day's transactions
# # into S3 under a raw prefix. This DAG must:
# #
# #   1. Extract   — read the raw JSON file from S3
# #   2. Transform — clean and reshape the records in Python, then write the
# #                  result back to S3 as CSV under a processed prefix
# #   3. Load      — use S3ToRedshiftOperator to upsert the processed CSV
# #                  into the Redshift production table
# #
# # Using S3 as the intermediate store between transform and load is the
# # standard production pattern for Redshift pipelines. It avoids hitting
# # Redshift row-by-row INSERT limits and makes each stage independently
# # replayable — if the load fails, the processed file is already in S3 and
# # transform does not need to re-run.
# #
# # S3 layout:
# #   raw/transactions/{date}/orders.json        ← written by upstream system
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

# import csv
# import io
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
# COLUMNS          = ["order_id", "sku", "quantity", "unit_price", "total_revenue"]


# # ---------------------------------------------------------------------------
# # Helpers (do not modify)
# # ---------------------------------------------------------------------------

# def _raw_key(date_str: str) -> str:
#     return f"raw/transactions/{date_str}/orders.json"

# def _processed_key(date_str: str) -> str:
#     return f"processed/transactions/{date_str}/orders.csv"


# # ---------------------------------------------------------------------------
# # DAG definition
# # ---------------------------------------------------------------------------

# with DAG(
#     dag_id="sales_etl_solution",
#     schedule="@daily",
#     start_date=datetime(2024, 1, 1),
#     catchup=False,
#     tags=["exercise", "etl", "aws"],
# ):

#     # -----------------------------------------------------------------------
#     # TASK 1 — extract
#     #
#     # Read the raw JSON file from S3 and return its contents as a list of
#     # dicts. Use S3Hook to fetch the file.
#     #
#     # Steps:
#     #   - Instantiate S3Hook(aws_conn_id=S3_CONN_ID)
#     #   - Call hook.read_key(key=_raw_key(date_str), bucket_name=S3_BUCKET)
#     #     to get the raw JSON string
#     #   - Parse with json.loads() and return the list
#     #
#     # Hint: the logical date string is available as context["ds"].
#     # -----------------------------------------------------------------------

#     @task
#     def extract(**context) -> list[dict]:
#         date_str = context["ds"]
#         hook = S3Hook(aws_conn_id=S3_CONN_ID)
#         raw = hook.read_key(key=_raw_key(date_str), bucket_name=S3_BUCKET)
#         records = json.loads(raw)
#         print(f"Extracted {len(records)} records from {_raw_key(date_str)}")
#         return records

#     # -----------------------------------------------------------------------
#     # TASK 2 — transform
#     #
#     # Receive the raw record list from extract, clean and reshape the records,
#     # then write the result to S3 as CSV. Return the S3 key of the output
#     # file — this is passed to S3ToRedshiftOperator as its s3_key.
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
#     #   - Keep only the fields in COLUMNS
#     #
#     # Then write a CSV (with header row) to S3:
#     #   - Build the CSV string in memory using csv.DictWriter and io.StringIO
#     #   - Upload with hook.load_string(
#     #         string_data=...,
#     #         key=_processed_key(date_str),
#     #         bucket_name=S3_BUCKET,
#     #         replace=True,
#     #     )
#     #   - Return _processed_key(date_str)
#     # -----------------------------------------------------------------------

#     @task
#     def transform(records: list[dict], **context) -> str:
#         date_str = context["ds"]

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

#         buf = io.StringIO()
#         writer = csv.DictWriter(buf, fieldnames=COLUMNS)
#         writer.writeheader()
#         writer.writerows(cleaned)

#         hook = S3Hook(aws_conn_id=S3_CONN_ID)
#         hook.load_string(
#             string_data=buf.getvalue(),
#             key=_processed_key(date_str),
#             bucket_name=S3_BUCKET,
#             replace=True,
#         )
#         print(f"Wrote {len(cleaned)} records to {_processed_key(date_str)}")
#         return _processed_key(date_str)

#     # -----------------------------------------------------------------------
#     # TASK 3 — load
#     #
#     # Instantiate an S3ToRedshiftOperator that reads the processed CSV from
#     # S3 and upserts it into the Redshift target table.
#     #
#     # S3ToRedshiftOperator handles the COPY and upsert logic for you — no
#     # SQL required. Configure it with:
#     #
#     #   task_id          = "load"
#     #   s3_bucket        = S3_BUCKET
#     #   s3_key           = _processed_key("{{ ds }}")
#     #   schema           = REDSHIFT_SCHEMA
#     #   table            = REDSHIFT_TABLE
#     #   column_list      = COLUMNS
#     #   aws_conn_id      = S3_CONN_ID
#     #   redshift_conn_id = REDSHIFT_CONN_ID
#     #   method           = "UPSERT"
#     #   upsert_keys      = ["order_id"]
#     #   copy_options     = ["CSV", "IGNOREHEADER 1"]
#     #
#     # method="UPSERT" and upsert_keys tell the operator to delete any
#     # existing rows that match on order_id before inserting, making the
#     # load idempotent.
#     # -----------------------------------------------------------------------

#     load = S3ToRedshiftOperator(
#         task_id="load",
#         s3_bucket=S3_BUCKET,
#         s3_key=_processed_key("{{ ds }}"),
#         schema=REDSHIFT_SCHEMA,
#         table=REDSHIFT_TABLE,
#         column_list=COLUMNS,
#         aws_conn_id=S3_CONN_ID,
#         redshift_conn_id=REDSHIFT_CONN_ID,
#         method="UPSERT",
#         upsert_keys=["order_id"],
#         copy_options=["CSV", "IGNOREHEADER 1"],
#     )

#     # -----------------------------------------------------------------------
#     # WIRING
#     #
#     # Chain the three tasks so data flows:
#     #   extract → transform → load
#     #
#     # transform receives the list returned by extract.
#     # load must run after transform — use >> to set the dependency since
#     # S3ToRedshiftOperator reads its S3 key from the Jinja-templated
#     # s3_key parameter rather than from XCom.
#     # -----------------------------------------------------------------------

#     records = extract()
#     transformed = transform(records)
#     transformed >> load