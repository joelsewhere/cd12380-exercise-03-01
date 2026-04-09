# Run this DAG once before starting the exercise.
# It seeds S3 with a fake orders.json file for each day 

# S3 layout written:
#   landing/transactions/{date}/orders.json   for each date from 2026-01-01
#                                         to 2026-01-07

from __future__ import annotations

import json
import random
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


S3_CONN_ID = "aws_default"
S3_BUCKET  = NotImplemented ### SET YOUR BUCKET NAME HERE

# Exercise window — one file per day
START_DATE = datetime(2026, 1, 1)
END_DATE   = datetime(2026, 1, 7)

SKUS = ["WGT-A", "WGT-B", "GAD-C", "GAD-D", "PART-E"]


def _raw_key(date_str: str) -> str:
    return f"landing/transactions/{date_str}/orders.json"


def _generate_orders(date_str: str, n: int = 20) -> list[dict]:
    """
    Generate n fake order records for a given date.
    quantity and unit_price are intentionally stored as strings
    to match the exercise scenario (cast required in transform).
    The notes field is included as a field the student must drop.
    """
    rng = random.Random(date_str)  # seeded so reruns are deterministic
    orders = []
    for i in range(1, n + 1):
        quantity   = rng.randint(1, 100)
        unit_price = round(rng.uniform(1.99, 199.99), 2)
        orders.append({
            "order_id":   f"{date_str}-ORD-{i:03d}",
            "sku":        rng.choice(SKUS),
            "quantity":   str(quantity),        # string — student must cast
            "unit_price": str(unit_price),      # string — student must cast
            "notes":      rng.choice([          # field — student must drop
                "fragile", "gift wrap", "urgent", "standard", ""
            ]),
        })
    return orders


@dag
def setup_s3():

    @task
    def seed_s3():
        
        hook = S3Hook(aws_conn_id=S3_CONN_ID)

        current = START_DATE
        while current <= END_DATE:
            date_str = current.strftime("%Y-%m-%d")
            orders   = _generate_orders(date_str)
            key      = _raw_key(date_str)

            hook.load_string(
                string_data=json.dumps(orders, indent=2),
                key=key,
                bucket_name=S3_BUCKET,
                replace=True,
            )
            print(f"Uploaded {len(orders)} orders → s3://{S3_BUCKET}/{key}")

            current += timedelta(days=1)

        print(f"Setup complete. Seeded {(END_DATE - START_DATE).days + 1} days.")

    seed_s3()


setup_s3()