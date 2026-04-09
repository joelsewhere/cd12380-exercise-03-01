# Exercise: AWS ETL Pipeline

Build an Airflow DAG that moves daily sales transactions from S3 into Redshift.

## Setup

**1. Configure your S3 bucket**

Open `setup.py` and set `S3_BUCKET` to your assigned bucket name.

**2. Run the setup DAG**

Trigger `setup` manually from the Airflow UI. This seeds your S3 bucket with one JSON file per day for the exercise window.

**3. Verify**

Check your S3 bucket for files at `landing/transactions/{date}/orders.json`.

---

## Your Task

Complete `dag.py`. The DAG has three tasks:

| Task | What it does |
|---|---|
| `extract` | Reads the JSON from the landing prefix, validates it, writes it to raw |
| `transform` | Reads the raw JSON, cleans the records, writes a CSV to processed |
| `load` | Upserts the processed CSV into Redshift using `S3ToRedshiftOperator` |

Each `### YOUR CODE HERE` marker indicates where you need to write code.
Read the comment above each marker carefully — it describes exactly what
the task should do and which methods to use.

---

## Connections Required

| Connection ID | Type | Points at |
|---|---|---|
| `aws_default` | Amazon Web Services | Your S3 bucket |
| `redshift_default` | Amazon Redshift | The course Redshift cluster |

---

## S3 Layout

```
landing/transactions/{date}/orders.json    ← seeded by setup DAG
raw/transactions/{date}/orders.json        ← written by extract
processed/transactions/{date}/orders.csv   ← written by transform
```

## Redshift Target

Schema: `sales` — Table: `daily_transactions`