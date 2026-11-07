"""
Phase 8 Portfolio Project 9 — Cloud-Based ETL Pipeline

Extract  : read the raw orders CSV with pandas
Transform: clean, validate and derive business fields
Load     : write the clean table to BigQuery

Run from the repository root:
    python src/etl_to_bigquery.py

Before the first run (see README.md, Part 1):
  1. Sign in once with:  gcloud auth application-default login
  2. Copy .env.example to .env and fill in your own values.

Settings (read from .env, or from environment variables set in your terminal):
  GCP_PROJECT_ID  required   your Google Cloud project ID
  BQ_DATASET      optional   default: phase8_training
  BQ_TABLE        optional   default: supply_chain_orders
  BQ_LOCATION     optional   default: US (must match your dataset's location)
  BQ_WRITE_MODE   optional   WRITE_TRUNCATE (default, replace) or WRITE_APPEND (add rows)
  SOURCE_FILE     optional   default: data/supply_chain_orders_raw.csv
"""

import os
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

REPO_ROOT = Path(__file__).resolve().parent.parent

# Load settings from a .env file at the repository root, if one exists.
# Variables already set in the terminal take priority over the .env file.
try:
    from dotenv import load_dotenv
    load_dotenv(REPO_ROOT / ".env")
except ImportError:
    pass

SOURCE_FILE = os.getenv("SOURCE_FILE", str(REPO_ROOT / "data" / "supply_chain_orders_raw.csv"))

def extract(path: str) -> pd.DataFrame:
    """Extract raw orders from CSV."""
    return pd.read_csv(path)

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Clean, validate, and derive business metrics."""
    data = df.copy()
    data.columns = [c.strip().lower() for c in data.columns]
    data["order_date"] = pd.to_datetime(data["order_date"], errors="coerce")
    data["quantity"] = pd.to_numeric(data["quantity"], errors="coerce")
    data["unit_cost_ngn"] = pd.to_numeric(data["unit_cost_ngn"], errors="coerce")
    data["delivery_days"] = pd.to_numeric(data["delivery_days"], errors="coerce")

    # Remove exact duplicate records
    data = data.drop_duplicates()

    # Standardise text fields
    for col in ["product", "region", "supplier", "status"]:
        data[col] = data[col].astype("string").str.strip().str.title()

    # Apply transparent, explainable cleaning rules
    data = data.dropna(subset=["order_id", "order_date", "product", "quantity", "unit_cost_ngn"])
    data = data[data["quantity"] > 0]
    data = data[data["unit_cost_ngn"] >= 0]
    data.loc[data["delivery_days"] < 0, "delivery_days"] = pd.NA
    data["supplier"] = data["supplier"].fillna("Unknown")
    data["order_value_ngn"] = data["quantity"] * data["unit_cost_ngn"]
    data["order_month"] = data["order_date"].dt.to_period("M").astype(str)
    return data

def load_to_bigquery(data: pd.DataFrame) -> None:
    project_id = os.environ["GCP_PROJECT_ID"]
    dataset_id = os.getenv("BQ_DATASET", "phase8_training")
    table_name = os.getenv("BQ_TABLE", "supply_chain_orders")
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset_id}.{table_name}"

    # Create the dataset if it does not already exist
    dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
    dataset.location = os.getenv("BQ_LOCATION", "US")  # must match the location of an existing dataset
    client.create_dataset(dataset, exists_ok=True)

    # WRITE_TRUNCATE replaces the table on every run (safe to rerun).
    # WRITE_APPEND adds rows (use only when each source file holds new orders).
    write_mode = os.getenv("BQ_WRITE_MODE", "WRITE_TRUNCATE").upper()
    if write_mode not in ("WRITE_TRUNCATE", "WRITE_APPEND"):
        raise ValueError(f"BQ_WRITE_MODE must be WRITE_TRUNCATE or WRITE_APPEND, not {write_mode!r}")

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_mode,
        autodetect=True,
    )
    job = client.load_table_from_dataframe(data, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(data):,} rows into {table_id}")

if __name__ == "__main__":
    raw = extract(SOURCE_FILE)
    clean = transform(raw)
    print(f"Raw rows: {len(raw)} | Clean rows: {len(clean)}")
    load_to_bigquery(clean)
