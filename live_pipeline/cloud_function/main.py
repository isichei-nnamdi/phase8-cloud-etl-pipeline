"""
Live ETL: a Cloud Run function that runs every time a CSV file is
uploaded to a Cloud Storage bucket.

    CSV uploaded to gs://<bucket>  ->  on_upload()  ->  transform()  ->  BigQuery

It reuses transform() and load_to_bigquery() from src/etl_to_bigquery.py. The deploy script copies that file into
this folder before deploying, so there is only one copy of the rules.

Deploy with deploy.ps1 (Windows) or deploy.sh (macOS / Linux).
See README.md, Part 6, Option B.
"""
import io

import functions_framework
import pandas as pd
from google.cloud import storage

from etl_to_bigquery import load_to_bigquery, transform


@functions_framework.cloud_event
def on_upload(cloud_event):
    """Triggered by a 'file finalized' event on the landing bucket."""
    info = cloud_event.data
    bucket_name, file_name = info["bucket"], info["name"]

    if not file_name.lower().endswith(".csv"):
        print(f"Skipping {file_name}: not a CSV file")
        return

    print(f"New file: gs://{bucket_name}/{file_name}")

    # Extract: download the uploaded file into a DataFrame
    blob = storage.Client().bucket(bucket_name).blob(file_name)
    raw = pd.read_csv(io.BytesIO(blob.download_as_bytes()))

    # Transform and load: the same functions as the local pipeline
    clean = transform(raw)
    print(f"Raw rows: {len(raw)} | Clean rows: {len(clean)}")
    load_to_bigquery(clean)
