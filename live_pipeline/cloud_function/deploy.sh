#!/usr/bin/env bash
# ====================================================================
# Deploys the live ETL pipeline to Google Cloud (macOS / Linux / Cloud Shell).
#   CSV uploaded to a Cloud Storage bucket -> Cloud Run function -> BigQuery
#
# REQUIRES A BILLING ACCOUNT linked to the project. Read README.md,
# Part 6, Option B before running this, and set a budget alert first.
#
# Run:  bash live_pipeline/cloud_function/deploy.sh
# ====================================================================
set -euo pipefail

# ---------------- EDIT THESE VALUES ----------------
PROJECT_ID="your-project-id"
REGION="us-central1"
BUCKET="${PROJECT_ID}-supply-chain-landing"
DATASET="phase8_training"
TABLE="supply_chain_orders"
BQ_LOCATION="US"
FUNCTION="supply-chain-etl"
# ---------------------------------------------------

if [ "$PROJECT_ID" = "your-project-id" ]; then echo "Edit PROJECT_ID at the top of deploy.sh first."; exit 1; fi

cd "$(dirname "$0")"
gcloud config set project "$PROJECT_ID"

echo "[1/5] Enabling the required APIs (1-2 minutes the first time)..."
gcloud services enable cloudfunctions.googleapis.com run.googleapis.com cloudbuild.googleapis.com \
  artifactregistry.googleapis.com eventarc.googleapis.com pubsub.googleapis.com \
  storage.googleapis.com bigquery.googleapis.com logging.googleapis.com

echo "[2/5] Creating the landing bucket gs://$BUCKET ..."
if ! gcloud storage buckets describe "gs://$BUCKET" >/dev/null 2>&1; then
  gcloud storage buckets create "gs://$BUCKET" --location="$REGION" --uniform-bucket-level-access
else
  echo "Bucket already exists."
fi

echo "[3/5] Granting permissions to the service accounts..."
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format="value(projectNumber)")
RUNTIME_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
GCS_AGENT=$(gcloud storage service-agent --project="$PROJECT_ID")
for ROLE in roles/bigquery.dataEditor roles/bigquery.jobUser roles/storage.objectViewer \
            roles/eventarc.eventReceiver roles/run.invoker roles/cloudbuild.builds.builder; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:$RUNTIME_SA" \
    --role="$ROLE" --condition=None --quiet >/dev/null
done
gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:$GCS_AGENT" \
  --role="roles/pubsub.publisher" --condition=None --quiet >/dev/null

echo "[4/5] Copying the shared ETL code into the function folder..."
cp ../../src/etl_to_bigquery.py .

echo "[5/5] Deploying the function (3-5 minutes)..."
gcloud functions deploy "$FUNCTION" --gen2 --runtime=python312 --region="$REGION" \
  --source=. --entry-point=on_upload --trigger-bucket="$BUCKET" \
  --memory=512Mi --timeout=300s \
  --set-env-vars="GCP_PROJECT_ID=$PROJECT_ID,BQ_DATASET=$DATASET,BQ_TABLE=$TABLE,BQ_LOCATION=$BQ_LOCATION,BQ_WRITE_MODE=WRITE_TRUNCATE"

echo
echo "Done. Test it by uploading the CSV:"
echo "  gcloud storage cp ../../data/supply_chain_orders_raw.csv gs://$BUCKET/"
echo "Then read the logs:"
echo "  gcloud functions logs read $FUNCTION --gen2 --region=$REGION --limit=20"
