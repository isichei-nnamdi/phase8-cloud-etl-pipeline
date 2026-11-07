# ====================================================================
# Deploys the live ETL pipeline to Google Cloud (Windows PowerShell).
#   CSV uploaded to a Cloud Storage bucket -> Cloud Run function -> BigQuery
#
# REQUIRES A BILLING ACCOUNT linked to the project. Read README.md,
# Part 6, Option B before running this, and set a budget alert first.
#
# Run from anywhere:
#   powershell -ExecutionPolicy Bypass -File live_pipeline\cloud_function\deploy.ps1
# ====================================================================

# ---------------- EDIT THESE VALUES ----------------
$ProjectId  = "your-project-id"
$Region     = "us-central1"
$Bucket     = "$ProjectId-supply-chain-landing"   # bucket names are global, so the project ID keeps it unique
$Dataset    = "phase8_training"
$Table      = "supply_chain_orders"
$BqLocation = "US"
$Function   = "supply-chain-etl"
# ---------------------------------------------------

if ($ProjectId -eq "your-project-id") { Write-Host "Edit `$ProjectId at the top of deploy.ps1 first." -ForegroundColor Red; exit 1 }

function Check($step) {
    if ($LASTEXITCODE -ne 0) { Write-Host "FAILED at: $step. Read the error above, then see README.md troubleshooting." -ForegroundColor Red; exit 1 }
}

Set-Location $PSScriptRoot
gcloud config set project $ProjectId; Check "set project"

Write-Host "`n[1/5] Enabling the required APIs (1-2 minutes the first time)..." -ForegroundColor Cyan
gcloud services enable cloudfunctions.googleapis.com run.googleapis.com cloudbuild.googleapis.com `
    artifactregistry.googleapis.com eventarc.googleapis.com pubsub.googleapis.com `
    storage.googleapis.com bigquery.googleapis.com logging.googleapis.com
Check "enable APIs"

Write-Host "`n[2/5] Creating the landing bucket gs://$Bucket ..." -ForegroundColor Cyan
$null = gcloud storage buckets describe "gs://$Bucket" 2>&1
if ($LASTEXITCODE -ne 0) {
    gcloud storage buckets create "gs://$Bucket" --location=$Region --uniform-bucket-level-access
    Check "create bucket"
} else { Write-Host "Bucket already exists." }

Write-Host "`n[3/5] Granting permissions to the service accounts..." -ForegroundColor Cyan
$ProjectNumber = gcloud projects describe $ProjectId --format="value(projectNumber)"; Check "get project number"
$RuntimeSa = "$ProjectNumber-compute@developer.gserviceaccount.com"
$GcsAgent  = gcloud storage service-agent --project=$ProjectId; Check "get storage service agent"
$roles = "roles/bigquery.dataEditor", "roles/bigquery.jobUser", "roles/storage.objectViewer",
         "roles/eventarc.eventReceiver", "roles/run.invoker", "roles/cloudbuild.builds.builder"
foreach ($role in $roles) {
    $null = gcloud projects add-iam-policy-binding $ProjectId --member="serviceAccount:$RuntimeSa" --role=$role --condition=None --quiet
    Check "grant $role"
}
$null = gcloud projects add-iam-policy-binding $ProjectId --member="serviceAccount:$GcsAgent" --role="roles/pubsub.publisher" --condition=None --quiet
Check "grant pubsub.publisher to the Cloud Storage service agent"

Write-Host "`n[4/5] Copying the shared ETL code into the function folder..." -ForegroundColor Cyan
Copy-Item "..\..\src\etl_to_bigquery.py" "." -Force

Write-Host "`n[5/5] Deploying the function (3-5 minutes)..." -ForegroundColor Cyan
gcloud functions deploy $Function --gen2 --runtime=python312 --region=$Region `
    --source=. --entry-point=on_upload --trigger-bucket=$Bucket `
    --memory=512Mi --timeout=300s `
    --set-env-vars="GCP_PROJECT_ID=$ProjectId,BQ_DATASET=$Dataset,BQ_TABLE=$Table,BQ_LOCATION=$BqLocation,BQ_WRITE_MODE=WRITE_TRUNCATE"
Check "deploy function"

Write-Host "`nDone. Test it by uploading the CSV:" -ForegroundColor Green
Write-Host "  gcloud storage cp ..\..\data\supply_chain_orders_raw.csv gs://$Bucket/"
Write-Host "Then read the logs:"
Write-Host "  gcloud functions logs read $Function --gen2 --region=$Region --limit=20"
