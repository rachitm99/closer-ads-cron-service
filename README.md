# ads-fetcher (Python)

This Cloud Run service fetches Facebook ads for all brands in your Firestore `brands` collection. It fetches ads from RapidAPI, tracks when each brand was last fetched, and creates Cloud Tasks for processing.

## Features

- **Firestore Integration**: Reads all brands from `brands` collection
- **Smart Fetching**: Uses `lastFetched` timestamp per brand (or 10 days default)
- **RapidAPI**: Fetches ads with pagination until hitting old ads
- **Cloud Tasks**: Creates tasks for face processing service
- **Automatic Scheduling**: Runs daily at midnight via Cloud Scheduler

## Files

- `python_app/app.py` - Flask service with RapidAPI integration
- `python_app/requirements.txt` - Python dependencies
- `python_app/Dockerfile` - Container image for Cloud Run
- `scripts/*.ps1` - Windows PowerShell deployment and management scripts

## Setup

1) Authenticate locally:
   ```powershell
   gcloud auth login
   gcloud auth application-default login
   ```

2) Create service accounts and assign IAM roles:
   ```powershell
   .\scripts\create_service_accounts.ps1
   ```

3) Deploy the Cloud Run service:
   ```powershell
   .\scripts\deploy_service.ps1
   ```

4) Create Cloud Scheduler job (runs daily at midnight):
   ```powershell
   .\scripts\create_scheduler_job.ps1
   ```

## Usage

### Manual Trigger (Run Immediately)
```powershell
.\scripts\trigger_now.ps1
```

### Test Health & Authentication
```powershell
.\scripts\smoke_test.ps1
```

### Scheduled Execution
The service runs **automatically every day at 12:00 AM UTC** via Cloud Scheduler.

View the schedule:
```powershell
gcloud scheduler jobs describe ads-fetcher-cron --location=us-central1 --project=closer-video-similarity
```

## Environment Variables

- `EXPECT_AUDIENCES` - Service URL for OIDC token validation
- `GCP_PROJECT` - GCP project ID
- `RAPIDAPI_KEY_SECRET` - Secret Manager secret name for RapidAPI key
- `CLOUD_TASKS_QUEUE` - Cloud Tasks queue name (default: face-processing-queue-2)
- `CLOUD_TASKS_LOCATION` - Queue location (default: us-central1)
- `WORKER_URL` - Face processor service URL
- `TASKS_INVOKER_SA` - Service account for task invocation

## How It Works

1. Service reads all brands from Firestore `brands` collection
2. For each brand:
   - Gets `page_id` field
   - Checks `lastFetched` timestamp (or uses 10 days default)
   - Fetches ads from RapidAPI since that timestamp
   - Stops pagination when hitting ads older than cutoff
   - Creates Cloud Tasks for each ad
   - Updates brand document with new `lastFetched` timestamp

## Requirements

- GCP Project with billing enabled
- Cloud Run API enabled
- Cloud Scheduler API enabled
- Cloud Tasks API enabled
- Secret Manager with `rapidapi-key` secret
- Firestore database with `brands` collection
