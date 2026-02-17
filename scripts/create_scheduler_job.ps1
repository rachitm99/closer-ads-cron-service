# create_scheduler_job.ps1
param(
  [string]$Project = 'closer-video-similarity',
  [string]$Region = 'us-central1',
  [string]$Schedule = '0 0 * * *' # daily at midnight
)

gcloud config set project $Project

$SERVICE_URL = gcloud run services describe ads-fetcher-service --region=$Region --format='value(status.url)' --project=$Project
Write-Output "Using service URL: $SERVICE_URL"

Write-Output "Creating Cloud Scheduler job 'ads-fetcher-cron'..."
gcloud scheduler jobs create http ads-fetcher-cron --schedule="$Schedule" --uri="$SERVICE_URL/run" --http-method=POST --oidc-service-account-email=ads-fetcher-invoker@$Project.iam.gserviceaccount.com --oidc-token-audience="$SERVICE_URL" --location=$Region --project=$Project

Write-Output "Scheduler job created. Verify in console or with 'gcloud scheduler jobs describe ads-fetcher-cron --location=$Region --project=$Project'"