# deploy_service.ps1
param(
  [string]$Project = 'closer-video-similarity',
  [string]$Region = 'us-central1',
  [string]$ImageTag = 'latest',
  [string]$PageId = '444025482768886',
  [string]$WorkerUrl = 'https://face-processor-810614481902.us-central1.run.app/task',
  [string]$TasksQueue = 'face-processing-queue-2'
)

gcloud config set project $Project

$img = "gcr.io/$Project/ads-fetcher:$ImageTag"
Write-Output "Building and pushing image: $img"
# Build from the python_app directory where the Dockerfile lives
gcloud builds submit python_app --tag $img --project $Project

Write-Output "Deploying to Cloud Run as 'ads-fetcher-service' with runtime SA..."
gcloud run deploy ads-fetcher-service --image=$img --region=$Region --platform=managed --service-account=ads-fetcher-sa@$Project.iam.gserviceaccount.com --no-allow-unauthenticated --project=$Project

$SERVICE_URL = gcloud run services describe ads-fetcher-service --region=$Region --format='value(status.url)' --project=$Project
Write-Output "Service URL: $SERVICE_URL"

Write-Output "Setting environment variables..."
$envVars = @(
    "EXPECT_AUDIENCES=$SERVICE_URL",
    "GCP_PROJECT=$Project",
    "RAPIDAPI_KEY_SECRET=rapidapi-key",
    "CLOUD_TASKS_QUEUE=$TasksQueue",
    "CLOUD_TASKS_LOCATION=$Region",
    "WORKER_URL=$WorkerUrl",
    "TASKS_INVOKER_SA=tasks-invoker@$Project.iam.gserviceaccount.com",
    "PAGE_ID=$PageId"
) -join ','

gcloud run services update ads-fetcher-service --update-env-vars="$envVars" --region=$Region --project=$Project

Write-Output "Granting run.invoker to ads-fetcher-invoker service account..."
gcloud run services add-iam-policy-binding ads-fetcher-service --member="serviceAccount:ads-fetcher-invoker@$Project.iam.gserviceaccount.com" --role="roles/run.invoker" --region=$Region --project=$Project

Write-Output "Deploy complete. SERVICE_URL: $SERVICE_URL"