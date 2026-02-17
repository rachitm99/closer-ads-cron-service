# test_service.ps1
param(
  [string]$Project = 'closer-video-similarity',
  [string]$Region = 'us-central1'
)

gcloud config set project $Project
$SERVICE_URL = gcloud run services describe ads-fetcher-service --region=$Region --format='value(status.url)' --project=$Project
Write-Output "Service URL: $SERVICE_URL"

Write-Output "Fetching identity token by impersonating ads-fetcher-invoker..."
$TOKEN = gcloud auth print-identity-token --impersonate-service-account=ads-fetcher-invoker@$Project.iam.gserviceaccount.com --audiences=$SERVICE_URL --project=$Project

Write-Output "Calling /run endpoint..."
Invoke-RestMethod -Method POST -Uri "$SERVICE_URL/run" -Headers @{Authorization = "Bearer $TOKEN" } -ContentType 'application/json' -Body '{}'

Write-Output "Call completed. Check Firestore 'ads' collection for results."