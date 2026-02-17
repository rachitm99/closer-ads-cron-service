# trigger_now.ps1
# Manually trigger the ads fetcher service immediately
param(
  [string]$Project = 'closer-video-similarity',
  [string]$Region = 'us-central1'
)

Write-Output "Triggering ads-fetcher-service manually..."
Write-Output "Project: $Project"
Write-Output "Region: $Region"
Write-Output ""

gcloud config set project $Project

# Get the service URL
$SERVICE_URL = gcloud run services describe ads-fetcher-service --region=$Region --format='value(status.url)' --project=$Project

if (-not $SERVICE_URL) {
  Write-Error "Failed to get service URL"
  exit 1
}

Write-Output "Service URL: $SERVICE_URL"
Write-Output ""

# Get identity token by impersonating the invoker service account
Write-Output "Fetching identity token..."
$invoker = "ads-fetcher-invoker@$Project.iam.gserviceaccount.com"
try {
  $TOKEN = gcloud auth print-identity-token --impersonate-service-account=$invoker --audiences=$SERVICE_URL --project=$Project
} catch {
  Write-Error "Failed to get identity token: $_"
  exit 2
}

if (-not $TOKEN) {
  Write-Error "No identity token received"
  exit 2
}

Write-Output "Token acquired successfully"
Write-Output ""
Write-Output "Calling POST $SERVICE_URL/run..."
Write-Output ""

try {
  $resp = Invoke-RestMethod -Method POST -Uri "$SERVICE_URL/run" -Headers @{ Authorization = "Bearer $TOKEN" } -ContentType 'application/json' -Body '{}' -TimeoutSec 600
  
  Write-Output "Response:"
  Write-Output ($resp | ConvertTo-Json -Depth 5)
  Write-Output ""
  
  if ($resp.status -eq 'ok') {
    Write-Output "SUCCESS!" -ForegroundColor Green
    Write-Output "  Brands processed: $($resp.brands_processed)"
    Write-Output "  Ads fetched: $($resp.ads_fetched)"
    Write-Output "  Tasks created: $($resp.tasks_created)"
    Write-Output "  Tasks skipped: $($resp.tasks_skipped)"
    exit 0
  } else {
    Write-Error "Run returned non-ok status: $($resp | ConvertTo-Json -Depth 5)"
    exit 3
  }
} catch {
  Write-Error "API call failed: $_"
  exit 3
}
