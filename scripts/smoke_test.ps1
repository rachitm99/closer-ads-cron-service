<#
smoke_test.ps1

Checks that the ads-fetcher Cloud Run service is healthy and that an authenticated POST /run succeeds.
Exits with code 0 on success, non-zero on failure.

Usage:
  .\scripts\smoke_test.ps1 -Project closer-video-similarity -Region us-central1 -ServiceName ads-fetcher-service
#>
param(
  [string]$Project = 'closer-video-similarity',
  [string]$Region = 'us-central1',
  [string]$ServiceName = 'ads-fetcher-service'
)

Write-Output "Using project: $Project, region: $Region, service: $ServiceName"

# Ensure gcloud project
gcloud config set project $Project | Out-Null

# Get service URL
$SERVICE_URL = gcloud run services describe $ServiceName --region=$Region --format='value(status.url)' --project=$Project
if (-not $SERVICE_URL) {
  Write-Error "Could not determine service URL for $ServiceName"
  exit 2
}
Write-Output "Service URL: $SERVICE_URL"

# 1) Health check (authenticated)
$invoker = "ads-fetcher-invoker@$Project.iam.gserviceaccount.com"
Write-Output "Fetching identity token by impersonating $invoker for health check..."
try {
  $TOKEN_HEALTH = gcloud auth print-identity-token --impersonate-service-account=$invoker --audiences=$SERVICE_URL --project=$Project
} catch {
  Write-Error "Failed to get identity token for health check: $_"
  exit 3
}

Write-Output "--- Health check: GET $SERVICE_URL/health (with ID token) ---"
try {
  $health = Invoke-RestMethod -Method GET -Uri "$SERVICE_URL/health" -Headers @{ Authorization = "Bearer $TOKEN_HEALTH" } -UseBasicParsing -TimeoutSec 15
  if ($health -eq 'ok') {
    Write-Output "Health: OK"
  } else {
    Write-Warning "Unexpected health body: $health"
  }
} catch {
  Write-Error "Health check failed: $_"
  exit 3
}

# 2) Authenticated POST /run using impersonation of invoker SA
$invoker = "ads-fetcher-invoker@$Project.iam.gserviceaccount.com"
Write-Output "Fetching identity token by impersonating $invoker..."
try {
  $TOKEN = gcloud auth print-identity-token --impersonate-service-account=$invoker --audiences=$SERVICE_URL --project=$Project
} catch {
  Write-Error "Failed to get identity token (impersonation may not be configured): $_"
  exit 4
}

if (-not $TOKEN) {
  Write-Error "No identity token received"
  exit 4
}

Write-Output "Calling POST $SERVICE_URL/run..."
try {
  $resp = Invoke-RestMethod -Method POST -Uri "$SERVICE_URL/run" -Headers @{ Authorization = "Bearer $TOKEN" } -ContentType 'application/json' -Body '{}' -TimeoutSec 120
  Write-Output "Response:`n$($resp | ConvertTo-Json -Depth 5)"
  if ($resp.status -eq 'ok') {
    Write-Output "Run: OK, brands_processed=$($resp.brands_processed), ads_fetched=$($resp.ads_fetched), tasks_created=$($resp.tasks_created), tasks_skipped=$($resp.tasks_skipped)"
    Write-Output "SMOKE TEST: PASSED"
    exit 0
  } else {
    Write-Error "Run returned non-ok status: $($resp | ConvertTo-Json -Depth 5)"
    exit 5
  }
} catch {
  Write-Error "Run POST failed: $_"
  exit 5
}
