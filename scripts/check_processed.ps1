param(
  [string]$Project = 'closer-video-similarity',
  [string]$Region = 'us-central1',
  [string]$ServiceName = 'ads-fetcher-service'
)

Write-Output "Checking processed count from $ServiceName in project $Project (region $Region)"

gcloud config set project $Project | Out-Null
$SERVICE_URL = gcloud run services describe $ServiceName --region=$Region --format='value(status.url)' --project=$Project
if (-not $SERVICE_URL) { Write-Error "Service URL not found"; exit 2 }
Write-Output "Service URL: $SERVICE_URL"

$invoker = "ads-fetcher-invoker@$Project.iam.gserviceaccount.com"
$TOKEN = gcloud auth print-identity-token --impersonate-service-account=$invoker --audiences=$SERVICE_URL --project=$Project
if (-not $TOKEN) { Write-Error "Failed to fetch identity token"; exit 3 }

# Call endpoint
try {
  $resp = Invoke-RestMethod -Method POST -Uri "$SERVICE_URL/run" -Headers @{ Authorization = "Bearer $TOKEN" } -ContentType 'application/json' -Body '{}' -TimeoutSec 60
} catch {
  Write-Error "Request failed: $_"
  exit 4
}

# Validate processed
if ($null -ne $resp.processed) {
  try {
    $count = [int]$resp.processed
    Write-Output "Service returned processed count: $count"
    exit 0
  } catch {
    Write-Error "Processed value is not numeric: $($resp.processed)"
    exit 5
  }
} else {
  Write-Error "Response missing 'processed' field: $($resp | ConvertTo-Json -Depth 5)"
  exit 6
}
