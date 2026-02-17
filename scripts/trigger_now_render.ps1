param(
  [string]$ServiceUrl = "https://closer-ads-cron-service.onrender.com",
  [string]$Invoker = "ads-fetcher-invoker@closer-video-similarity.iam.gserviceaccount.com"
)

Write-Host "Triggering $ServiceUrl/run ..." -ForegroundColor Cyan

# Prefer CI-provided BEARER_TOKEN; otherwise mint an identity token via gcloud
if ($env:BEARER_TOKEN) {
    $token = $env:BEARER_TOKEN
} else {
    Write-Host "Requesting identity token via gcloud..." -ForegroundColor Yellow
    $tokenOutput = gcloud auth print-identity-token --impersonate-service-account=$Invoker --audiences=$ServiceUrl 2>&1
    $token = ($tokenOutput | Where-Object { $_ -notlike "*WARNING*" -and $_ -notlike "*Python*" } | Select-Object -First 1).Trim()
}

if (-not $token) {
    Write-Error "Failed to obtain identity token"
    exit 1
}

try {
    $resp = Invoke-RestMethod -Method POST -Uri "$ServiceUrl/run" -Headers @{ Authorization = "Bearer $token" } -ContentType 'application/json' -Body '{}' -TimeoutSec 600
    $resp | ConvertTo-Json -Depth 6

    if ($resp.status -eq 'ok') {
        Write-Host "✔ run completed" -ForegroundColor Green
        Write-Host "  Brands processed: $($resp.brands_processed)"
        Write-Host "  Ads fetched: $($resp.ads_fetched)"
        Write-Host "  Tasks created: $($resp.tasks_created)"
        Write-Host "  Tasks skipped: $($resp.tasks_skipped)"
        exit 0
    } else {
        Write-Error "Run returned non-ok status: $($resp | ConvertTo-Json -Depth 4)"
        exit 2
    }
} catch {
    Write-Error "API call failed: $_"
    exit 3
}