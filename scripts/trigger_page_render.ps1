param(
  [string]$PageId = "444025482768886",
  [string]$ServiceUrl = "https://closer-ads-cron-service.onrender.com",
  [string]$Invoker = "ads-fetcher-invoker@closer-video-similarity.iam.gserviceaccount.com"
)

Write-Host "Triggering /run-page for page_id=$PageId on $ServiceUrl" -ForegroundColor Cyan

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

$body = @{ page_id = $PageId } | ConvertTo-Json

try {
    $response = Invoke-RestMethod -Uri "$ServiceUrl/run-page" -Method Post -Headers @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" } -Body $body -TimeoutSec 600

    Write-Host "Status: $($response.status)" -ForegroundColor Green
    Write-Host "Ads fetched: $($response.ads_fetched)  Tasks created: $($response.tasks_created)  Skipped: $($response.tasks_skipped)"

    if ($response.logs) {
        Write-Host "`nFetcher logs:" -ForegroundColor Cyan
        foreach ($logLine in $response.logs) {
            Write-Host "  $logLine" -ForegroundColor Gray
        }
    }

    if ($response.raw_responses -and $response.raw_responses.Count -gt 0) {
        $ts = (Get-Date).ToString("yyyyMMdd_HHmmss")
        $file = "raw_api_responses_${PageId}_${ts}.json"
        $response.raw_responses | ConvertTo-Json -Depth 12 | Out-File -FilePath $file -Encoding utf8
        Write-Host "Saved raw responses to $file" -ForegroundColor Yellow
    }
} catch {
    Write-Error "API call failed: $_"
    exit 2
}