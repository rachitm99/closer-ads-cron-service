# Get page_id from parameter or use default
param(
    # [string]$PageId = "444025482768886"
    [string]$PageId = "170694943611404"
)

# Get OIDC token
Write-Host "Getting identity token..." -ForegroundColor Yellow
$SERVICE_URL = "https://ads-fetcher-service-hkcufhsnwq-uc.a.run.app"
$tokenOutput = gcloud auth print-identity-token --impersonate-service-account=ads-fetcher-invoker@closer-video-similarity.iam.gserviceaccount.com --audiences=$SERVICE_URL 2>&1
$token = ($tokenOutput | Where-Object { $_ -notlike "*WARNING*" -and $_ -notlike "*Python*" } | Select-Object -First 1).Trim()

if (!$token) {
    Write-Host "Failed to get identity token" -ForegroundColor Red
    Write-Host "Output: $tokenOutput" -ForegroundColor Red
    exit 1
}

Write-Host "Token obtained successfully" -ForegroundColor Green
Write-Host "Token preview: $($token.Substring(0, [Math]::Min(20, $token.Length)))..." -ForegroundColor Gray

Write-Host "Triggering /run-page for page_id: $PageId" -ForegroundColor Cyan

$url = "https://ads-fetcher-service-hkcufhsnwq-uc.a.run.app/run-page"
$body = @{
    page_id = $PageId
} | ConvertTo-Json

Write-Host "Sending POST request..." -ForegroundColor Yellow

try {
    $headers = @{
        "Authorization" = "Bearer $token"
        "Content-Type" = "application/json"
    }
    $response = Invoke-RestMethod -Uri $url -Method Post -Headers $headers -Body $body -TimeoutSec 600

    Write-Host "`nResponse:" -ForegroundColor Green
    Write-Host "Status: $($response.status)" -ForegroundColor Cyan
    if ($response.brand_id) {
        Write-Host "Brand ID: $($response.brand_id)" -ForegroundColor White
    }
    if ($response.page_id) {
        Write-Host "Page ID: $($response.page_id)" -ForegroundColor White
    }
    Write-Host "Ads fetched: $($response.ads_fetched)" -ForegroundColor Yellow
    Write-Host "Tasks created: $($response.tasks_created)" -ForegroundColor Green
    Write-Host "Tasks skipped: $($response.tasks_skipped)" -ForegroundColor Magenta
    
    # Display logs if present
    if ($response.logs -and $response.logs.Count -gt 0) {
        Write-Host "`nFetcher Logs:" -ForegroundColor Cyan
        foreach ($logLine in $response.logs) {
            Write-Host "  $logLine" -ForegroundColor Gray
        }
    }
} catch {
    Write-Host "`nError:" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    if ($_.ErrorDetails.Message) {
        Write-Host $_.ErrorDetails.Message -ForegroundColor Red
    }
}
