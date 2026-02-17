# Test single page endpoint with a specific page_id
param(
    [string]$PageId = "444025482768886"  # Change this to test different brands
)

Write-Host "Testing /run-page endpoint with page_id: $PageId" -ForegroundColor Cyan
Write-Host "Getting identity token..." -ForegroundColor Yellow

$token = gcloud auth print-identity-token --impersonate-service-account=ads-fetcher-invoker@closer-video-similarity.iam.gserviceaccount.com

if (!$token) {
    Write-Host "Failed to get identity token" -ForegroundColor Red
    exit 1
}

$url = "https://ads-fetcher-service-hkcufhsnwq-uc.a.run.app/run-page"
$body = @{
    page_id = $PageId
} | ConvertTo-Json

Write-Host "`nSending request to $url" -ForegroundColor Yellow
Write-Host "Body: $body" -ForegroundColor Gray

try {
    $response = Invoke-RestMethod -Uri $url -Method Post -Headers @{
        "Authorization" = "Bearer $token"
        "Content-Type" = "application/json"
    } -Body $body -TimeoutSec 600 -Verbose

    Write-Host "`n=== RESPONSE ===" -ForegroundColor Green
    Write-Host "Status: $($response.status)" -ForegroundColor Cyan
    
    if ($response.brand_id) {
        Write-Host "Brand ID: $($response.brand_id)" -ForegroundColor White
    }
    if ($response.page_id) {
        Write-Host "Page ID: $($response.page_id)" -ForegroundColor White
    }
    if ($response.error) {
        Write-Host "Error: $($response.error)" -ForegroundColor Red
    }
    
    Write-Host "`nResults:" -ForegroundColor Yellow
    Write-Host "  Ads fetched: $($response.ads_fetched)" -ForegroundColor $(if($response.ads_fetched -gt 0){"Green"}else{"Yellow"})
    Write-Host "  Tasks created: $($response.tasks_created)" -ForegroundColor $(if($response.tasks_created -gt 0){"Green"}else{"Yellow"})
    Write-Host "  Tasks skipped: $($response.tasks_skipped)" -ForegroundColor Magenta
    
} catch {
    Write-Host "`n=== ERROR ===" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    if ($_.ErrorDetails.Message) {
        Write-Host "`nDetails:" -ForegroundColor Yellow
        Write-Host $_.ErrorDetails.Message -ForegroundColor Red
    }
    
    Write-Host "`nTo view detailed logs, run:" -ForegroundColor Yellow
    Write-Host "gcloud logging read 'resource.type=cloud_run_revision AND resource.labels.service_name=ads-fetcher-service' --limit 50 --format=json" -ForegroundColor Gray
}
