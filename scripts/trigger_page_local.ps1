# Local testing script for /run-page endpoint
# No authentication needed for local development

# $pageId = "170694943611404"
$pageId = "444025482768886"
# $brandId = "170694943611404"
$brandId = "444025482768886"

Write-Host "Triggering /run-page locally for page_id: $pageId" -ForegroundColor Cyan
Write-Host ""

$body = @{
    page_id = $pageId
    brand_id = $brandId
} | ConvertTo-Json

Write-Host "Sending POST request to http://localhost:8080/run-page..." -ForegroundColor Yellow

try {
    $response = Invoke-RestMethod -Uri "http://localhost:8080/run-page" -Method Post -Body $body -ContentType "application/json"
    
    Write-Host ""
    Write-Host "Response:" -ForegroundColor Green
    Write-Host "Status: $($response.status)"
    Write-Host "Brand ID: $($response.brand_id)"
    Write-Host "Page ID: $($response.page_id)"
    Write-Host "Ads fetched: $($response.ads_fetched)"
    Write-Host "Tasks created: $($response.tasks_created)"
    Write-Host "Tasks skipped: $($response.tasks_skipped)"
    
    if ($response.logs -and $response.logs.Count -gt 0) {
        Write-Host ""
        Write-Host "Fetcher Logs:" -ForegroundColor Cyan
        foreach ($log in $response.logs) {
            Write-Host "  $log"
        }
    }
    
    if ($response.raw_responses -and $response.raw_responses.Count -gt 0) {
        Write-Host ""
        Write-Host "Raw Responses: $($response.raw_responses.Count) API calls" -ForegroundColor Magenta
    }
}
catch {
    Write-Host ""
    Write-Host "Error:" -ForegroundColor Red
    Write-Host $_.Exception.Message
    if ($_.ErrorDetails.Message) {
        Write-Host $_.ErrorDetails.Message
    }
}
