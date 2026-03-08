# Local testing script for /run-page endpoint
# No authentication needed for local development

# $pageId = "170694943611404"
$pageId = "107959920549143"
# $brandId = "170694943611404"
$brandId = "107959920549143"

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
    Write-Host "Status:        $($response.status)"
    Write-Host "Brand ID:      $($response.brand_id)"
    Write-Host "Page ID:       $($response.page_id)"
    Write-Host "Ads fetched:   $($response.ads_fetched)"
    Write-Host "Tasks created: $($response.tasks_created)"
    Write-Host "Tasks skipped: $($response.tasks_skipped)"
}
catch {
    Write-Host ""
    Write-Host "Error:" -ForegroundColor Red
    Write-Host $_.Exception.Message
    if ($_.ErrorDetails.Message) {
        Write-Host $_.ErrorDetails.Message
    }
}
