# Local testing script for /run endpoint (trigger the whole job now)
# No authentication needed for local development (set LOCAL_DEV=true when running the server)

Write-Host "Triggering /run locally (full cron)" -ForegroundColor Cyan
Write-Host ""

Write-Host "Sending POST request to http://localhost:8080/run..." -ForegroundColor Yellow

try {
    $response = Invoke-RestMethod -Uri "http://localhost:8080/run" -Method Post -Body "{}" -ContentType "application/json"

    Write-Host ""
    Write-Host "Response:" -ForegroundColor Green
    Write-Host "Status:           $($response.status)"
    Write-Host "Brands processed: $($response.brands_processed)"
    Write-Host "Brands failed:    $($response.brands_failed)"
    Write-Host "Ads fetched:      $($response.ads_fetched)"
    Write-Host "Tasks created:    $($response.tasks_created)"
    Write-Host "Tasks skipped:    $($response.tasks_skipped)"
}
catch {
    Write-Host ""
    Write-Host "Error:" -ForegroundColor Red
    Write-Host $_.Exception.Message
    if ($_.ErrorDetails.Message) {
        Write-Host $_.ErrorDetails.Message
    }
}