# Start the Flask service locally with LOCAL_DEV mode enabled
# This disables authentication for easier local testing

Write-Host "Starting Flask service locally..." -ForegroundColor Cyan
Write-Host "LOCAL_DEV mode: Enabled (authentication disabled)" -ForegroundColor Yellow
Write-Host "Server will run on: http://localhost:8080" -ForegroundColor Green
Write-Host ""
Write-Host "Press Ctrl+C to stop the server" -ForegroundColor Gray
Write-Host ""

# Set environment variable for local development
$env:LOCAL_DEV = "true"

# Change to python_app directory and run Flask
Set-Location python_app
python app.py
