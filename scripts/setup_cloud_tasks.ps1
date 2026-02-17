# setup_cloud_tasks.ps1
# Ensures Cloud Tasks queue exists and service account has cloudtasks.enqueuer role
param(
  [string]$Project = 'closer-video-similarity',
  [string]$Location = 'us-central1',
  [string]$QueueName = 'face-processing-queue-2'
)

Write-Output "Setting project: $Project"
gcloud config set project $Project

Write-Output "Checking if queue '$QueueName' exists in $Location..."
$existingQueue = gcloud tasks queues describe $QueueName --location=$Location --project=$Project 2>$null

if ($LASTEXITCODE -eq 0) {
  Write-Output "Queue '$QueueName' already exists"
} else {
  Write-Output "Creating queue '$QueueName'..."
  gcloud tasks queues create $QueueName --location=$Location --project=$Project
  if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to create queue"
    exit 1
  }
  Write-Output "Queue created successfully"
}

Write-Output "Granting cloudtasks.enqueuer role to ads-fetcher-sa..."
gcloud projects add-iam-policy-binding $Project `
  --member="serviceAccount:ads-fetcher-sa@$Project.iam.gserviceaccount.com" `
  --role="roles/cloudtasks.enqueuer" `
  --project=$Project

Write-Output "Cloud Tasks setup complete"
