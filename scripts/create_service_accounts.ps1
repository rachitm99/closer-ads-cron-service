# create_service_accounts.ps1
param(
  [string]$Project = 'closer-video-similarity',
  [string]$Region = 'us-central1'
)

gcloud config set project $Project

Write-Output "Creating runtime service account ads-fetcher-sa..."
gcloud iam service-accounts create ads-fetcher-sa --display-name "Ads Fetcher Runtime SA" --project=$Project

Write-Output "Creating invoker service account ads-fetcher-invoker..."
gcloud iam service-accounts create ads-fetcher-invoker --display-name "Ads Fetcher Invoker" --project=$Project

Write-Output "Granting Firestore (datastore.user) to runtime SA..."
gcloud projects add-iam-policy-binding $Project --member="serviceAccount:ads-fetcher-sa@$Project.iam.gserviceaccount.com" --role="roles/datastore.user"

Write-Output "(Optional) Granting Secret Manager accessor to runtime SA for secret 'rapidapi-key'..."
# Ensure the secret exists or create it in Secret Manager first
# gcloud secrets create rapidapi-key --replication-policy="automatic" --project=$Project
gcloud secrets add-iam-policy-binding rapidapi-key --member="serviceAccount:ads-fetcher-sa@$Project.iam.gserviceaccount.com" --role="roles/secretmanager.secretAccessor" --project=$Project

Write-Output "Done. Next: deploy the service then add run.invoker binding for ads-fetcher-invoker."
Write-Output "Run 'scripts\deploy_service.ps1' to build & deploy, then 'scripts\create_scheduler_job.ps1' to create the scheduler."