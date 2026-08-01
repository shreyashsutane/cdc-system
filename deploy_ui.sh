#!/usr/bin/env bash
# ==============================================================================
# UNIFIED CLOUD RUN DEPLOYMENT SCRIPT FOR SYSTEM INTERFACE
# ==============================================================================
set -euo pipefail

# Print help if no project ID is provided
if [ $# -lt 1 ]; then
    echo "Usage: ./deploy_ui.sh [GCP_PROJECT_ID]"
    exit 1
fi

PROJECT_ID="$1"
REGION="us-central1"
SERVICE_NAME="cdc-dashboard-gateway"
REPO_NAME="cdc-registry"
IMAGE_TAG="us-central1-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/${SERVICE_NAME}:latest"

echo "======================================================================"
echo "🚀 Target GCP Project ID: ${PROJECT_ID}"
echo "📍 Deployment Region:     ${REGION}"
echo "======================================================================"

# 1. Enable required services for Cloud Build and Artifact Registry
echo "⚙️  Enabling required API services in project..."
gcloud services enable \
    artifactregistry.googleapis.com \
    cloudbuild.googleapis.com \
    run.googleapis.com \
    --project="${PROJECT_ID}"

# 2. Create Artifact Registry Repository if not exists
echo "📦 Creating Artifact Registry repository..."
gcloud artifacts repositories create "${REPO_NAME}" \
    --repository-format=docker \
    --location="${REGION}" \
    --description="Repository for CDC Streaming Dashboard Gateway" \
    --project="${PROJECT_ID}" || echo "Repository already exists, continuing."

# 3. Submit build using Cloud Build
echo "🏗️  Submitting container image build to Cloud Build..."
gcloud builds submit \
    --tag "${IMAGE_TAG}" \
    --project="${PROJECT_ID}"

# 4. Deploy the combined image to Cloud Run
echo "⚡ Deploying CDC Dashboard Gateway service to Google Cloud Run..."
gcloud run deploy "${SERVICE_NAME}" \
    --image "${IMAGE_TAG}" \
    --region "${REGION}" \
    --platform=managed \
    --allow-unauthenticated \
    --project="${PROJECT_ID}" \
    --set-env-vars="NODE_ENV=production,GCP_PROJECT=${PROJECT_ID}"

echo "======================================================================"
echo "🎉 Deployment successful!"
echo "📡 You can access the CDC dashboard from anywhere at the URL printed above."
echo "======================================================================"
