#!/bin/bash

# export PROJECT_ID="your-project-id"
# export REGION="us-central1" # Or your preferred region

# # 2. Enable the Artifact Registry API
# gcloud services enable artifactregistry.googleapis.com

# # 3. Create a Docker repository named 'my-repo'
# gcloud artifacts repositories create my-repo \
#     --repository-format=docker \
#     --location=$REGION \
#     --description="Docker repository for Bigtable worker"

# # 4. Configure Docker to authenticate with Google Cloud
# gcloud auth configure-docker ${REGION}-docker.pkg.dev

export PROJECT_ID="sbhattarai-test101"
export REGION="us-central1"
export REPO_NAME="bigtable-utils"
export IMAGE_NAME="bigtable-worker"
export TAG=$(git rev-parse --short HEAD)

# --- AUTOMATION ---
# Construct the full image URL
export IMAGE_URL="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/${IMAGE_NAME}:${TAG}"

echo "Building image: $IMAGE_URL"
docker build --platform linux/amd64 -t $IMAGE_URL .

echo "Pushing image to Artifact Registry..."
docker push $IMAGE_URL

echo "Done! Image available at: $IMAGE_URL"