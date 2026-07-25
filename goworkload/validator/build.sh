#!/bin/bash

export PROJECT_ID="sbhattarai-test101"
export REGION="us-central1"
export REPO_NAME="bigtable-utils"
export IMAGE_NAME="bigtable-validator"
export TAG=$(git rev-parse --short HEAD)

export IMAGE_URL="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/${IMAGE_NAME}:${TAG}"

echo "Building image: $IMAGE_URL"
docker build --platform linux/amd64 -t $IMAGE_URL .

echo "Pushing image to Artifact Registry..."
docker push $IMAGE_URL

echo "Done! Image available at: $IMAGE_URL"
