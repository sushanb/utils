#!/bin/bash

export PROJECT_ID="sbhattarai-test101"
export REGION="us-central1"
export REPO_NAME="bigtable-utils"
export IMAGE_NAME="bigtable-validator"
export TAG=$(git rev-parse --short HEAD)

export IMAGE_URL="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/${IMAGE_NAME}:${TAG}"

# Resolve the local replace directive into ./vendor so the Docker build
# doesn't need access to the host path from go.mod. Tidy first so
# vendor/modules.txt matches the actually-imported set (Go 1.14+ vendor
# mode rejects any drift).
echo "Vendoring dependencies..."
go mod tidy
go mod vendor

echo "Building image: $IMAGE_URL"
docker build --platform linux/amd64 -t $IMAGE_URL .

echo "Pushing image to Artifact Registry..."
docker push $IMAGE_URL

echo "Done! Image available at: $IMAGE_URL"
