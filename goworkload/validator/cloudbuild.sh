#!/bin/bash

set -e

PROJECT_ID=$(gcloud config get-value project)
YAML_FILE="quickbuild.yaml"
REGION="us-central1"
REPO_NAME="bigtable-utils"
IMAGE_NAME="bigtable-validator"

FULL_IMAGE_PATH="$REGION-docker.pkg.dev/$PROJECT_ID/$REPO_NAME/$IMAGE_NAME:latest"

echo "========================================================"
echo "Submitting build to Project: $PROJECT_ID"
echo "Target Repository: $REPO_NAME ($REGION)"
echo "Image Tag: $FULL_IMAGE_PATH"
echo "========================================================"

if [ -z "$PROJECT_ID" ]; then
  echo "Error: No Google Cloud project selected."
  echo "Run 'gcloud config set project [YOUR_PROJECT_ID]' first."
  exit 1
fi

# Resolve the local replace directive into ./vendor so Cloud Build
# doesn't need access to the host path from go.mod. Tidy first so
# vendor/modules.txt matches the actually-imported set (Go 1.14+ vendor
# mode rejects any drift).
echo "Vendoring dependencies..."
go mod tidy
go mod vendor

gcloud builds submit --config $YAML_FILE .

echo "========================================================"
echo "Build submitted successfully."
echo "Image pushed to: $FULL_IMAGE_PATH"
echo "========================================================"
