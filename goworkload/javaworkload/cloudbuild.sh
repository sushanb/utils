#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Configuration
PROJECT_ID=$(gcloud config get-value project)
YAML_FILE="quickbuild.yaml"
REGION="us-central1"
REPO_NAME="bigtable-utils"
IMAGE_NAME="java-worker"  # <--- CHANGED to java-worker

FULL_IMAGE_PATH="$REGION-docker.pkg.dev/$PROJECT_ID/$REPO_NAME/$IMAGE_NAME:latest"

echo "========================================================"
echo "Submitting build to Project: $PROJECT_ID"
echo "Target Repository: $REPO_NAME ($REGION)"
echo "Image Tag: $FULL_IMAGE_PATH"
echo "========================================================"

# Check if a project is actually set
if [ -z "$PROJECT_ID" ]; then
  echo "Error: No Google Cloud project selected."
  echo "Run 'gcloud config set project [YOUR_PROJECT_ID]' first."
  exit 1
fi

# Submit the build
gcloud builds submit --config $YAML_FILE .

echo "========================================================"
echo "Build submitted successfully."
echo "Image pushed to: $FULL_IMAGE_PATH"
echo "========================================================"