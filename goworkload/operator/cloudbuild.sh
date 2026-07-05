#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Set your project ID explicitly to avoid environment variable errors
PROJECT_ID="sbhattarai-test101"

echo "========================================================"
echo "Submitting build to Project: $PROJECT_ID"
echo "Target Repository: bigtable-utils (us-central1)"
echo "Image Tag: us-central1-docker.pkg.dev/$PROJECT_ID/bigtable-utils/crd-operator:v50"
echo "========================================================"

# Submit the build using the config file
gcloud builds submit --project="$PROJECT_ID" --config=cloudbuild.yaml .