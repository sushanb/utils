#!/bin/bash
#
# One-time setup: log-based metric + Cloud Monitoring alert policy that
# fires whenever the bigtable-validator prints "MISMATCH row=..." (see
# validator/main.go).
#
# Run this ONCE per project. Rerunning will fail on the already-created
# resources; that's expected — delete them first with:
#   gcloud logging metrics delete validation_mismatch_count --project=$PROJECT
#   gcloud alpha monitoring policies delete <policy-name> --project=$PROJECT
#   gcloud beta monitoring channels delete <channel-name> --project=$PROJECT

set -e

PROJECT="${PROJECT:-sbhattarai-test101}"
EMAIL="${EMAIL:-sushantsusan@google.com}"

echo "Project: $PROJECT"
echo "Notify:  $EMAIL"
echo

# --- 1. Log-based counter metric ------------------------------------------
echo ">> Creating log-based metric 'validation_mismatch_count'..."
gcloud logging metrics create validation_mismatch_count \
  --project="$PROJECT" \
  --description="MISMATCH rows detected by bigtable-validator pods" \
  --log-filter='resource.type="k8s_container"
resource.labels.container_name="validator"
textPayload:"MISMATCH row="'

# --- 2. Email notification channel ----------------------------------------
echo ">> Creating notification channel..."
gcloud beta monitoring channels create \
  --project="$PROJECT" \
  --display-name="Validator alerts" \
  --type=email \
  --channel-labels=email_address="$EMAIL"

CHANNEL=$(gcloud beta monitoring channels list \
  --project="$PROJECT" \
  --filter='displayName="Validator alerts"' \
  --format='value(name)')
echo "   channel: $CHANNEL"

# --- 3. Alert policy — fire on first mismatch in a 60s window -------------
POLICY_FILE=$(mktemp)
cat > "$POLICY_FILE" <<EOF
displayName: Bigtable validator mismatch detected
combiner: OR
conditions:
  - displayName: mismatch count > 0
    conditionThreshold:
      filter: |-
        resource.type="k8s_container"
        AND metric.type="logging.googleapis.com/user/validation_mismatch_count"
      aggregations:
        - alignmentPeriod: 60s
          perSeriesAligner: ALIGN_SUM
          crossSeriesReducer: REDUCE_SUM
      comparison: COMPARISON_GT
      thresholdValue: 0
      duration: 0s
      trigger:
        count: 1
notificationChannels:
  - $CHANNEL
alertStrategy:
  autoClose: 3600s
EOF

echo ">> Creating alert policy..."
gcloud alpha monitoring policies create \
  --project="$PROJECT" \
  --policy-from-file="$POLICY_FILE"

rm -f "$POLICY_FILE"

echo
echo "Done. Verify with:"
echo "  gcloud logging metrics describe validation_mismatch_count --project=$PROJECT"
echo "  gcloud alpha monitoring policies list --project=$PROJECT --filter='displayName=\"Bigtable validator mismatch detected\"'"
