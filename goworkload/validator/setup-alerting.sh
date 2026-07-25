#!/bin/bash
#
# One-time setup: log-based metric + Cloud Monitoring alert policy that
# fires whenever the bigtable-validator prints "MISMATCH row=..." (see
# validator/main.go). Safe to re-run — each step skips if the resource
# already exists.

set -e

PROJECT="${PROJECT:-sbhattarai-test101}"
EMAIL="${EMAIL:-sushantsusan@google.com}"
METRIC_NAME="validation_mismatch_count"
CHANNEL_NAME="Validator alerts"
POLICY_NAME="Bigtable validator mismatch detected"

echo "Project: $PROJECT"
echo "Notify:  $EMAIL"
echo

# --- 1. Log-based counter metric ------------------------------------------
if gcloud logging metrics describe "$METRIC_NAME" --project="$PROJECT" >/dev/null 2>&1; then
  echo ">> Log-based metric '$METRIC_NAME' already exists — skipping."
else
  echo ">> Creating log-based metric '$METRIC_NAME'..."
  gcloud logging metrics create "$METRIC_NAME" \
    --project="$PROJECT" \
    --description="MISMATCH rows detected by bigtable-validator pods" \
    --log-filter='resource.type="k8s_container"
resource.labels.container_name="validator"
textPayload:"MISMATCH row="'
fi

# --- 2. Email notification channel ----------------------------------------
CHANNEL=$(gcloud beta monitoring channels list \
  --project="$PROJECT" \
  --filter="displayName=\"$CHANNEL_NAME\"" \
  --format='value(name)' | head -n1)

if [ -n "$CHANNEL" ]; then
  echo ">> Notification channel '$CHANNEL_NAME' already exists — skipping."
else
  echo ">> Creating notification channel '$CHANNEL_NAME'..."
  gcloud beta monitoring channels create \
    --project="$PROJECT" \
    --display-name="$CHANNEL_NAME" \
    --type=email \
    --channel-labels=email_address="$EMAIL"
  CHANNEL=$(gcloud beta monitoring channels list \
    --project="$PROJECT" \
    --filter="displayName=\"$CHANNEL_NAME\"" \
    --format='value(name)' | head -n1)
fi
echo "   channel: $CHANNEL"

# --- 3. Alert policy — fire on first mismatch in a 60s window -------------
EXISTING_POLICY=$(gcloud alpha monitoring policies list \
  --project="$PROJECT" \
  --filter="displayName=\"$POLICY_NAME\"" \
  --format='value(name)' | head -n1)

if [ -n "$EXISTING_POLICY" ]; then
  echo ">> Alert policy '$POLICY_NAME' already exists — skipping."
  echo "   policy: $EXISTING_POLICY"
else
  POLICY_FILE=$(mktemp)
  cat > "$POLICY_FILE" <<EOF
displayName: $POLICY_NAME
combiner: OR
conditions:
  - displayName: mismatch count > 0
    conditionThreshold:
      filter: |-
        resource.type="k8s_container"
        AND metric.type="logging.googleapis.com/user/$METRIC_NAME"
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

  echo ">> Creating alert policy '$POLICY_NAME'..."
  gcloud alpha monitoring policies create \
    --project="$PROJECT" \
    --policy-from-file="$POLICY_FILE"

  rm -f "$POLICY_FILE"
fi

echo
echo "Done. Verify with:"
echo "  gcloud logging metrics describe $METRIC_NAME --project=$PROJECT"
echo "  gcloud alpha monitoring policies list --project=$PROJECT --filter='displayName=\"$POLICY_NAME\"'"
