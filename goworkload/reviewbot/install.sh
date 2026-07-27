#!/usr/bin/env bash
#
# End-to-end GKE install for the sushanb-review-bot.
#
# Matches the goworkload/ convention:
#   - Artifact Registry: us-central1-docker.pkg.dev/${PROJECT}/bigtable-utils/reviewbot:latest
#   - Cloud Build via reviewbot/cloudbuild.yaml (same shape as operator/cloudbuild.yaml)
#   - Target cluster: bigtable-client / europe-west1 / sbhattarai-test101 (override with env vars)
#
# Prereqs on the machine running this script:
#   - gcloud, kubectl, git
#   - `gcloud auth login` + `gcloud auth application-default login` done
#
# GitHub App prereqs (one-time, outside this script):
#   - App registered (e.g. sushanb-review-bot), installed on the target repo
#   - App ID + Installation ID + private-key .pem downloaded
#
# Usage:
#   GITHUB_APP_ID=1234567 \
#   GITHUB_APP_INSTALL_ID=89012345 \
#   GITHUB_APP_KEY=~/Downloads/sushanb-review-bot.private-key.pem \
#     ./install.sh
#
# Override defaults if needed:
#   PROJECT, CLUSTER, CLUSTER_REGION (or CLUSTER_ZONE), AR_REGION,
#   SKIP_BUILD (=1 to reuse existing image), SKIP_TEST (=1 to skip the test job).

set -euo pipefail

# ─── Defaults (match your cluster) ────────────────────────────────────────
PROJECT="${PROJECT:-sbhattarai-test101}"
CLUSTER="${CLUSTER:-bigtable-client}"
CLUSTER_REGION="${CLUSTER_REGION:-europe-west1}"
AR_REGION="${AR_REGION:-us-central1}"
AR_REPO="${AR_REPO:-bigtable-utils}"

# ─── Required vars ────────────────────────────────────────────────────────
: "${GITHUB_APP_ID:?set GITHUB_APP_ID=<numeric app id>}"
: "${GITHUB_APP_INSTALL_ID:?set GITHUB_APP_INSTALL_ID=<numeric install id>}"
: "${GITHUB_APP_KEY:?set GITHUB_APP_KEY=/path/to/private-key.pem}"

if [[ ! -f "$GITHUB_APP_KEY" ]]; then
  echo "ERROR: GITHUB_APP_KEY file does not exist: $GITHUB_APP_KEY" >&2
  exit 1
fi

# ─── Derived ──────────────────────────────────────────────────────────────
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT" --format='value(projectNumber)')
COMPUTE_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
IMAGE="${AR_REGION}-docker.pkg.dev/${PROJECT}/${AR_REPO}/reviewbot:latest"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"   # goworkload/

banner() { printf "\n\033[1;34m━━━ %s ━━━\033[0m\n" "$1"; }
note()   { printf "  \033[36m→\033[0m %s\n" "$1"; }
ok()     { printf "  \033[32m✓\033[0m %s\n" "$1"; }

banner "reviewbot install"
note "Project:   $PROJECT"
note "Cluster:   $CLUSTER ($CLUSTER_REGION)"
note "Image:     $IMAGE"
note "Compute SA: $COMPUTE_SA"
note "GitHub:    app=$GITHUB_APP_ID install=$GITHUB_APP_INSTALL_ID"

# ─── 1. Enable APIs ───────────────────────────────────────────────────────
banner "1/8  Enabling GCP APIs"
gcloud services enable \
  aiplatform.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  container.googleapis.com \
  --project="$PROJECT"
ok "APIs enabled"

# ─── 2. IAM ───────────────────────────────────────────────────────────────
banner "2/8  Granting IAM roles"
gcloud projects add-iam-policy-binding "$PROJECT" \
  --member="serviceAccount:${COMPUTE_SA}" \
  --role="roles/aiplatform.user" \
  --condition=None >/dev/null
ok "roles/aiplatform.user → compute SA (Claude via Vertex)"

gcloud projects add-iam-policy-binding "$PROJECT" \
  --member="serviceAccount:${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com" \
  --role="roles/artifactregistry.writer" \
  --condition=None >/dev/null
ok "roles/artifactregistry.writer → Cloud Build SA"

# ─── 3. Artifact Registry (reuse if exists) ───────────────────────────────
banner "3/8  Verifying Artifact Registry repo"
if gcloud artifacts repositories describe "$AR_REPO" --location="$AR_REGION" --project="$PROJECT" >/dev/null 2>&1; then
  ok "Repo '$AR_REPO' exists in $AR_REGION"
else
  note "Repo '$AR_REPO' missing — creating"
  gcloud artifacts repositories create "$AR_REPO" \
    --repository-format=docker --location="$AR_REGION" --project="$PROJECT"
  ok "Repo created"
fi

# ─── 4. Build the image ───────────────────────────────────────────────────
banner "4/8  Building container image"
if [[ -n "${SKIP_BUILD:-}" ]]; then
  ok "SKIP_BUILD set — skipping"
else
  note "Cloud Build from $REPO_ROOT (~5 min)"
  ( cd "$REPO_ROOT" && gcloud builds submit \
    --config=reviewbot/cloudbuild.yaml \
    --project="$PROJECT" . )
  ok "Image pushed: $IMAGE"
fi

# ─── 5. kubectl → cluster ────────────────────────────────────────────────
banner "5/8  Configuring kubectl"
if [[ -n "${CLUSTER_ZONE:-}" ]]; then
  gcloud container clusters get-credentials "$CLUSTER" --zone="$CLUSTER_ZONE" --project="$PROJECT"
else
  gcloud container clusters get-credentials "$CLUSTER" --region="$CLUSTER_REGION" --project="$PROJECT"
fi
kubectl cluster-info >/dev/null
ok "kubectl connected to $CLUSTER"

# ─── 6. Namespace + PVC ───────────────────────────────────────────────────
banner "6/8  Applying namespace + PVC"
kubectl apply -f "${SCRIPT_DIR}/deploy/01-pvc.yaml"

note "Waiting for PVC to bind (up to 60s)"
for i in $(seq 1 12); do
  status=$(kubectl -n reviewbot get pvc reviewbot-state -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
  if [[ "$status" == "Bound" ]]; then
    ok "PVC bound"
    break
  fi
  sleep 5
done

# ─── 7. GitHub App Secret ─────────────────────────────────────────────────
banner "7/8  Uploading GitHub App credentials"
if kubectl -n reviewbot get secret github-app >/dev/null 2>&1; then
  note "Existing 'github-app' Secret found — replacing"
  kubectl -n reviewbot delete secret github-app
fi
kubectl -n reviewbot create secret generic github-app \
  --from-literal=app-id="$GITHUB_APP_ID" \
  --from-literal=installation-id="$GITHUB_APP_INSTALL_ID" \
  --from-file=private-key.pem="$GITHUB_APP_KEY"
ok "Secret 'github-app' created"

# ─── 8. CronJob + test run ────────────────────────────────────────────────
banner "8/8  Applying CronJob"
# The manifest hardcodes the image path for sbhattarai-test101 already.
# If PROJECT != sbhattarai-test101, patch it in-place before applying.
TMP_CRON=$(mktemp -t reviewbot-cronjob.XXXXXX.yaml)
trap "rm -f $TMP_CRON" EXIT
sed "s|us-central1-docker.pkg.dev/sbhattarai-test101/bigtable-utils/reviewbot:latest|${IMAGE}|" \
  "${SCRIPT_DIR}/deploy/02-cronjob.yaml" > "$TMP_CRON"
kubectl apply -f "$TMP_CRON"
ok "CronJob 'reviewbot' installed"

if [[ -z "${SKIP_TEST:-}" ]]; then
  JOB="reviewbot-install-test-$(date +%s)"
  note "Kicking a test run: $JOB"
  kubectl -n reviewbot create job --from=cronjob/reviewbot "$JOB"
  note "Waiting for pod to be ready ..."
  kubectl -n reviewbot wait --for=condition=ready pod -l job-name="$JOB" --timeout=120s || true
  note "Streaming logs (Ctrl-C anytime — the pod finishes on its own):"
  kubectl -n reviewbot logs -f -l job-name="$JOB" --tail=200 || true
fi

# ─── Done ─────────────────────────────────────────────────────────────────
banner "Install complete"
cat <<EOF

Cluster:  $CLUSTER ($CLUSTER_REGION, $PROJECT)
Image:    $IMAGE
Schedule: every minute (concurrencyPolicy: Forbid)

Trigger the bot by posting on any PR in the configured repo:
  @sushanb-review-bot review

Watch live logs:
  kubectl -n reviewbot logs -f -l app=reviewbot --tail=200

Pause the bot:
  kubectl -n reviewbot patch cronjob reviewbot -p '{"spec":{"suspend":true}}'

Rebuild + roll a new image (matches goworkload's :latest convention;
force pods to re-pull since the tag stays the same):
  ( cd $REPO_ROOT && gcloud builds submit \\
      --config=reviewbot/cloudbuild.yaml --project=$PROJECT . )
  # CronJob has imagePullPolicy=Always so the next fire pulls the new image
  # automatically — no rollout needed.

EOF
