# reviewbot — @sushanb-review-bot on GKE

Polls PR comments on `sushanb/google-cloud-go`. When someone (or the PR
author) writes `@sushanb-review-bot review`, the bot checks out the PR
into a container and runs three Claude subagents against the diff:

- `session-reviewer` — behavioral spec compliance
- `session-component-review` — layer/boundary compliance
- `igor-reviewer` — persona review

Consolidated findings are posted back as inline review comments plus one
top-level summary comment.

## Design summary

| | |
|---|---|
| Trigger | Poll `GET /repos/{repo}/issues/comments?since=T` every minute (CronJob). Filter body for `@sushanb-review-bot review`. |
| Compute | GKE, one pod per CronJob fire (`concurrencyPolicy: Forbid`). |
| GitHub auth | GitHub App installation token minted per-run from a mounted `.pem`. Exported as `GH_TOKEN` for `gh` / `git`. |
| Anthropic auth | **None.** `claude` uses Vertex AI via GCE metadata → ADC → pod's compute SA (`roles/aiplatform.user`). |
| State | PVC (10Gi). Holds `cursor.json` + per-PR git worktrees for reuse. |
| Cost | ~$2–3/mo GKE infra + Vertex per-token per triggered review. No secrets rotation for Anthropic. |

## One-time GCP setup

```bash
# 1. Enable APIs.
gcloud services enable aiplatform.googleapis.com artifactregistry.googleapis.com \
  cloudbuild.googleapis.com container.googleapis.com

# 2. Give the pod's compute SA Vertex access.
PROJECT_NUMBER=$(gcloud projects describe $PROJECT --format='value(projectNumber)')
gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --role="roles/aiplatform.user"

# 3. Artifact Registry repo for the image.
gcloud artifacts repositories create reviewbot \
  --repository-format=docker --location=us-central1

# 4. Give Cloud Build's SA push access.
gcloud artifacts repositories add-iam-policy-binding reviewbot \
  --location=us-central1 \
  --member="serviceAccount:${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com" \
  --role="roles/artifactregistry.writer"
```

## GitHub App setup (one-time)

Follow the "Option B" outline you did earlier:

1. `github.com/settings/apps/new` — name `sushanb-review-bot`, homepage
   `https://github.com/sushanb/google-cloud-go`, webhook OFF.
2. Permissions: `Contents: Read & write`, `Pull requests: Read & write`,
   `Metadata: Read`. Nothing else.
3. Generate private key → downloads `sushanb-review-bot.YYYY-MM-DD.private-key.pem`.
4. Install the App on `sushanb/google-cloud-go` from
   `github.com/apps/sushanb-review-bot`.
5. Note the **App ID** (visible on the App settings page) and the
   **Installation ID** (visible in the install URL:
   `github.com/settings/installations/<INSTALLATION_ID>`).

## Bundle the reviewer agents into the image

The image bakes in the `.claude/agents/` reviewer definitions. Spec files
are **not** bundled — they live in the target repo at `bigtable/docs/specs/`
([source](https://github.com/googleapis/google-cloud-go/tree/main/bigtable/docs/specs))
and are read from the PR checkout at review time. This ensures the reviewer
always sees the PR's version of the spec.

```bash
# From goworkload/ — bundle the three agent definitions.
cp /home/sushantsusan_google_com/.claude/agents/igor-reviewer.md \
   reviewbot/agents/
cp /home/sushantsusan_google_com/google-cloud-go/.claude/agents/{session-reviewer,session-component-review}.md \
   reviewbot/agents/
```

The `session-reviewer.md` and `session-component-review.md` copies in
`reviewbot/agents/` have been edited to point at `bigtable/docs/specs/`
instead of repo root. If you re-copy from the source (e.g. after editing
the canonical versions on sessionz), re-apply that path fix — or run:

```bash
sed -i 's|`SESSION_COMPONENT_SPEC\.md`|`bigtable/docs/specs/SESSION_COMPONENT_SPEC.md`|g; \
        s|`SESSION_SPEC\.md`|`bigtable/docs/specs/SESSION_SPEC.md`|g; \
        s|`SESSION_CLIENT_SPEC\.md`|`bigtable/docs/specs/SESSION_CLIENT_SPEC.md`|g; \
        s|`SESSION_POOL_SPEC\.md`|`bigtable/docs/specs/SESSION_POOL_SPEC.md`|g; \
        s|`CLIENT_SIDE_METRICS_SPEC\.md`|`bigtable/docs/specs/CLIENT_SIDE_METRICS_SPEC.md`|g' \
   reviewbot/agents/{session-reviewer,session-component-review}.md
```

Any time you edit an agent locally, rebuild the image before the change is
live in the bot. (An improvement would be to mount these from a ConfigMap;
skip for now.)

## Build the image

```bash
# From goworkload/ (must run from Go module root):
gcloud builds submit --config=reviewbot/cloudbuild.yaml \
  --substitutions=_REGION=us-central1,_REPO=reviewbot,_TAG=$(git rev-parse --short HEAD) .
```

Successful run pushes:
- `us-central1-docker.pkg.dev/PROJECT/reviewbot/reviewbot:<git-sha>`
- `us-central1-docker.pkg.dev/PROJECT/reviewbot/reviewbot:latest`

Update `reviewbot/deploy/02-cronjob.yaml` `image:` field with your project's
full path.

## Deploy to GKE

```bash
# 1. Namespace + PVC.
kubectl apply -f reviewbot/deploy/01-pvc.yaml

# 2. GitHub App secret. Use kubectl create (skip 00-secret.yaml if you
#    don't want plaintext keys in YAML).
kubectl -n reviewbot create secret generic github-app \
  --from-literal=app-id=<APP_ID> \
  --from-literal=installation-id=<INSTALLATION_ID> \
  --from-file=private-key.pem=./sushanb-review-bot.YYYY-MM-DD.private-key.pem

# 3. CronJob.
kubectl apply -f reviewbot/deploy/02-cronjob.yaml
```

## Verify

```bash
# Trigger a manual test run (no need to wait for the schedule).
kubectl -n reviewbot create job --from=cronjob/reviewbot reviewbot-test-$(date +%s)

# Watch logs.
kubectl -n reviewbot logs -f -l app=reviewbot --tail=100
```

Expected log shape on an idle run:
```
boot: repo=sushanb/google-cloud-go trigger="@sushanb-review-bot review" ...
poll: 0 new comment(s) since 2026-07-27T21:00:00Z
done: processed=0
```

Then to end-to-end test: on any open PR in the fork, post the comment
`@sushanb-review-bot review`. Within 1 minute the next CronJob fire
picks it up. Watch logs for:
```
trigger: comment=... PR=#... by=sushanb
claude: invoking on PR #... (wt=/var/lib/reviewbot/wt/pr-...)
DONE: reviewers=3 findings_raw=... findings_posted=... verdict=...
```

## Troubleshooting

| Symptom | Cause |
|---|---|
| `metadata /computeMetadata/v1/project/project-id: HTTP 404` | Not running on GCE/GKE. Set `GCP_PROJECT` env explicitly. |
| Claude 401 or auth error | Compute SA missing `roles/aiplatform.user`. See step 2 above. |
| `gh: authentication error` | GitHub App install lost its permissions, or you rotated the private key without updating the Secret. |
| `git fetch origin: authentication failed` | Same as above — the token is scoped by the App install. |
| Comment posted twice on same trigger | Cursor didn't advance (previous run crashed after posting). Manually bump `cursor.json` on the PVC or accept the duplicate. |
| Bot doesn't respond to my `@mention` | Comment must be on a PR (not an issue), and comment body must contain the exact trigger string. Case-sensitive. |

## Cost knobs

- Bump `REVIEWBOT_MAX_PER_RUN` to process more triggers per minute (default 3).
- Lower `REVIEWBOT_MODEL` to `claude-sonnet-5` or `claude-haiku-4-5` for
  cheaper reviews. Model quality matters most on the boundary/spec agents;
  `igor-reviewer` is style-heavy and works fine on Sonnet.
- Change `schedule: "* * * * *"` to `"*/5 * * * *"` for 5-min latency if you
  want fewer wake-ups.
- Add `startingDeadlineSeconds` cap if the CronJob queue gets stuck.

## What's NOT in this cut

Deferred to follow-up iterations:

- **Auto-commit safe fixes** (goimports, obvious renames). Comment-only for
  now — safer while the bot is new.
- **Mode label** (`bot:skip-review`) to disable review on WIP PRs.
- **Multi-repo support.** Currently pinned to one `REVIEWBOT_REPO`.
- **Webhook trigger** for lower latency. CronJob poll is simpler for a
  personal bot; add a webhook receiver if minute-latency isn't enough.
- **Secret Manager CSI** for private-key rotation without pod restart. K8s
  Secret is fine for now.
- **ConfigMap-mounted agents/specs** for hot-reload. Bake into image for
  now — determinism > convenience during rollout.
