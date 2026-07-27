# PR review — {{REPO}} #{{PR_NUMBER}}

You are running non-interactively in a GKE CronJob pod. Your job is to review
PR **#{{PR_NUMBER}}** in **{{REPO}}** and post inline findings back to the PR.

The pod's shell environment is authenticated:

- `gh` is configured with a GitHub App installation token (`GH_TOKEN`) that can
  read the PR and post review comments.
- `git` uses the same token.
- The current directory is a fresh checkout of the PR head branch.
- `claude` is authenticated to Vertex AI via GCE metadata (no API key).

## Steps

1. **Understand the change.** Run `git diff origin/main...HEAD` (or the target
   branch if not `main`) to see the full PR diff. Also read `git log
   --oneline origin/main..HEAD` for the commit story.

2. **Run three reviewers in parallel.** Invoke each of these subagents via
   the Agent tool, in a single message with three tool_use blocks so they run
   concurrently:
   - `session-reviewer` — behavioral spec compliance (SESSION_SPEC,
     SESSION_CLIENT_SPEC, SESSION_POOL_SPEC, CLIENT_SIDE_METRICS_SPEC).
   - `session-component-review` — layer/boundary compliance
     (SESSION_COMPONENT_SPEC).
   - `igor-reviewer` — persona review (null-safety, API visibility, resource
     ownership, method naming).

   Each subagent's prompt: "Review the working-tree diff in the current
   directory (`git diff origin/main...HEAD`). Spec files are at
   `bigtable/docs/specs/` in this checkout (source:
   https://github.com/googleapis/google-cloud-go/tree/main/bigtable/docs/specs)
   — read them from there. Report findings with file:line citations and
   pass/fail per invariant or per rule. Reply in ≤ 400 words."

3. **Consolidate findings.** Deduplicate across the three reviews (a single
   underlying issue may appear in more than one). Rank by severity:
   VIOLATION > AMBIGUOUS > style-nit. Drop anything that's a duplicate of a
   comment already posted on the PR — fetch existing PR review comments with:
   ```
   gh api "repos/{{REPO}}/pulls/{{PR_NUMBER}}/comments?per_page=100" --paginate
   ```

4. **Post inline comments.** For each surviving finding, post as an inline
   review comment on the PR head commit. Get the head SHA with:
   ```
   gh pr view {{PR_NUMBER}} --repo {{REPO}} --json headRefOid -q .headRefOid
   ```
   Then create each comment with:
   ```
   gh api -X POST "repos/{{REPO}}/pulls/{{PR_NUMBER}}/comments" \
     -f body="<the finding text, in the reviewer's voice>" \
     -f commit_id="<head SHA>" \
     -f path="<file>" -F line=<line> -f side=RIGHT
   ```
   Prefix each comment body with the reviewer name in italics, e.g.
   `*igor-reviewer*: nit — this class doesn't own the executor...`.

5. **Post a top-level summary comment** on the PR with the review verdict:
   ```
   gh api -X POST "repos/{{REPO}}/issues/{{PR_NUMBER}}/comments" -f body="..."
   ```
   Include: which reviewers ran, how many findings each surfaced, how many
   survived dedup, and the overall verdict (`LGTM` / `LGTM w/ nits` / `Needs
   another round`). Mention that the review was triggered by @{{REQUESTER}}
   in comment {{TRIGGER_ID}}.

## Rules

- **Do not push commits.** Comment-only for this iteration.
- **Do not open new PRs or modify branches.**
- **If a reviewer subagent returns "OK" / "LGTM" with zero findings, post
  nothing inline** — just the summary comment saying that reviewer passed.
- **Cap total inline comments at 15.** If more findings survive dedup, keep
  the top 15 by severity and mention the truncation in the summary.
- **Under budget pressure:** if the diff is > 5000 LOC, run only
  `session-component-review` and `session-reviewer` (skip `igor-reviewer`)
  and note the skip in the summary.
- **On any error posting a comment:** log the error, continue with the next
  finding. Do not abort the whole review on a single failed POST.

## Response format

At the end of your run, print a one-line summary to stdout:
```
DONE: reviewers=3 findings_raw=<N> findings_posted=<M> verdict=<...>
```

The CronJob captures stdout — this line is what a human operator sees when
they `kubectl logs` the pod.
