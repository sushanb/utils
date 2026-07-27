// Package main runs the sushanb PR reviewer bot as a GKE CronJob.
//
// Every fire (~1 min), it:
//  1. Mints a GitHub App installation token.
//  2. Fetches issue comments on the target repo created since the last poll.
//  3. Filters for the configured trigger phrase.
//  4. For each match: checks out the PR, invokes `claude` (via Vertex AI) to
//     run the three configured reviewer subagents, and lets claude post its
//     findings back as inline PR review comments (using the same App token
//     exported as GH_TOKEN).
//  5. Advances the last-seen-comment-id cursor on the PVC.
//
// No Anthropic API key — claude authenticates to Vertex via GCE metadata
// (Application Default Credentials) using the pod's compute service account.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/bradleyfalzon/ghinstallation/v2"
	"github.com/google/go-github/v66/github"
)

type config struct {
	appID          int64
	installationID int64
	privateKeyPath string
	repo           string // "owner/repo"
	trigger        string // e.g. "@sushanb-robot review"
	stateDir       string // PVC mount
	agentsDir      string // baked into image (also HOME/.claude/agents)
	promptFile     string // baked prompt template
	gcpProject     string // Vertex project
	gcpRegion      string // Vertex region ("global" or "us-east5" etc.)
	model          string // e.g. "claude-opus-5"
	maxPerRun      int    // cap comments processed per invocation
	lookbackHours  int    // on cold start, how far back to scan
}

func loadConfig() (*config, error) {
	c := &config{
		repo:           envOr("REVIEWBOT_REPO", "sushanb/google-cloud-go"),
		trigger:        envOr("REVIEWBOT_TRIGGER", "@sushanb-robot review"),
		stateDir:       envOr("REVIEWBOT_STATE_DIR", "/var/lib/reviewbot"),
		agentsDir:      envOr("REVIEWBOT_AGENTS_DIR", "/opt/reviewbot/agents"),
		promptFile:     envOr("REVIEWBOT_PROMPT_FILE", "/opt/reviewbot/prompts/review.md"),
		gcpProject:     os.Getenv("GCP_PROJECT"),
		gcpRegion:      envOr("GCP_REGION", "global"),
		model:          envOr("REVIEWBOT_MODEL", "claude-opus-5"),
		maxPerRun:      envInt("REVIEWBOT_MAX_PER_RUN", 3),
		lookbackHours:  envInt("REVIEWBOT_LOOKBACK_HOURS", 24),
		privateKeyPath: envOr("GITHUB_APP_PRIVATE_KEY", "/var/secrets/github-app/private-key.pem"),
	}

	var err error
	c.appID, err = envInt64("GITHUB_APP_ID", 0)
	if err != nil || c.appID == 0 {
		return nil, fmt.Errorf("GITHUB_APP_ID required: %v", err)
	}
	c.installationID, err = envInt64("GITHUB_APP_INSTALLATION_ID", 0)
	if err != nil || c.installationID == 0 {
		return nil, fmt.Errorf("GITHUB_APP_INSTALLATION_ID required: %v", err)
	}

	if c.gcpProject == "" {
		if p, err := metadataGet("project/project-id"); err == nil {
			c.gcpProject = p
		} else {
			return nil, fmt.Errorf("GCP_PROJECT unset and metadata lookup failed: %v", err)
		}
	}
	return c, nil
}

func main() {
	oneShot := flag.Bool("one-shot", false, "process one triggered comment then exit (test mode)")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.LUTC)
	ctx := context.Background()

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	log.Printf("boot: repo=%s trigger=%q model=%s vertex=%s/%s",
		cfg.repo, cfg.trigger, cfg.model, cfg.gcpProject, cfg.gcpRegion)

	tr, err := ghinstallation.NewKeyFromFile(http.DefaultTransport,
		cfg.appID, cfg.installationID, cfg.privateKeyPath)
	if err != nil {
		log.Fatalf("github app: %v", err)
	}
	gh := github.NewClient(&http.Client{Transport: tr})

	comments, since, err := fetchNewMentions(ctx, gh, cfg)
	if err != nil {
		log.Fatalf("fetch mentions: %v", err)
	}
	log.Printf("poll: %d new comment(s) since %s", len(comments), since.Format(time.RFC3339))

	if len(comments) == 0 {
		return
	}

	processed := 0
	for _, cm := range comments {
		if processed >= cfg.maxPerRun {
			log.Printf("cap reached (%d) — remaining comments will process next run", cfg.maxPerRun)
			break
		}
		if err := handleTrigger(ctx, cfg, tr, cm); err != nil {
			log.Printf("comment %d: FAILED: %v", cm.GetID(), err)
			// Don't advance cursor past a failure — retry next run.
			continue
		}
		if err := saveCursor(cfg.stateDir, cm.GetID(), cm.GetCreatedAt().Time); err != nil {
			log.Printf("cursor save: %v", err)
		}
		processed++
		if *oneShot {
			break
		}
	}
	log.Printf("done: processed=%d", processed)
}

// ─── GitHub polling ────────────────────────────────────────────────────────

func fetchNewMentions(ctx context.Context, gh *github.Client, cfg *config) ([]*github.IssueComment, time.Time, error) {
	since, err := loadCursor(cfg.stateDir, cfg.lookbackHours)
	if err != nil {
		return nil, since, err
	}
	owner, repo, _ := strings.Cut(cfg.repo, "/")
	if owner == "" || repo == "" {
		return nil, since, fmt.Errorf("bad repo %q, want owner/repo", cfg.repo)
	}

	var out []*github.IssueComment
	opts := &github.IssueListCommentsOptions{
		Sort:        github.String("created"),
		Direction:   github.String("asc"),
		Since:       &since,
		ListOptions: github.ListOptions{PerPage: 100},
	}
	for {
		page, resp, err := gh.Issues.ListComments(ctx, owner, repo, 0, opts)
		if err != nil {
			return nil, since, err
		}
		for _, c := range page {
			if c.Body == nil || !strings.Contains(*c.Body, cfg.trigger) {
				continue
			}
			// Only PR comments — not issue comments. IssueURL suffix distinguishes:
			// PRs: .../pulls/N/comments (issue_url points at /issues/N still, so
			// we check html_url which reliably contains /pull/).
			if !strings.Contains(c.GetHTMLURL(), "/pull/") {
				continue
			}
			out = append(out, c)
		}
		if resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}
	return out, since, nil
}

// ─── Per-comment handling ─────────────────────────────────────────────────

func handleTrigger(ctx context.Context, cfg *config, tr *ghinstallation.Transport, cm *github.IssueComment) error {
	prNum, err := extractPRNumber(cm.GetHTMLURL())
	if err != nil {
		return fmt.Errorf("extract PR number: %w", err)
	}
	log.Printf("trigger: comment=%d PR=#%d by=%s", cm.GetID(), prNum, cm.GetUser().GetLogin())

	// Mint a fresh installation token for the shell tools (gh/git/claude).
	token, err := tr.Token(ctx)
	if err != nil {
		return fmt.Errorf("mint token: %w", err)
	}

	wt := filepath.Join(cfg.stateDir, "wt", fmt.Sprintf("pr-%d", prNum))
	if err := ensureWorktree(ctx, cfg, wt, prNum, token); err != nil {
		return fmt.Errorf("worktree: %w", err)
	}

	if err := runClaude(ctx, cfg, wt, prNum, token, cm); err != nil {
		return fmt.Errorf("claude: %w", err)
	}
	return nil
}

func ensureWorktree(ctx context.Context, cfg *config, wt string, prNum int, token string) error {
	if _, err := os.Stat(wt); err != nil {
		if err := os.MkdirAll(filepath.Dir(wt), 0o755); err != nil {
			return err
		}
		// gh pr checkout requires a repo. Start with a fresh clone.
		if err := run(ctx, nil, filepath.Dir(wt), map[string]string{"GH_TOKEN": token},
			"gh", "repo", "clone", cfg.repo, wt, "--", "--depth=200"); err != nil {
			return err
		}
	}
	// Fetch + check out the PR head (works for same-repo and forked-repo PRs).
	env := map[string]string{"GH_TOKEN": token}
	if err := run(ctx, nil, wt, env, "git", "fetch", "origin", "--prune"); err != nil {
		return err
	}
	if err := run(ctx, nil, wt, env, "gh", "pr", "checkout", strconv.Itoa(prNum), "--force"); err != nil {
		return err
	}
	return nil
}

func runClaude(ctx context.Context, cfg *config, wt string, prNum int, token string, cm *github.IssueComment) error {
	prompt, err := renderPrompt(cfg, prNum, cm)
	if err != nil {
		return fmt.Errorf("render prompt: %w", err)
	}
	promptPath := filepath.Join(cfg.stateDir, fmt.Sprintf("prompt-pr%d.md", prNum))
	if err := os.WriteFile(promptPath, []byte(prompt), 0o644); err != nil {
		return err
	}

	// claude picks up agents from $HOME/.claude/agents; point HOME at the bundle.
	env := map[string]string{
		"HOME":                       "/opt/reviewbot",
		"GH_TOKEN":                   token,
		"CLAUDE_CODE_USE_VERTEX":     "1",
		"ANTHROPIC_VERTEX_PROJECT_ID": cfg.gcpProject,
		"CLOUD_ML_REGION":            cfg.gcpRegion,
		"ANTHROPIC_MODEL":            cfg.model,
	}
	cctx, cancel := context.WithTimeout(ctx, 25*time.Minute)
	defer cancel()
	log.Printf("claude: invoking on PR #%d (wt=%s)", prNum, wt)
	return run(cctx, os.Stdout, wt, env,
		"claude", "--dangerously-skip-permissions", "-p", "@"+promptPath)
}

func renderPrompt(cfg *config, prNum int, cm *github.IssueComment) (string, error) {
	tpl, err := os.ReadFile(cfg.promptFile)
	if err != nil {
		return "", err
	}
	s := string(tpl)
	repl := map[string]string{
		"{{REPO}}":       cfg.repo,
		"{{PR_NUMBER}}":  strconv.Itoa(prNum),
		"{{TRIGGER_ID}}": strconv.FormatInt(cm.GetID(), 10),
		"{{REQUESTER}}":  cm.GetUser().GetLogin(),
	}
	for k, v := range repl {
		s = strings.ReplaceAll(s, k, v)
	}
	return s, nil
}

// ─── State cursor ─────────────────────────────────────────────────────────

type cursor struct {
	LastCommentID int64     `json:"last_comment_id"`
	LastCreatedAt time.Time `json:"last_created_at"`
}

func cursorPath(dir string) string { return filepath.Join(dir, "cursor.json") }

func loadCursor(dir string, fallbackHours int) (time.Time, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return time.Time{}, err
	}
	b, err := os.ReadFile(cursorPath(dir))
	if errors.Is(err, os.ErrNotExist) {
		return time.Now().UTC().Add(-time.Duration(fallbackHours) * time.Hour), nil
	}
	if err != nil {
		return time.Time{}, err
	}
	var c cursor
	if err := json.Unmarshal(b, &c); err != nil {
		return time.Time{}, err
	}
	// +1s so we don't re-see the boundary comment.
	return c.LastCreatedAt.Add(time.Second), nil
}

func saveCursor(dir string, id int64, ts time.Time) error {
	b, err := json.MarshalIndent(cursor{LastCommentID: id, LastCreatedAt: ts.UTC()}, "", "  ")
	if err != nil {
		return err
	}
	tmp := cursorPath(dir) + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, cursorPath(dir))
}

// ─── Small helpers ─────────────────────────────────────────────────────────

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envInt64(k string, def int64) (int64, error) {
	if v := os.Getenv(k); v != "" {
		return strconv.ParseInt(v, 10, 64)
	}
	return def, nil
}

func extractPRNumber(htmlURL string) (int, error) {
	// e.g. https://github.com/owner/repo/pull/20225#issuecomment-...
	i := strings.Index(htmlURL, "/pull/")
	if i < 0 {
		return 0, fmt.Errorf("not a PR URL: %s", htmlURL)
	}
	rest := htmlURL[i+len("/pull/"):]
	if j := strings.IndexAny(rest, "#/?"); j >= 0 {
		rest = rest[:j]
	}
	return strconv.Atoi(rest)
}

func run(ctx context.Context, stdout io.Writer, dir string, env map[string]string, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	if stdout != nil {
		cmd.Stdout = stdout
	}
	cmd.Stderr = os.Stderr
	// Start from process env so gcloud/PATH/etc. carry through.
	envSlice := os.Environ()
	for k, v := range env {
		envSlice = append(envSlice, k+"="+v)
	}
	cmd.Env = envSlice
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s %s: %w", name, strings.Join(args, " "), err)
	}
	return nil
}

func metadataGet(path string) (string, error) {
	req, err := http.NewRequest("GET", "http://metadata.google.internal/computeMetadata/v1/"+path, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Metadata-Flavor", "Google")
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("metadata %s: HTTP %d", path, resp.StatusCode)
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}
