# syntax=docker/dockerfile:1.7

# ─── Build stage ──────────────────────────────────────────────────────────
FROM golang:1.25-bookworm AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/reviewbot ./reviewbot

# ─── Runtime stage ────────────────────────────────────────────────────────
FROM debian:bookworm-slim

# System deps + GitHub CLI + Node.js (for Claude CLI).
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl git jq gnupg && \
    curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | \
        gpg --dearmor -o /usr/share/keyrings/githubcli-archive-keyring.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
        > /etc/apt/sources.list.d/github-cli.list && \
    curl -fsSL https://deb.nodesource.com/setup_22.x | bash - && \
    apt-get update && apt-get install -y --no-install-recommends gh nodejs && \
    rm -rf /var/lib/apt/lists/*

# Claude Code CLI (pinned via env at build time).
ARG CLAUDE_VERSION=latest
RUN npm install -g @anthropic-ai/claude-code@${CLAUDE_VERSION}

# Bundle the reviewer agents + prompt template. Spec files are NOT bundled —
# they live in the target repo at bigtable/docs/specs/ (source:
# https://github.com/googleapis/google-cloud-go/tree/main/bigtable/docs/specs)
# and are read from the PR checkout at review time. That way the reviewer
# always sees the PR's version of the spec, not a stale bundled copy.
RUN mkdir -p /opt/reviewbot/.claude/agents /opt/reviewbot/prompts
COPY reviewbot/agents/ /opt/reviewbot/.claude/agents/
COPY reviewbot/prompts/ /opt/reviewbot/prompts/

# Non-root user for the pod.
RUN useradd -m -u 1000 -d /opt/reviewbot -s /bin/bash bot && \
    chown -R bot:bot /opt/reviewbot && \
    mkdir -p /var/lib/reviewbot && chown -R bot:bot /var/lib/reviewbot

COPY --from=builder /out/reviewbot /usr/local/bin/reviewbot

USER bot
WORKDIR /var/lib/reviewbot
ENTRYPOINT ["/usr/local/bin/reviewbot"]
