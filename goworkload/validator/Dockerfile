# --- Stage 1: Builder ---
FROM golang:1.25-alpine AS builder

# Install git for fetching dependencies
RUN apk add --no-cache git

WORKDIR /app

# Copy dependency files first to leverage Docker cache
# COPY go.mod go.sum ./
# RUN go mod download

# Copy the source code (vendor/ is included, so build resolves deps
# from ./vendor rather than the network — required because go.mod's
# replace directive points at a local path outside the build context).
COPY . .

# Build the application
# CGO_ENABLED=0 ensures a static binary
RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -o bigtable-validator main.go

# --- Stage 2: Runner ---
# We use a small alpine image.
# We need ca-certificates to make HTTPS calls to Google APIs.
FROM alpine:3.18
RUN apk add --no-cache ca-certificates

WORKDIR /root/

# Copy the binary from the builder stage
COPY --from=builder /app/bigtable-validator .

# Expose the pprof port defined in your code
EXPOSE 6060

# Entrypoint - Note: Flags will be passed via Kubernetes args
ENTRYPOINT ["./bigtable-validator"]
