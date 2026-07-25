# --- Stage 1: Builder ---
FROM golang:1.25-alpine AS builder

RUN apk add --no-cache git

WORKDIR /app

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o bigtable-validator main.go

# --- Stage 2: Runner ---
FROM alpine:3.18
RUN apk add --no-cache ca-certificates

WORKDIR /root/

COPY --from=builder /app/bigtable-validator .

EXPOSE 6060

ENTRYPOINT ["./bigtable-validator"]
