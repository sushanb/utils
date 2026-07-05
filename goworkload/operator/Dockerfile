FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY main.go .
RUN CGO_ENABLED=0 GOOS=linux go build -o operator main.go

FROM alpine:3.18
WORKDIR /root/
COPY --from=builder /app/operator .
ENTRYPOINT ["./operator"]