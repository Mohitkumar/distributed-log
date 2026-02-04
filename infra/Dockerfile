# Build stage
FROM golang:1.25-alpine AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /server ./cmd/server

# Run stage
FROM alpine:3.19
RUN apk --no-cache add ca-certificates netcat-openbsd
WORKDIR /app
COPY --from=builder /server .

# Default: run server (args overridden by docker-compose)
ENTRYPOINT ["/app/server"]
CMD ["--bind-addr", "0.0.0.0:9092", "--raft-addr", "0.0.0.0:9093", "--rpc-port", "9094", "--data-dir", "/data", "--node-id", "node-1", "--bootstrap", "false"]
