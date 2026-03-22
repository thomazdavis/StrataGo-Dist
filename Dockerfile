# Build Stage
FROM golang:alpine AS builder

WORKDIR /app

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code and build
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o stratago main.go

# Run Stage
FROM alpine:latest

# Explicitly install wget for reliable Docker healthchecks
RUN apk add --no-cache wget

WORKDIR /app

# Copy the compiled binary
COPY --from=builder /app/stratago .

# Expose default Raft, gRPC, and HTTP Management ports
EXPOSE 17001
EXPOSE 18001
EXPOSE 19001

ENTRYPOINT ["/app/stratago"]