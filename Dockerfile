# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Install dependencies
RUN apk add --no-cache git ca-certificates tzdata

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o pipeline cmd/main.go

# Final stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata
WORKDIR /app

# Copy the binary
COPY --from=builder /app/pipeline .

# Copy configuration files
COPY --from=builder /app/config ./config

# Create output directory
RUN mkdir -p output

# Create non-root user
RUN addgroup -g 1001 pipeline && \
    adduser -D -s /bin/sh -u 1001 -G pipeline pipeline

RUN chown -R pipeline:pipeline /app

USER pipeline

EXPOSE 8080

CMD ["./pipeline"]
