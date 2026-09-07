# Stage 1: Build
FROM golang:1.26-alpine AS builder

WORKDIR /workspace

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o dya-controller ./dya/cmd/dya-controller

# Stage 2: Runtime
FROM alpine:3.19

RUN apk add --no-cache ca-certificates

COPY --from=builder /workspace/dya-controller /usr/local/bin/

# Non-root user
RUN adduser -D -u 1001 dya
USER 1001

ENTRYPOINT ["/usr/local/bin/dya-controller"]
