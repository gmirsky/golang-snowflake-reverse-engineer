# syntax=docker/dockerfile:1.7
# Enables BuildKit-specific directives (heredocs, cache mounts, etc.).

# --- build stage ---------------------------------------------------
# Use the host platform for compilation so cross-compilation works
# without requiring QEMU on the build machine.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder

# TARGETOS / TARGETARCH are injected by `docker buildx build --platform`.
ARG TARGETOS
ARG TARGETARCH

WORKDIR /src

# Download module dependencies before copying sources so this layer is
# cached unless go.mod / go.sum change.
COPY go.mod go.sum* ./
RUN go mod download

COPY . .

# -trimpath removes absolute build paths; -ldflags="-s -w" strips debug info
# to produce a smaller, reproducible static binary.
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/snowflake-reverse-engineer ./cmd/snowflake-reverse-engineer

# --- runtime stage -------------------------------------------------
# Use a minimal Alpine image; no Go toolchain is needed at runtime.
FROM alpine:3.21

# ca-certificates is required for TLS connections to Snowflake endpoints.
RUN apk add --no-cache ca-certificates

WORKDIR /app

# Copy only the compiled binary from the builder stage.
COPY --from=builder /out/snowflake-reverse-engineer /usr/local/bin/snowflake-reverse-engineer

# All CLI flags are passed as arguments at container run time.
ENTRYPOINT ["/usr/local/bin/snowflake-reverse-engineer"]
