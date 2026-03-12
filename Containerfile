# Podman-focused container build file for golang-snowflake-reverse-engineer.
# Kept intentionally aligned with Dockerfile for parity.

# --- build stage ---------------------------------------------------
# Use Chainguard's Go builder image for a hardened software supply chain.
# The -dev variant includes build tooling needed by `go mod` and `go build`.
FROM cgr.dev/chainguard/go:latest-dev@sha256:7538dc60b012b069bc82f8558c4cceb05cf33ab7cf9aad3926887635c22e6063 AS builder

# Optional overrides for cross-build scenarios.
ARG TARGETOS=linux
ARG TARGETARCH=amd64

WORKDIR /src

# Download dependencies first to improve cache reuse.
COPY go.mod go.sum* ./
RUN go mod download

COPY . .

# Build a static binary with stripped debug symbols.
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/snowflake-reverse-engineer ./cmd/snowflake-reverse-engineer

# --- runtime stage -------------------------------------------------
# Use Chainguard's minimal static runtime image to reduce attack surface.
FROM cgr.dev/chainguard/static:latest@sha256:2fdfacc8d61164aa9e20909dceec7cc28b9feb66580e8e1a65b9f2443c53b61b

WORKDIR /app

# Run as an unprivileged user by default.
USER 65532:65532

# Copy only the compiled binary from the builder stage.
COPY --from=builder /out/snowflake-reverse-engineer /usr/local/bin/snowflake-reverse-engineer

# All CLI flags are passed as arguments at container run time.
ENTRYPOINT ["/usr/local/bin/snowflake-reverse-engineer"]
