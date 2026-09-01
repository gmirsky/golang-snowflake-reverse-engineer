# Podman-focused container build file for golang-snowflake-reverse-engineer.
# Kept intentionally aligned with Dockerfile for parity.
# Keeping these two files aligned reduces debugging friction between Docker and Podman users.

# --- build stage ---------------------------------------------------
# Use Chainguard's Go builder image for a hardened software supply chain.
# The -dev variant includes build tooling needed by `go mod` and `go build`.
# Pinning by digest provides deterministic builds and safer supply-chain behavior.
FROM cgr.dev/chainguard/go:latest-dev@sha256:af99bbaafe0a6e0ae3497deb21d19e52f24371b72c5f19ab203c4dfa89944f81 AS builder

# Optional overrides for cross-build scenarios.
# Defaults target Linux AMD64, but callers can override for other platforms.
ARG TARGETOS=linux
ARG TARGETARCH=amd64

# Use a stable working path so COPY/RUN behavior is consistent across builders.
WORKDIR /src

# Download dependencies first to improve cache reuse.
# Changes to app source code will not invalidate this layer unless module files change.
COPY go.mod go.sum* ./
RUN go mod download

# Copy full source tree once dependencies are cached.
COPY . .

# Build a static binary with stripped debug symbols.
# Static output is ideal for minimal runtime images and portable execution.
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/snowflake-reverse-engineer ./cmd/snowflake-reverse-engineer

# --- runtime stage -------------------------------------------------
# Use Chainguard's minimal static runtime image to reduce attack surface.
# Final image excludes compilers and source files to minimize risk and size.
FROM cgr.dev/chainguard/static:latest@sha256:f51c2493951313c3ad4069080b2814ffb6ed6fe3909dabeb84a9482f42d5600b

# Runtime work directory for relative command/file references.
WORKDIR /app

# Run as an unprivileged user by default.
# Non-root execution is a core container security best practice.
USER 65532:65532

# Copy only the compiled binary from the builder stage.
# Multi-stage build keeps the runtime image lean and production-focused.
COPY --from=builder /out/snowflake-reverse-engineer /usr/local/bin/snowflake-reverse-engineer

# All CLI flags are passed as arguments at container run time.
# Users can append runtime flags directly: podman run ... --user ... --account ...
ENTRYPOINT ["/usr/local/bin/snowflake-reverse-engineer"]
