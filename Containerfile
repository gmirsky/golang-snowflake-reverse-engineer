# Podman-focused container build file for golang-snowflake-reverse-engineer.
# Kept intentionally aligned with Dockerfile for parity.
# Keeping these two files aligned reduces debugging friction between Docker and Podman users.

# --- build stage ---------------------------------------------------
# Use Chainguard's Go builder image for a hardened software supply chain.
# The -dev variant includes build tooling needed by `go mod` and `go build`.
# Pinning by digest provides deterministic builds and safer supply-chain behavior.
FROM cgr.dev/chainguard/go:latest-dev@sha256:334dd92cd6393623acffa207c0ce73219d2ab79ecf8a4fb43e8993b6d2a57768 AS builder

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
FROM cgr.dev/chainguard/static:latest@sha256:2fdfacc8d61164aa9e20909dceec7cc28b9feb66580e8e1a65b9f2443c53b61b

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
