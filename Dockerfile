# syntax=docker/dockerfile:1.7
# Enables BuildKit-specific directives (heredocs, cache mounts, etc.).
# Keeping this explicit avoids behavior differences between old and new Docker engines.

# --- build stage ---------------------------------------------------
# Use Chainguard's Go builder image for a hardened software supply chain.
# The -dev variant includes build tooling needed by `go mod` and `go build`.
# Pinning by digest makes builds reproducible and prevents surprise upstream changes.
FROM --platform=$BUILDPLATFORM cgr.dev/chainguard/go:latest-dev@sha256:fe9cf0af05cab0bc2b640f9f636713c7aabd64542e970a21db785fb1df31af98 AS builder

# TARGETOS / TARGETARCH are injected by `docker buildx build --platform`.
# This allows one Dockerfile to produce binaries for multiple CPU/OS targets.
ARG TARGETOS
ARG TARGETARCH

# Build steps happen under /src so paths are predictable across environments.
WORKDIR /src

# Download module dependencies before copying sources so this layer is
# cached unless go.mod / go.sum change.
# This is a standard optimization because dependency download is usually slower
# than compiling local source, and dependencies change less often.
COPY go.mod go.sum* ./
RUN go mod download

# Copy project files after dependency download to maximize cache reuse.
COPY . .

# -trimpath removes absolute build paths; -ldflags="-s -w" strips debug info
# to produce a smaller, reproducible static binary.
# CGO_ENABLED=0 produces a static binary, which simplifies runtime images and
# avoids libc compatibility issues in minimal containers.
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/snowflake-reverse-engineer ./cmd/snowflake-reverse-engineer

# --- runtime stage -------------------------------------------------
# Use Chainguard's minimal static runtime image to reduce attack surface.
# Runtime image contains only what is needed to execute the binary.
FROM cgr.dev/chainguard/static:latest@sha256:2fdfacc8d61164aa9e20909dceec7cc28b9feb66580e8e1a65b9f2443c53b61b

# /app is the working directory for relative file paths during container run.
WORKDIR /app

# Run as an unprivileged user by default.
# UID/GID 65532 is a common non-root identity in distroless/static images.
USER 65532:65532

# Copy only the compiled binary from the builder stage.
# Multi-stage copy keeps source code and build tooling out of the final image.
COPY --from=builder /out/snowflake-reverse-engineer /usr/local/bin/snowflake-reverse-engineer

# All CLI flags are passed as arguments at container run time.
# ENTRYPOINT makes the binary the default command while still allowing args override.
ENTRYPOINT ["/usr/local/bin/snowflake-reverse-engineer"]
