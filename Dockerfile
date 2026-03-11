# syntax=docker/dockerfile:1.7

FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /src

COPY go.mod go.sum* ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/snowflake-reverse-engineer ./cmd/snowflake-reverse-engineer

FROM alpine:3.21

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=builder /out/snowflake-reverse-engineer /usr/local/bin/snowflake-reverse-engineer

ENTRYPOINT ["/usr/local/bin/snowflake-reverse-engineer"]
