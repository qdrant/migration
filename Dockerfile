# syntax=docker/dockerfile:1
ARG BUILDKIT_SBOM_SCAN_CONTEXT=true
ARG BUILDKIT_SBOM_SCAN_STAGE=builder
FROM --platform=${BUILDPLATFORM:-linux/amd64} golang:1.24 AS builder

ARG TARGETPLATFORM
ARG BUILDPLATFORM
ARG TARGETOS
ARG TARGETARCH
ARG VERSION
ARG BUILD

RUN apt-get update && case "${TARGETARCH}" in \
    amd64) apt-get install -y gcc-x86-64-linux-gnu ;; \
    arm64) apt-get install -y gcc-aarch64-linux-gnu ;; \
esac && rm -rf /var/lib/apt/lists/*

COPY . /app

WORKDIR /app

RUN case "${TARGETARCH}" in \
      amd64) CC=x86_64-linux-gnu-gcc ;; \
      arm64) CC=aarch64-linux-gnu-gcc ;; \
    esac && \
    CGO_ENABLED=1 GOOS=${TARGETOS} GOARCH=${TARGETARCH} CC=$CC \
    go build -ldflags "-X 'main.projectVersion=${VERSION:-0.0.0}' -X 'main.projectBuild=${BUILD:-dev}'" -o bin/qdrant-migration main.go

FROM --platform=${TARGETPLATFORM:-linux/amd64} registry.suse.com/bci/bci-minimal:15.6

COPY --from=builder /app/bin/qdrant-migration /opt/

USER 1000

LABEL org.opencontainers.image.title="Qdrant Migration"
LABEL org.opencontainers.image.description="This tool helps to migrate data to Qdrant from other sources."
LABEL org.opencontainers.image.url="https://qdrant.com/"
LABEL org.opencontainers.image.documentation="https://qdrant.com/docs"
LABEL org.opencontainers.image.source="https://github.com/qdrant/migration"
LABEL org.opencontainers.image.vendor="Qdrant"

ENTRYPOINT ["/opt/qdrant-migration"]
