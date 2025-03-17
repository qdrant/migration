# syntax=docker/dockerfile:1
ARG BUILDKIT_SBOM_SCAN_CONTEXT=true
ARG BUILDKIT_SBOM_SCAN_STAGE=builder
FROM --platform=${BUILDPLATFORM:-linux/amd64} registry.suse.com/bci/golang:1.24 AS builder

ARG TARGETPLATFORM
ARG BUILDPLATFORM
ARG TARGETOS
ARG TARGETARCH
ARG VERSION
ARG BUILD

COPY . /app

WORKDIR /app

RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags "-X 'main.projectVersion=${VERSION:-0.0.0}' -X 'main.projectBuild=${BUILD:-dev}'" -o bin/qdrant-migration main.go

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
