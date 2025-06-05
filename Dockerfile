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

RUN zypper addrepo --no-gpgcheck https://download.opensuse.org/repositories/devel:gcc/openSUSE_Factory_ARM/devel:gcc.repo && \
    zypper refresh && \
    zypper update -y

COPY . /app
WORKDIR /app

RUN /bin/bash -x /app/cross_compile.sh

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
