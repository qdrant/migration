# syntax=docker/dockerfile:1
ARG BUILDKIT_SBOM_SCAN_CONTEXT=true
ARG BUILDKIT_SBOM_SCAN_STAGE=builder
FROM registry.suse.com/bci/golang:1.24 AS builder

ARG VERSION
ARG BUILD

COPY . /app

WORKDIR /app

RUN CGO_ENABLED=1 go build -ldflags "-X 'main.projectVersion=${VERSION:-0.0.0}' -X 'main.projectBuild=${BUILD:-dev}'" -o bin/qdrant-migration main.go

FROM python:3.11-slim AS runtime

RUN python3 -m pip install --no-cache-dir --no-compile --upgrade pip==25.2 setuptools==80.9.0 && \
    python3 -m pip install --no-cache-dir --no-compile --prefer-binary \
      faiss-cpu==1.11.0.post1 \
      numpy==2.3.2 \
      tqdm==4.67.1 \
      qdrant-client==1.15.1

COPY --from=builder /app/bin/qdrant-migration /opt/
COPY cmd/faiss_to_qdrant.py /opt/cmd/faiss_to_qdrant.py

WORKDIR /opt

USER 1000

LABEL org.opencontainers.image.title="Qdrant Migration"
LABEL org.opencontainers.image.description="This tool helps to migrate data to Qdrant from other sources."
LABEL org.opencontainers.image.url="https://qdrant.com/"
LABEL org.opencontainers.image.documentation="https://qdrant.com/docs"
LABEL org.opencontainers.image.source="https://github.com/qdrant/migration"
LABEL org.opencontainers.image.vendor="Qdrant"

ENTRYPOINT ["/opt/qdrant-migration"]
