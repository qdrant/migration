# Self-hosted Qdrant → Qdrant Cloud Onboarding

A shell script wrapper around the `qdrant-migration` CLI for moving an existing self-hosted Qdrant collection into [Qdrant Cloud](https://cloud.qdrant.io) with preflight checks, structured logs, and a dry-run mode.

If you already run Qdrant locally or on your own infrastructure and want to onboard onto Qdrant Cloud without writing your own glue around `docker run`, start here.

Features beyond the raw `docker run` command:
- Pre-flight validation of all required config variables
- TCP connectivity checks for source and target before starting
- Timestamped log file written to `logs/` for every run
- `--dry-run` mode to validate config without moving data
- Structured error output with exit codes and line-level error reporting
- Resumable — re-run the same command to continue an interrupted migration

> The script itself is plain `qdrant-migration` underneath, so it also works for any Qdrant → Qdrant migration (self-hosted → self-hosted, Cloud → self-hosted, etc.). The defaults and examples below are written for the self-hosted → Cloud onboarding case.

## Requirements

- Docker (running)
- `nc` (netcat) — optional, used for connectivity pre-checks
- Network access from this machine to both your self-hosted Qdrant and Qdrant Cloud
- A Qdrant Cloud cluster and API key — create one at [cloud.qdrant.io](https://cloud.qdrant.io)

## Usage

### 1. Make the script executable

```bash
chmod +x migrate.sh
```

### 2. Configure

Export environment variables (recommended — keeps secrets out of the file):

```bash
# Self-hosted source (Linux / Docker on Linux):
export SOURCE_URL="http://localhost:6334"
# Self-hosted source on macOS / Windows with Docker Desktop:
export SOURCE_URL="http://host.docker.internal:6334"
export SOURCE_COLLECTION="my_collection"

# Qdrant Cloud target:
export TARGET_URL="https://<cluster-id>.<region>.cloud.qdrant.io:6334"
export TARGET_API_KEY="your-qdrant-cloud-api-key"
export TARGET_COLLECTION="my_collection"
```

Or edit the `Configuration` section in `migrate.sh` directly. Environment variables always take precedence over in-file defaults.

### 3. Dry run (recommended)

Validates config and checks connectivity without moving any data:

```bash
./migrate.sh --dry-run
```

### 4. Run the migration

```bash
./migrate.sh
```

## Configuration Reference

| Variable | Required | Default | Description |
|---|---|---|---|
| `SOURCE_URL` | Yes | — | gRPC URL of the self-hosted source (port `6334`) |
| `SOURCE_API_KEY` | No | `""` | API key for the source; leave empty if unauthenticated |
| `SOURCE_COLLECTION` | Yes | — | Collection name on the source |
| `TARGET_URL` | Yes | — | gRPC URL of the Qdrant Cloud cluster (port `6334`) |
| `TARGET_API_KEY` | No | `""` | Qdrant Cloud API key |
| `TARGET_COLLECTION` | Yes | — | Collection name on the target |
| `BATCH_SIZE` | No | `64` | Points transferred per batch; increase for throughput, decrease for lower memory use |

## Logs

Each run writes a timestamped log to `logs/qdrant_migration_YYYYMMDD_HHMMSS.log`. To follow a migration in real time:

```bash
tail -f logs/qdrant_migration_*.log
```

## Notes

**gRPC port required**
The migration tool communicates over gRPC (port `6334`), not the REST API (port `6333`). Make sure your self-hosted Qdrant container exposes port `6334`:
```bash
docker run -d --name qdrant -p 6333:6333 -p 6334:6334 qdrant/qdrant
```
If your container was started without `-p 6334:6334`, stop it, back up the data volume, and recreate it with both ports mapped.

**macOS / Windows with Docker Desktop**
`--net=host` is not supported on Docker Desktop. The container cannot reach `localhost` on the host. Use `host.docker.internal` in `SOURCE_URL` instead of `localhost`.

## Troubleshooting

**`still contains a placeholder`**
A `{{...}}` value was not replaced. Export the corresponding environment variable or edit `migrate.sh`.

**`Docker daemon is not running`**
Start Docker Desktop (macOS) or run `sudo systemctl start docker` (Linux).

**`Cannot reach source/target`**
This is a non-fatal warning. If both endpoints are remote (not `localhost`), remove `--net=host` from the `docker run` call in `migrate.sh`. For Docker Desktop on macOS/Windows, use `host.docker.internal` in place of `localhost` in your URLs.

**Target collection already exists**
The tool will upsert into the existing collection. Vector size and distance metric must match the source; replication and shard settings may differ. See the [project README](../../README.md#from-another-qdrant-instance) for details.

**Migration is slow**
Increase `BATCH_SIZE` (e.g. `export BATCH_SIZE=256`) or set `--migration.num-workers` by adding it to the `docker run` call.
