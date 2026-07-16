#!/usr/bin/env bash
# =============================================================================
# migrate.sh — Qdrant → self-hosted (local) Qdrant migration wrapper
#
# Moves a collection into a self-hosted Qdrant instance. Source can be either
# another self-hosted instance (e.g. version upgrade, host migration) or
# Qdrant Cloud (e.g. offboarding back to your own infrastructure).
#
# Usage:
#   ./migrate.sh [--dry-run]
#
# Options:
#   --dry-run   Validate config and check connectivity without running migration.
#
# Configuration:
#   Edit the "Configuration" section below, or export the same variable names
#   as environment variables before running (env vars take precedence).
#
# Requirements:
#   - Docker (running)
#   - nc (netcat) for optional connectivity checks
#   - Network access to both the source and the self-hosted target
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_DIR="$(dirname "$0")/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/qdrant_migration_$(date +%Y%m%d_%H%M%S).log"

log()  { echo "[$(date +%Y-%m-%dT%H:%M:%S)] [INFO]  $*" | tee -a "$LOG_FILE"; }
warn() { echo "[$(date +%Y-%m-%dT%H:%M:%S)] [WARN]  $*" | tee -a "$LOG_FILE" >&2; }
err()  { echo "[$(date +%Y-%m-%dT%H:%M:%S)] [ERROR] $*" | tee -a "$LOG_FILE" >&2; }

# ---------------------------------------------------------------------------
# Traps
# ---------------------------------------------------------------------------
on_exit() {
  local code=$?
  if [[ $code -eq 0 ]]; then
    log "Migration completed successfully."
  else
    err "Migration failed with exit code $code. See $LOG_FILE for details."
  fi
  exit $code
}
trap on_exit EXIT

on_error() {
  err "Unexpected error on line $1 (command: $2)"
}
trap 'on_error $LINENO "$BASH_COMMAND"' ERR

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
DRY_RUN=false
for arg in "$@"; do
  case $arg in
    --dry-run) DRY_RUN=true ;;
    *) err "Unknown argument: $arg"; exit 1 ;;
  esac
done

# ---------------------------------------------------------------------------
# Configuration
# Env vars take precedence; edit defaults below if not setting env vars.
# ---------------------------------------------------------------------------

# Source Qdrant instance (another self-hosted instance or Qdrant Cloud)
SOURCE_URL="${SOURCE_URL:-"http://{{SOURCE_HOST}}:6334"}"
SOURCE_API_KEY="${SOURCE_API_KEY:-""}"                  # leave empty if no auth
SOURCE_COLLECTION="${SOURCE_COLLECTION:-"{{SOURCE_COLLECTION}}"}"

# Destination Qdrant instance (self-hosted / local)
TARGET_URL="${TARGET_URL:-"http://{{TARGET_HOST}}:6334"}"
TARGET_API_KEY="${TARGET_API_KEY:-""}"                  # leave empty if no auth
TARGET_COLLECTION="${TARGET_COLLECTION:-"{{TARGET_COLLECTION}}"}"

# Migration tuning
BATCH_SIZE="${BATCH_SIZE:-64}"

# Docker image
IMAGE="registry.cloud.qdrant.io/library/qdrant-migration"

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
validate() {
  local errors=0

  for var in SOURCE_URL SOURCE_COLLECTION TARGET_URL TARGET_COLLECTION; do
    local value="${!var}"
    if [[ -z "$value" || "$value" == *"{{"* ]]; then
      err "Required variable '$var' is not set or still contains a placeholder."
      (( errors++ )) || true
    fi
  done

  # API keys are optional (leave empty for unauthenticated instances).
  for var in SOURCE_API_KEY TARGET_API_KEY; do
    local value="${!var}"
    if [[ "$value" == *"{{"* ]]; then
      err "$var still contains a placeholder. Set it to your API key or export $var=\"\"."
      (( errors++ )) || true
    fi
  done

  if ! command -v docker &>/dev/null; then
    err "Docker is not installed or not found on PATH."
    (( errors++ )) || true
  elif ! docker info &>/dev/null 2>&1; then
    err "Docker daemon is not running."
    (( errors++ )) || true
  fi

  if ! command -v nc &>/dev/null; then
    warn "nc (netcat) not found — skipping connectivity pre-checks."
  fi

  if [[ $BATCH_SIZE -lt 1 ]]; then
    err "BATCH_SIZE must be a positive integer (got: $BATCH_SIZE)."
    (( errors++ )) || true
  fi

  [[ $errors -eq 0 ]] || { err "Validation failed. Aborting."; exit 1; }
  log "Validation passed."
}

# ---------------------------------------------------------------------------
# Connectivity check
# ---------------------------------------------------------------------------
check_connectivity() {
  local label=$1 url=$2

  if ! command -v nc &>/dev/null; then
    return 0
  fi

  local hostport
  hostport=$(echo "$url" | sed -E 's|https?://||; s|/.*||')
  local host="${hostport%%:*}"
  local port="${hostport##*:}"

  log "Checking connectivity to $label ($host:$port)..."
  if nc -zw5 "$host" "$port" 2>/dev/null; then
    log "$label is reachable."
  else
    warn "Cannot reach $label at $host:$port — proceeding anyway; Docker networking may differ."
  fi
}

# ---------------------------------------------------------------------------
# Pull image
# ---------------------------------------------------------------------------
pull_image() {
  log "Pulling migration image: $IMAGE ..."
  if ! docker pull "$IMAGE" 2>&1 | tee -a "$LOG_FILE"; then
    err "Failed to pull migration image."
    exit 1
  fi
  local digest
  digest=$(docker inspect --format='{{index .RepoDigests 0}}' "$IMAGE" 2>/dev/null || echo "unknown")
  log "Image digest: $digest"
}

# ---------------------------------------------------------------------------
# Run migration
# ---------------------------------------------------------------------------
run_migration() {
  log "Starting migration..."
  log "  Source: $SOURCE_URL  (collection: $SOURCE_COLLECTION)"
  log "  Target: $TARGET_URL  (collection: $TARGET_COLLECTION)"
  log "  Batch size: $BATCH_SIZE"

  docker run --net=host --rm \
    "$IMAGE" qdrant \
    --source.url            "$SOURCE_URL" \
    --source.api-key        "$SOURCE_API_KEY" \
    --source.collection     "$SOURCE_COLLECTION" \
    --target.url            "$TARGET_URL" \
    --target.api-key        "$TARGET_API_KEY" \
    --target.collection     "$TARGET_COLLECTION" \
    --migration.batch-size  "$BATCH_SIZE" \
    2>&1 | tee -a "$LOG_FILE"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
  log "========================================"
  log " Qdrant Migration Script"
  log " Log: $LOG_FILE"
  if $DRY_RUN; then
    log " Mode: DRY RUN (no data will be moved)"
  fi
  log "========================================"

  validate
  check_connectivity "source" "$SOURCE_URL"
  check_connectivity "target" "$TARGET_URL"

  if $DRY_RUN; then
    log "Dry run complete — all checks passed. Re-run without --dry-run to migrate."
    exit 0
  fi

  pull_image
  run_migration
}

main "$@"
