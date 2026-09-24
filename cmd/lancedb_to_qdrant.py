import base64
from contextlib import closing
from dataclasses import dataclass, field
from datetime import date, datetime, time as datetime_time
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import struct
import sys
import tempfile
import time
from urllib.parse import unquote, urlparse
import uuid

import lancedb
from lancedb.schema import blob_column_paths
import pyarrow as pa
from qdrant_client import QdrantClient, models


FORMAT_VERSION = 1
DISTANCES = {
    "cosine": models.Distance.COSINE,
    "dot": models.Distance.DOT,
    "euclid": models.Distance.EUCLID,
    "manhattan": models.Distance.MANHATTAN,
}


def canonical_json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=True, allow_nan=False,
                      separators=(",", ":"))


def canonical_uri(uri):
    parsed = urlparse(uri)
    if not parsed.scheme:
        return str(Path(uri).expanduser().resolve())
    if parsed.scheme == "file":
        if parsed.netloc not in ("", "localhost") or parsed.query or parsed.fragment:
            raise ValueError("Use a local file:// URI without query parameters")
        return str(Path(unquote(parsed.path)).resolve())
    if parsed.scheme not in ("s3", "gs", "az", "db"):
        raise ValueError("Supported source URIs: local paths, file://, s3://, gs://, az://, db://")
    if not parsed.netloc or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("Source URI must identify a database without embedded credentials or query parameters")
    return uri.rstrip("/")


@dataclass
class Config:
    uri: str
    table: str
    id_column: str
    collection: str
    vector_columns: list[str] = field(default_factory=lambda: ["vector"])
    metrics: dict[str, str] = field(default_factory=dict)
    version: int = 0
    source_api_key: str = ""
    region: str = "us-east-1"
    staging_dir: str = ""
    qdrant_url: str = "http://localhost:6334"
    qdrant_api_key: str = ""
    batch_size: int = 50
    batch_delay: int = 0
    restart: bool = False
    create_collection: bool = True
    offsets_collection: str = "_migration_offsets"

    def __post_init__(self):
        if not all((self.uri, self.table, self.id_column, self.collection, self.offsets_collection)):
            raise ValueError("Source URI, table, ID column, target and offsets collections are required")
        self.uri = canonical_uri(self.uri)
        if self.batch_size < 1 or self.batch_delay < 0 or self.version < 0:
            raise ValueError("Batch size must be positive; batch delay and version must be non-negative")
        if self.collection == self.offsets_collection:
            raise ValueError("Target and offsets collections must be different")
        if not self.vector_columns or any(not name for name in self.vector_columns):
            raise ValueError("At least one nonempty vector column is required")
        if len(set(self.vector_columns)) != len(self.vector_columns):
            raise ValueError("Vector columns must be unique")
        if self.id_column in self.vector_columns:
            raise ValueError("ID column cannot also be a vector column")
        supplied = self.metrics or {}
        if set(supplied) - set(self.vector_columns):
            raise ValueError("Distance mappings must refer to selected vector columns")
        self.metrics = {name: supplied.get(name, "cosine") for name in self.vector_columns}
        if any(metric not in DISTANCES for metric in self.metrics.values()):
            raise ValueError("Distances must be cosine, dot, euclid, or manhattan")

    @property
    def source_identity(self):
        return {"uri": self.uri, "table": self.table,
                "region": self.region if self.uri.startswith("db://") else ""}

    @property
    def checkpoint_id(self):
        identity = {"source": self.source_identity, "target": self.collection}
        return str(uuid.uuid5(uuid.NAMESPACE_URL, "lancedb-checkpoint:" + canonical_json(identity)))

    @property
    def point_namespace(self):
        return uuid.uuid5(uuid.NAMESPACE_URL, "lancedb-points:" + canonical_json({
            "source": self.source_identity, "id_column": self.id_column,
        }))


def point_id(config, value):
    if type(value) is int:
        key = ["integer", str(value)]
    elif type(value) is str:
        key = ["string", value]
    else:
        raise ValueError("IDs must be non-null strings or integers (not booleans)")
    return str(uuid.uuid5(config.point_namespace, canonical_json(key)))


def payload_value(value):
    """Explicit JSON conversions; never stringify an unknown object silently."""
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, int):
        return value if -(2**63) <= value < 2**63 else str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("Payload contains NaN or infinity")
        return value
    if isinstance(value, (datetime, date, datetime_time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("Payload contains a non-finite decimal")
        return str(value)
    if isinstance(value, bytes):
        return {"__base64__": base64.b64encode(value).decode("ascii")}
    if isinstance(value, list):
        return [payload_value(item) for item in value]
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        return {key: payload_value(item) for key, item in value.items()}
    raise ValueError(f"Unsupported payload type: {type(value).__name__}")


def validate_payload_type(dtype):
    if pa.types.is_struct(dtype):
        for child in dtype:
            validate_payload_type(child.type)
    elif pa.types.is_list(dtype) or pa.types.is_large_list(dtype) or pa.types.is_fixed_size_list(dtype):
        validate_payload_type(dtype.value_type)
    elif not any(check(dtype) for check in (
        pa.types.is_null, pa.types.is_boolean, pa.types.is_integer,
        pa.types.is_floating, pa.types.is_string, pa.types.is_large_string,
        pa.types.is_binary, pa.types.is_large_binary, pa.types.is_fixed_size_binary,
        pa.types.is_date, pa.types.is_time, pa.types.is_timestamp, pa.types.is_decimal,
    )):
        raise ValueError(f"Unsupported payload Arrow type: {dtype}")


def vector_dimensions(schema, config):
    if blob_column_paths(schema):
        raise ValueError("Lance blob columns require explicit export and are unsupported")
    if len(set(schema.names)) != len(schema.names):
        raise ValueError("Duplicate source column names are not supported")
    for name in [config.id_column, *config.vector_columns]:
        if name not in schema.names:
            raise ValueError(f"Source column does not exist: {name}")
    id_type = schema.field(config.id_column).type
    if not (pa.types.is_integer(id_type) or pa.types.is_string(id_type) or pa.types.is_large_string(id_type)):
        raise ValueError("ID column must have an integer or string Arrow type")
    dimensions = {}
    for column in schema:
        if column.name not in config.vector_columns:
            validate_payload_type(column.type)
            continue
        dtype = column.type
        if not (pa.types.is_fixed_size_list(dtype) and dtype.list_size > 0
                and pa.types.is_floating(dtype.value_type)):
            raise ValueError(f"Vector column {column.name} must be a fixed-size list of floats; binary, sparse and multivectors are unsupported")
        dimensions[column.name] = dtype.list_size
    return dimensions


def dense_vector(value, dimension, metric):
    if not isinstance(value, list) or len(value) != dimension:
        raise ValueError(f"Expected a non-null vector of dimension {dimension}")
    result = []
    for item in value:
        if isinstance(item, bool) or not isinstance(item, (float, int)) or not math.isfinite(item):
            raise ValueError("Vectors must contain only finite, non-null numbers")
        try:
            rounded = struct.unpack("<f", struct.pack("<f", item))[0]
        except (OverflowError, struct.error) as exc:
            raise ValueError("Vector value is outside float32 range") from exc
        if not math.isfinite(rounded):
            raise ValueError("Vector value is outside float32 range")
        result.append(rounded)
    if metric == "cosine":
        norm = math.hypot(*result)
        if norm == 0:
            raise ValueError("Zero vectors cannot be migrated with cosine distance")
        result = [struct.unpack("<f", struct.pack("<f", item / norm))[0] for item in result]
    return result


def convert_row(row, config, dimensions):
    return {
        "id": point_id(config, row[config.id_column]),
        "vector": {name: dense_vector(row[name], size, config.metrics[name])
                   for name, size in dimensions.items()},
        "payload": {name: payload_value(value) for name, value in row.items()
                    if name not in dimensions},
    }


def stage_table(table, config, spool, dimensions):
    spool.execute("CREATE TABLE points (id TEXT PRIMARY KEY, body TEXT NOT NULL) WITHOUT ROWID")
    expected = table.count_rows()
    total = 0

    reader = table.search().limit(None).to_batches(batch_size=config.batch_size)
    with closing(reader):
        for batch in reader:
            for row in batch.to_pylist():
                try:
                    point = convert_row(row, config, dimensions)
                    spool.execute("INSERT INTO points VALUES (?, ?)",
                                  (point["id"], canonical_json(point)))
                except sqlite3.IntegrityError as exc:
                    raise ValueError(f"Duplicate ID at source row {total}; no target data written") from exc
                except (ValueError, TypeError) as exc:
                    raise ValueError(f"Invalid source row {total}: {exc}") from exc
                total += 1
    if total != expected:
        raise ValueError(f"Incomplete source scan: expected {expected} rows, received {total}")
    spool.commit()
    digest = hashlib.sha256()
    for (body,) in spool.execute("SELECT body FROM points ORDER BY id"):
        digest.update(body.encode("ascii") + b"\n")
    return total, digest.hexdigest()


def batches(spool, size, after=""):
    cursor = spool.execute("SELECT body FROM points WHERE id > ? ORDER BY id", (after,))
    while rows := cursor.fetchmany(size):
        yield [json.loads(row[0]) for row in rows]


def enum_value(value):
    return getattr(value, "value", value)


def check_collection(client, name, dimensions, metrics):
    params = client.get_collection(name).config.params
    actual = params.vectors
    if actual is None and not dimensions:
        actual = {}
    if not isinstance(actual, dict) or set(actual) != set(dimensions):
        raise ValueError(f"Collection {name} must have exactly these named vectors: {sorted(dimensions)}")
    if params.sparse_vectors:
        raise ValueError(f"Collection {name} has unsupported sparse vector configuration")
    if enum_value(params.sharding_method) == "custom":
        raise ValueError("Custom-sharded target collections require shard routing and are unsupported")
    for column, size in dimensions.items():
        vector = actual[column]
        if (vector.size != size or vector.distance != DISTANCES[metrics[column]]
                or vector.multivector_config is not None
                or enum_value(vector.datatype) not in (None, "float32")):
            raise ValueError(f"Incompatible target vector configuration for {column}")


def ensure_collection(client, config, dimensions, resume):
    if not client.collection_exists(config.collection):
        if resume:
            raise ValueError("Checkpoint exists but target collection is missing; use --migration.restart")
        if not config.create_collection:
            raise ValueError("Target collection does not exist and collection creation is disabled")
        client.create_collection(config.collection, vectors_config={
            name: models.VectorParams(size=size, distance=DISTANCES[config.metrics[name]])
            for name, size in dimensions.items()
        })
    check_collection(client, config.collection, dimensions, config.metrics)


def load_checkpoint(client, config):
    if not client.collection_exists(config.offsets_collection):
        return None
    points = client.retrieve(config.offsets_collection, ids=[config.checkpoint_id],
                             with_payload=True, with_vectors=False)
    if not points:
        return None
    payload = points[0].payload
    if not isinstance(payload, dict) or payload.get("format") != FORMAT_VERSION:
        raise ValueError("Unrecognized LanceDB checkpoint; use --migration.restart")
    version = payload.get("version")
    if type(version) is not int or version < 1:
        raise ValueError("Invalid source version in checkpoint")
    return payload


def write_points(client, collection, points):
    result = client.upsert(collection, points=[models.PointStruct(**point) for point in points], wait=True)
    if result.status != models.UpdateStatus.COMPLETED:
        raise RuntimeError(f"Qdrant did not confirm completion for collection {collection}")


def save_checkpoint(client, config, manifest, last_id, migrated, complete=False):
    payload = dict(manifest, last_id=last_id, migrated=migrated, complete=complete)
    write_points(client, config.offsets_collection, [{
        "id": config.checkpoint_id, "vector": {}, "payload": payload,
    }])


def resume_position(checkpoint, manifest, spool):
    if checkpoint is None:
        return "", 0
    if any(checkpoint.get(key) != value for key, value in manifest.items()):
        raise ValueError("Checkpoint does not match source snapshot or migration mapping; use --migration.restart")
    last_id, migrated = checkpoint.get("last_id"), checkpoint.get("migrated")
    if not isinstance(last_id, str) or type(migrated) is not int or migrated < 0:
        raise ValueError("Invalid checkpoint position")
    if last_id and spool.execute("SELECT 1 FROM points WHERE id = ?", (last_id,)).fetchone() is None:
        raise ValueError("Checkpoint ID is missing from the source snapshot")
    actual = spool.execute("SELECT count(*) FROM points WHERE id <= ?", (last_id,)).fetchone()[0]
    if actual != migrated or (not last_id and migrated != 0):
        raise ValueError("Checkpoint count does not match its position")
    return last_id, migrated


def verify_target(client, config, spool):
    verified = 0
    for expected in batches(spool, config.batch_size):
        records = client.retrieve(config.collection, ids=[p["id"] for p in expected],
                                  with_payload=True, with_vectors=True)
        actual = {str(record.id): record for record in records}
        if set(actual) != {p["id"] for p in expected}:
            raise RuntimeError("Target verification found missing IDs; rerun with --migration.restart")
        for point in expected:
            record = actual[point["id"]]
            if record.payload != point["payload"]:
                raise RuntimeError(f"Target payload mismatch for {point['id']}; rerun with --migration.restart")
            if not isinstance(record.vector, dict) or set(record.vector) != set(point["vector"]):
                raise RuntimeError(f"Target vector names mismatch for {point['id']}")
            for name, vector in point["vector"].items():
                received = record.vector[name]
                if len(received) != len(vector) or any(
                    not math.isclose(a, b, rel_tol=1e-5, abs_tol=1e-6)
                    for a, b in zip(received, vector)
                ):
                    raise RuntimeError(f"Target vector mismatch for {point['id']}/{name}; rerun with --migration.restart")
        verified += len(expected)
    return verified


def migrate(config, client, table):
    checkpoint = None if config.restart else load_checkpoint(client, config)
    if checkpoint and config.version and config.version != checkpoint["version"]:
        raise ValueError("Requested version differs from checkpoint; use --migration.restart")
    version = checkpoint["version"] if checkpoint else (config.version or table.version)
    table.checkout(version)
    if table.version != version:
        raise ValueError("LanceDB did not check out the requested version")
    dimensions = vector_dimensions(table.schema, config)
    print(f"Staging LanceDB version {version}; validating all rows before target writes", flush=True)
    with tempfile.TemporaryDirectory(prefix="lancedb-migration-", dir=config.staging_dir or None) as directory:
        with closing(sqlite3.connect(str(Path(directory) / "points.sqlite"))) as spool:
            total, digest = stage_table(table, config, spool, dimensions)
            manifest = {
                "format": FORMAT_VERSION, "source": config.source_identity,
                "target": config.collection, "version": version,
                "id_column": config.id_column, "dimensions": dimensions,
                "metrics": config.metrics, "total": total, "digest": digest,
            }
            last_id, migrated = resume_position(checkpoint, manifest, spool)
            ensure_collection(client, config, dimensions, checkpoint is not None)
            if not client.collection_exists(config.offsets_collection):
                client.create_collection(config.offsets_collection, vectors_config={})
            check_collection(client, config.offsets_collection, {}, {})
            save_checkpoint(client, config, manifest, last_id, migrated)
            print(f"Validated {total} rows; resuming after {migrated} confirmed rows", flush=True)
            for points in batches(spool, config.batch_size, last_id):
                write_points(client, config.collection, points)
                migrated += len(points)
                last_id = points[-1]["id"]
                save_checkpoint(client, config, manifest, last_id, migrated)
                print(f"Migrated {migrated}/{total}", flush=True)
                if config.batch_delay:
                    time.sleep(config.batch_delay / 1000)
            verified = verify_target(client, config, spool)
            if verified != total:
                raise RuntimeError(f"Verification count mismatch: {verified} != {total}")
            save_checkpoint(client, config, manifest, last_id, migrated, complete=True)
            print(f"Migration complete: verified {verified} IDs, payloads and vectors (source version {version})", flush=True)
            return verified


def target_client(config):
    parsed = urlparse(config.qdrant_url)
    if (parsed.scheme not in ("http", "https") or not parsed.hostname
            or parsed.username or parsed.password or parsed.path not in ("", "/")
            or parsed.query or parsed.fragment):
        raise ValueError("Qdrant URL must be an http(s) gRPC endpoint without a path, query or credentials")
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    return QdrantClient(host=parsed.hostname, grpc_port=port,
                        https=parsed.scheme == "https", prefer_grpc=True,
                        api_key=config.qdrant_api_key or None, timeout=60,
                        check_compatibility=False)


def main():
    config = Config(**json.load(sys.stdin))
    if not urlparse(config.uri).scheme and not Path(config.uri).is_dir():
        raise ValueError("Local LanceDB source directory does not exist")
    kwargs = {}
    if config.uri.startswith("db://"):
        if not config.source_api_key:
            raise ValueError("LanceDB Cloud requires LANCEDB_API_KEY or --lancedb.api-key")
        kwargs = {"api_key": config.source_api_key, "region": config.region}
    connection = lancedb.connect(config.uri, **kwargs)
    table = connection.open_table(config.table)
    with closing(target_client(config)) as client:
        migrate(config, client, table)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Migration interrupted; rerun the same command to resume", file=sys.stderr)
        sys.exit(130)
    except Exception as exc:
        print(f"LanceDB migration failed: {exc}", file=sys.stderr)
        sys.exit(1)
