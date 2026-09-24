import argparse
from contextlib import closing
import math
from pathlib import Path

import lancedb
import pyarrow as pa
from qdrant_client import QdrantClient


SCHEMA = pa.schema([
    ("id", pa.string()),
    ("vector", pa.list_(pa.float32(), 3)),
    ("image", pa.list_(pa.float32(), 2)),
    ("text", pa.string()),
    ("tags", pa.list_(pa.string())),
    ("meta", pa.struct([("rank", pa.int64()), ("active", pa.bool_())])),
])


def seed(uri):
    db = lancedb.connect(uri)
    data = [{"id": f"doc-{i}", "vector": [float(i), 1.0, -2.0],
             "image": [float(i) / 2, 3.0], "text": f"Document {i}: café",
             "tags": ["fixture", f"group-{i % 3}"],
             "meta": {"rank": i, "active": i % 2 == 0}} for i in range(38)]
    table = db.create_table("documents", pa.Table.from_pylist(data[:20], schema=SCHEMA))
    table.add(pa.Table.from_pylist(data[20:], schema=SCHEMA))
    table.delete("id = 'doc-5'")
    print(f"Created {table.count_rows()} rows in documents at version {table.version}")


def verify(uri, url, collection):
    table = lancedb.connect(uri).open_table("documents")
    source = {row["id"]: row for batch in table.search().limit(None).to_batches()
              for row in batch.to_pylist()}
    with closing(QdrantClient(url=url)) as client:
        found = {}
        offset = None
        while True:
            records, offset = client.scroll(collection, limit=7, offset=offset,
                                             with_payload=True, with_vectors=True)
            for record in records:
                source_id = record.payload["id"]
                if source_id in found:
                    raise AssertionError(f"Duplicate original ID: {source_id}")
                found[source_id] = record
            if offset is None:
                break
        assert set(found) == set(source), "Source and target ID sets differ"
        assert client.count(collection, exact=True).count == len(source)
        for source_id, row in source.items():
            record = found[source_id]
            expected_payload = {key: value for key, value in row.items() if key not in ("vector", "image")}
            assert record.payload == expected_payload, f"Payload mismatch: {source_id}"
            assert set(record.vector) == {"vector", "image"}
            for name in ("vector", "image"):
                assert len(record.vector[name]) == len(row[name])
                assert all(math.isclose(a, b, rel_tol=1e-6, abs_tol=1e-6)
                           for a, b in zip(record.vector[name], row[name])), f"Vector mismatch: {source_id}/{name}"
        print(f"PASS: independently verified all {len(source)} IDs, payloads and both vectors")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["seed", "verify"])
    parser.add_argument("--uri", required=True)
    parser.add_argument("--url", default="http://localhost:6333", help="Qdrant REST URL for the audit")
    parser.add_argument("--collection", default="lancedb_manual")
    args = parser.parse_args()
    uri = str(Path(args.uri).resolve())
    if args.action == "seed":
        seed(uri)
    else:
        verify(uri, args.url, args.collection)
