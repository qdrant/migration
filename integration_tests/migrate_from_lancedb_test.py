from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import os
import sqlite3
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
import uuid
from unittest.mock import patch

import lancedb
import pyarrow as pa
from qdrant_client import QdrantClient, models

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "cmd"))
import lancedb_to_qdrant as migration


SCHEMA = pa.schema([
    ("id", pa.string()), ("vector", pa.list_(pa.float32(), 3)),
    ("text", pa.string()), ("meta", pa.struct([("rank", pa.int64())])),
])


def rows(count=13):
    return [{"id": f"doc-{i}", "vector": [float(i), 1.0, -2.0],
             "text": f"document {i}", "meta": {"rank": i}} for i in range(count)]


class FaultClient:
    """Inject an ambiguous write or checkpoint failure after one good batch."""

    def __init__(self, client, collection, checkpoint_failure=False, after_apply=False):
        self.client = client
        self.collection = collection
        self.checkpoint_failure = checkpoint_failure
        self.after_apply = after_apply
        self.data_writes = 0

    def __getattr__(self, name):
        return getattr(self.client, name)

    def upsert(self, collection_name, **kwargs):
        if collection_name == self.collection:
            self.data_writes += 1
            if self.data_writes == 2 and not self.checkpoint_failure:
                if self.after_apply:
                    self.client.upsert(collection_name, **kwargs)
                raise RuntimeError("injected data failure")
        elif self.checkpoint_failure and self.data_writes == 2:
            raise RuntimeError("injected checkpoint failure")
        return self.client.upsert(collection_name, **kwargs)


class ConversionTests(unittest.TestCase):
    def setUp(self):
        self.config = migration.Config(uri="/tmp/lance-test", table="docs", id_column="id", collection="target")

    def test_typed_stable_namespaced_ids(self):
        make = lambda value: migration.point_id(self.config, value)
        self.assertEqual(make("a"), make("a"))
        self.assertNotEqual(make(1), make("1"))
        self.assertNotEqual(make("a"), migration.point_id(replace(self.config, table="other"), "a"))
        for invalid in (None, True, 1.5, [], {}):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                make(invalid)

    def test_payload_conversions(self):
        value = {"time": datetime(2024, 1, 2, tzinfo=timezone.utc), "decimal": Decimal("1.20"),
                 "binary": b"abc", "big": 2**63, "nested": [None, True, {"x": 1.5}]}
        self.assertEqual(migration.payload_value(value), {
            "time": "2024-01-02T00:00:00+00:00", "decimal": "1.20",
            "binary": {"__base64__": "YWJj"}, "big": str(2**63), "nested": [None, True, {"x": 1.5}],
        })
        for invalid in (float("nan"), float("inf"), Decimal("NaN"), object()):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                migration.payload_value(invalid)

    def test_invalid_vectors(self):
        for invalid in (None, [1.0], [0.0, 0.0], [float("nan"), 1.0],
                        [float("inf"), 1.0], [None, 1.0], [True, 1.0], [1e100, 1.0]):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                migration.dense_vector(invalid, 2, "cosine")
        self.assertEqual(migration.dense_vector([0.0, 0.0], 2, "dot"), [0.0, 0.0])
        self.assertEqual(migration.dense_vector([3.0, 4.0], 2, "cosine"),
                         [migration.struct.unpack("<f", migration.struct.pack("<f", x))[0] for x in (0.6, 0.8)])

    def test_schema_rejects_missing_or_unsupported_vectors(self):
        for dtype in (pa.list_(pa.float32()), pa.list_(pa.uint8(), 3), pa.list_(pa.list_(pa.float32(), 3))):
            with self.subTest(dtype=dtype), self.assertRaises(ValueError):
                migration.vector_dimensions(pa.schema([("id", pa.string()), ("vector", dtype)]), self.config)
        with self.assertRaises(ValueError):
            migration.vector_dimensions(pa.schema([("id", pa.string())]), self.config)
        blob_schema = SCHEMA.append(pa.field("blob", pa.large_binary(), metadata={b"lance-encoding:blob": b"true"}))
        with self.assertRaisesRegex(ValueError, "blob"):
            migration.vector_dimensions(blob_schema, self.config)

    def test_configuration_rejects_invalid_settings(self):
        for overrides in ({"batch_size": 0}, {"batch_delay": -1}, {"version": -1},
                          {"vector_columns": []}, {"vector_columns": ["vector", "vector"]},
                          {"metrics": {"missing": "dot"}}, {"metrics": {"vector": "invalid"}},
                          {"offsets_collection": "target"}, {"vector_columns": ["id"]}):
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                replace(self.config, **overrides)

    def test_grpc_endpoint_and_credentials(self):
        config = replace(self.config, qdrant_url="https://example.com:7443", qdrant_api_key="secret")
        with patch.object(migration, "QdrantClient") as constructor:
            migration.target_client(config)
        self.assertEqual(constructor.call_args.kwargs["grpc_port"], 7443)
        self.assertTrue(constructor.call_args.kwargs["https"])
        self.assertTrue(constructor.call_args.kwargs["prefer_grpc"])
        self.assertEqual(constructor.call_args.kwargs["api_key"], "secret")
        for url in ("localhost:6334", "https://host/path", "http://user:pass@host", "http://host:bad"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                migration.target_client(replace(config, qdrant_url=url))

    def test_unconfirmed_upsert_is_a_failure(self):
        client = SimpleNamespace(upsert=lambda *args, **kwargs: SimpleNamespace(status=models.UpdateStatus.ACKNOWLEDGED))
        with self.assertRaisesRegex(RuntimeError, "did not confirm"):
            migration.write_points(client, "target", [])


class SnapshotMigrationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.config = migration.Config(uri=str(Path(self.directory.name) / "source"),
                                       table="docs", id_column="id", collection="target",
                                       batch_size=4, metrics={"vector": "euclid"})
        self.db = lancedb.connect(self.config.uri)
        self.table = self.db.create_table("docs", pa.Table.from_pylist(rows(), schema=SCHEMA))
        self.client = QdrantClient(":memory:")
        self.addCleanup(self.client.close)

    def checkpoint(self):
        return migration.load_checkpoint(self.client, self.config)

    def assert_target(self, expected):
        self.assertEqual(self.client.count("target", exact=True).count, len(expected))
        for row in expected:
            record = self.client.retrieve("target", ids=[migration.point_id(self.config, row["id"])],
                                          with_payload=True, with_vectors=True)[0]
            self.assertEqual(record.payload, {key: value for key, value in row.items() if key != "vector"})
            self.assertEqual(record.vector, {"vector": row["vector"]})

    def test_all_rows_multiple_batches_payloads_and_repeat(self):
        unrelated_id = "00000000-0000-0000-0000-000000000001"
        self.client.create_collection("_migration_offsets", vectors_config={})
        self.client.upsert("_migration_offsets", points=[models.PointStruct(id=unrelated_id, vector={}, payload={"keep": True})], wait=True)
        self.assertEqual(migration.migrate(self.config, self.client, self.table), 13)
        self.assert_target(rows())
        self.assertTrue(self.checkpoint()["complete"])
        self.assertEqual(len(self.client.retrieve("_migration_offsets", ids=[unrelated_id])), 1)
        with patch.object(self.client, "upsert", wraps=self.client.upsert) as upsert:
            migration.migrate(self.config, self.client, self.db.open_table("docs"))
        self.assertTrue(all(call.args[0] == "_migration_offsets" for call in upsert.call_args_list))
        self.assert_target(rows())

    def test_resume_after_failures_and_ambiguous_acknowledgement(self):
        for checkpoint_failure, after_apply in ((False, False), (False, True), (True, False)):
            with self.subTest(checkpoint_failure=checkpoint_failure, after_apply=after_apply):
                config = replace(self.config, restart=True)
                faulty = FaultClient(self.client, "target", checkpoint_failure, after_apply)
                with self.assertRaisesRegex(RuntimeError, "injected"):
                    migration.migrate(config, faulty, self.db.open_table("docs"))
                self.assertEqual(self.checkpoint()["migrated"], 4)
                migration.migrate(replace(self.config, batch_size=3), self.client, self.db.open_table("docs"))
                self.assert_target(rows())

    def test_source_append_does_not_change_resume_snapshot(self):
        with self.assertRaises(RuntimeError):
            migration.migrate(self.config, FaultClient(self.client, "target"), self.table)
        version = self.checkpoint()["version"]
        writer = self.db.open_table("docs")
        writer.add(pa.Table.from_pylist([dict(rows(1)[0], id="new")], schema=SCHEMA))
        self.assertGreater(writer.version, version)
        migration.migrate(self.config, self.client, self.db.open_table("docs"))
        self.assert_target(rows())
        migration.migrate(replace(self.config, restart=True), self.client, self.db.open_table("docs"))
        self.assertEqual(self.client.count("target", exact=True).count, 14)

    def test_duplicate_id_fails_before_target_writes(self):
        self.table.add(pa.Table.from_pylist([rows(1)[0]], schema=SCHEMA))
        with self.assertRaisesRegex(ValueError, "Duplicate ID"):
            migration.migrate(self.config, self.client, self.table)
        self.assertFalse(self.client.collection_exists("target"))
        self.assertFalse(self.client.collection_exists("_migration_offsets"))

    def test_null_id_fails_before_target_writes(self):
        self.table.add(pa.Table.from_pylist([dict(rows(1)[0], id=None)], schema=SCHEMA))
        with self.assertRaisesRegex(ValueError, "non-null"):
            migration.migrate(self.config, self.client, self.table)
        self.assertFalse(self.client.collection_exists("target"))

    def test_empty_table_retains_dimensions(self):
        table = self.db.create_table("empty", schema=SCHEMA)
        config = replace(self.config, table="empty")
        self.assertEqual(migration.migrate(config, self.client, table), 0)
        self.assertEqual(self.client.count("target", exact=True).count, 0)

    def test_multiple_named_vectors_and_cosine_normalization(self):
        schema = SCHEMA.append(pa.field("image", pa.list_(pa.float64(), 2)))
        data = [dict(row, image=[3.0, 4.0]) for row in rows()]
        table = self.db.create_table("multi", pa.Table.from_pylist(data, schema=schema))
        config = replace(self.config, table="multi", vector_columns=["vector", "image"], metrics={"vector": "dot", "image": "cosine"})
        self.assertEqual(migration.migrate(config, self.client, table), 13)
        point = self.client.retrieve("target", ids=[migration.point_id(config, "doc-0")], with_vectors=True)[0]
        self.assertAlmostEqual(point.vector["image"][0], 0.6, places=5)
        self.assertAlmostEqual(point.vector["image"][1], 0.8, places=5)

    def test_existing_schema_mismatch_and_creation_disabled(self):
        with self.assertRaisesRegex(ValueError, "creation is disabled"):
            migration.migrate(replace(self.config, create_collection=False), self.client, self.table)
        for vectors in (
            models.VectorParams(size=3, distance=models.Distance.EUCLID),
            {"vector": models.VectorParams(size=4, distance=models.Distance.EUCLID)},
            {"vector": models.VectorParams(size=3, distance=models.Distance.DOT)},
        ):
            with self.subTest(vectors=vectors):
                self.client.create_collection("target", vectors_config=vectors)
                with self.assertRaises(ValueError):
                    migration.migrate(self.config, self.client, self.table)
                self.assertFalse(self.client.collection_exists("_migration_offsets"))
                self.client.delete_collection("target")

    def test_changed_mapping_and_version_are_rejected(self):
        migration.migrate(self.config, self.client, self.table)
        for config in (replace(self.config, metrics={"vector": "dot"}),
                       replace(self.config, version=self.table.version + 1)):
            with self.subTest(config=config), self.assertRaises(ValueError):
                migration.migrate(config, self.client, self.table)

    def test_target_removed_or_missing_points_detected(self):
        migration.migrate(self.config, self.client, self.table)
        self.client.delete("target", points_selector=models.PointIdsList(points=[migration.point_id(self.config, "doc-0")]), wait=True)
        with self.assertRaisesRegex(RuntimeError, "missing IDs"):
            migration.migrate(self.config, self.client, self.table)
        migration.migrate(replace(self.config, restart=True), self.client, self.table)
        self.assert_target(rows())
        self.client.delete_collection("target")
        with self.assertRaisesRegex(ValueError, "target collection is missing"):
            migration.migrate(self.config, self.client, self.table)

    def test_target_payload_and_vector_corruption_prevents_completion(self):
        migration.migrate(self.config, self.client, self.table)
        identifier = migration.point_id(self.config, "doc-0")
        for field in ("payload", "vector"):
            with self.subTest(field=field):
                record = self.client.retrieve("target", ids=[identifier], with_payload=True, with_vectors=True)[0]
                point = {"id": identifier, "payload": record.payload, "vector": record.vector}
                if field == "payload":
                    point["payload"]["text"] = "corrupted"
                else:
                    point["vector"]["vector"] = [100.0, 100.0, 100.0]
                self.client.upsert("target", points=[models.PointStruct(**point)], wait=True)
                with self.assertRaisesRegex(RuntimeError, f"Target {field} mismatch"):
                    migration.migrate(self.config, self.client, self.table)
                self.assertFalse(self.checkpoint()["complete"])
                migration.migrate(replace(self.config, restart=True), self.client, self.table)
                self.assert_target(rows())

    def test_corrupt_checkpoint_position_fails_before_upsert(self):
        migration.migrate(self.config, self.client, self.table)
        self.client.set_payload("_migration_offsets", payload={"migrated": 999},
                                points=[self.config.checkpoint_id], wait=True)
        with patch.object(self.client, "upsert", wraps=self.client.upsert) as upsert:
            with self.assertRaisesRegex(ValueError, "Checkpoint count"):
                migration.migrate(self.config, self.client, self.table)
            upsert.assert_not_called()

    def test_stage_order_independence_and_incomplete_scan(self):
        class ScanTable:
            def __init__(self, data, count):
                self.data, self.count = data, count
            def count_rows(self):
                return self.count
            def search(self):
                return self
            def limit(self, value):
                if value is not None:
                    raise AssertionError("scan must be unlimited")
                return self
            def to_batches(self, batch_size):
                table = pa.Table.from_pylist(self.data, schema=SCHEMA)
                return pa.RecordBatchReader.from_batches(SCHEMA, table.to_batches(max_chunksize=batch_size))

        digests = []
        for data in (rows(), list(reversed(rows()))):
            with sqlite3.connect(":memory:") as spool:
                digests.append(migration.stage_table(ScanTable(data, 13), self.config, spool, {"vector": 3})[1])
        self.assertEqual(*digests)
        with sqlite3.connect(":memory:") as spool, self.assertRaisesRegex(ValueError, "Incomplete source scan"):
            migration.stage_table(ScanTable(rows(3), 13), self.config, spool, {"vector": 3})

    def test_mutated_source_digest_rejected_before_writes(self):
        migration.migrate(self.config, self.client, self.table)
        with patch.object(migration, "stage_table", return_value=(13, "changed-digest")):
            with patch.object(self.client, "upsert", wraps=self.client.upsert) as upsert:
                with self.assertRaisesRegex(ValueError, "does not match"):
                    migration.migrate(self.config, self.client, self.table)
                upsert.assert_not_called()


@unittest.skipUnless(os.environ.get("LANCEDB_MIGRATION_BINARY") and os.environ.get("LANCEDB_TEST_QDRANT_URL"),
                     "Set LANCEDB_MIGRATION_BINARY and LANCEDB_TEST_QDRANT_URL to opt in")
class LiveCLITests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        suffix = uuid.uuid4().hex
        self.config = migration.Config(
            uri=str(Path(self.directory.name) / "source"), table="docs", id_column="id",
            collection="lancedb_test_" + suffix, offsets_collection="lancedb_offsets_" + suffix,
            qdrant_url=os.environ["LANCEDB_TEST_QDRANT_URL"],
            qdrant_api_key=os.environ.get("LANCEDB_TEST_QDRANT_API_KEY", ""),
        )
        self.client = migration.target_client(self.config)
        self.addCleanup(self.client.close)
        self.addCleanup(self.cleanup_collections)
        self.schema = pa.schema([("id", pa.int64()), ("vector", pa.list_(pa.float32(), 3)), ("text", pa.string())])
        self.rows = [{"id": i, "vector": [float(i), 1.0, -1.0], "text": f"row {i}"} for i in range(23)]
        self.db = lancedb.connect(self.config.uri)
        self.table = self.db.create_table("docs", pa.Table.from_pylist(self.rows, schema=self.schema))

    def cleanup_collections(self):
        for name in (self.config.collection, self.config.offsets_collection):
            if self.client.collection_exists(name):
                self.client.delete_collection(name)

    def run_cli(self):
        command = [str(Path(os.environ["LANCEDB_MIGRATION_BINARY"]).resolve()), "lancedb",
                   "--lancedb.uri", self.config.uri, "--lancedb.table", "docs",
                   "--lancedb.id-column", "id", "--qdrant.url", self.config.qdrant_url,
                   "--qdrant.collection", self.config.collection,
                   "--qdrant.distance-metric", "vector=euclid",
                   "--migration.offsets-collection", self.config.offsets_collection,
                   "--migration.batch-size", "4"]
        if self.config.qdrant_api_key:
            command.extend(["--qdrant.api-key", self.config.qdrant_api_key])
        return subprocess.run(command, cwd=self.directory.name, text=True, capture_output=True, timeout=180)

    def test_cli_real_grpc_and_idempotent_rerun(self):
        for _ in range(2):
            result = self.run_cli()
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("verified 23 IDs, payloads and vectors", result.stdout)
            self.assertEqual(self.client.count(self.config.collection, exact=True).count, 23)
        records, _ = self.client.scroll(self.config.collection, limit=100, with_payload=True, with_vectors=True)
        actual = {record.payload["id"]: record for record in records}
        self.assertEqual(set(actual), set(range(23)))
        for row in self.rows:
            self.assertEqual(actual[row["id"]].payload, {"id": row["id"], "text": row["text"]})
            self.assertEqual(actual[row["id"]].vector, {"vector": row["vector"]})

    def test_cli_duplicate_id_fails_without_target_writes(self):
        self.table.add(pa.Table.from_pylist([self.rows[0]], schema=self.schema))
        result = self.run_cli()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Duplicate ID", result.stderr)
        self.assertFalse(self.client.collection_exists(self.config.collection))
        self.assertFalse(self.client.collection_exists(self.config.offsets_collection))


if __name__ == "__main__":
    unittest.main()
