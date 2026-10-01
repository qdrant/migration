#!/usr/bin/env python3
"""
Unit and integration tests for sync_cluster.py.
Validates cluster data synchronization logic, collection discovery,
schema extraction, incremental streaming, and robust error guarding.
"""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock
from qdrant_client import QdrantClient, models

import sync_cluster


class TestClusterDataSync(unittest.TestCase):
    """Test suite covering Qdrant cluster data sync functionality."""

    def setUp(self) -> None:
        self.source_client = QdrantClient(":memory:")
        self.target_client = QdrantClient(":memory:")

    def test_verify_connectivity_success(self) -> None:
        """Verifies that connectivity checks pass on accessible clusters."""
        sync_cluster.verify_connectivity(
            client=self.source_client,
            cluster_label="source",
            url="memory://source",
        )
        sync_cluster.verify_connectivity(
            client=self.target_client,
            cluster_label="target",
            url="memory://target",
        )

    def test_collection_discovery_all(self) -> None:
        """Verifies auto-discovery of all existing collections when none are specified."""
        self.source_client.create_collection(
            collection_name="users_col",
            vectors_config=models.VectorParams(size=4, distance=models.Distance.COSINE),
        )
        self.source_client.create_collection(
            collection_name="products_col",
            vectors_config=models.VectorParams(size=8, distance=models.Distance.DOT),
        )

        discovered = sync_cluster.get_target_collections(self.source_client)
        self.assertEqual(sorted(discovered), ["products_col", "users_col"])

    def test_collection_discovery_selective(self) -> None:
        """Verifies filtering when a specific comma-separated list of collections is given."""
        self.source_client.create_collection(
            collection_name="col1",
            vectors_config=models.VectorParams(size=4, distance=models.Distance.COSINE),
        )
        self.source_client.create_collection(
            collection_name="col2",
            vectors_config=models.VectorParams(size=8, distance=models.Distance.DOT),
        )

        selected = sync_cluster.get_target_collections(
            source_client=self.source_client,
            requested_collections="col1,missing_col",
        )
        self.assertEqual(selected, ["col1"])

    def test_ensure_target_collection_provisioning(self) -> None:
        """Verifies that target collections are provisioned with matching parameters."""
        self.source_client.create_collection(
            collection_name="test_schema",
            vectors_config=models.VectorParams(size=128, distance=models.Distance.EUCLID),
        )
        self.assertFalse(self.target_client.collection_exists("test_schema"))

        sync_cluster.ensure_target_collection_exists(
            source_client=self.source_client,
            target_client=self.target_client,
            collection_name="test_schema",
        )

        self.assertTrue(self.target_client.collection_exists("test_schema"))
        target_info = self.target_client.get_collection("test_schema")
        self.assertEqual(target_info.config.params.vectors.size, 128)
        self.assertEqual(target_info.config.params.vectors.distance, models.Distance.EUCLID)

    def test_stream_points_and_sync_single_vector(self) -> None:
        """Verifies end-to-end streaming of points with payloads for standard single vector."""
        self.source_client.create_collection(
            collection_name="articles",
            vectors_config=models.VectorParams(size=2, distance=models.Distance.COSINE),
        )
        points_to_insert = [
            models.PointStruct(
                id=i,
                vector=[0.1 * i, 0.2 * i],
                payload={"title": f"article_{i}", "category": "tech"},
            )
            for i in range(12)
        ]
        self.source_client.upsert(collection_name="articles", points=points_to_insert)

        result = sync_cluster.sync_collection(
            source_client=self.source_client,
            target_client=self.target_client,
            collection_name="articles",
            batch_size=5,
            wait_for_upsert=True,
        )

        self.assertEqual(result.status, "SUCCESS")
        self.assertEqual(result.synced_points, 12)
        self.assertEqual(self.target_client.get_collection("articles").points_count, 12)

    def test_multi_vector_collection_sync(self) -> None:
        """Verifies synchronization for collections using named multi-vectors."""
        self.source_client.create_collection(
            collection_name="multimodal",
            vectors_config={
                "text_dense": models.VectorParams(size=2, distance=models.Distance.COSINE),
                "image_dense": models.VectorParams(size=3, distance=models.Distance.DOT),
            },
        )
        points = [
            models.PointStruct(
                id=i,
                vector={
                    "text_dense": [0.1 * i, 0.2 * i],
                    "image_dense": [0.1, 0.2, 0.3],
                },
                payload={"sku": f"sku_{i}"},
            )
            for i in range(6)
        ]
        self.source_client.upsert(collection_name="multimodal", points=points)

        result = sync_cluster.sync_collection(
            source_client=self.source_client,
            target_client=self.target_client,
            collection_name="multimodal",
            batch_size=4,
            wait_for_upsert=True,
        )

        self.assertEqual(result.status, "SUCCESS")
        self.assertEqual(result.synced_points, 6)
        self.assertEqual(self.target_client.get_collection("multimodal").points_count, 6)

    def test_error_guarding_graceful_proceed(self) -> None:
        """Verifies that an error in one collection does not halt synchronization of others."""
        self.source_client.create_collection(
            collection_name="faulty_col",
            vectors_config=models.VectorParams(size=2, distance=models.Distance.COSINE),
        )
        self.source_client.create_collection(
            collection_name="healthy_col",
            vectors_config=models.VectorParams(size=2, distance=models.Distance.COSINE),
        )
        self.source_client.upsert(
            collection_name="healthy_col",
            points=[models.PointStruct(id=1, vector=[0.1, 0.2], payload={"ok": True})],
        )

        # Mock a broken target client that fails on faulty_col
        mock_target = MagicMock()
        mock_target.collection_exists.side_effect = Exception("Node timeout / connection refused")

        faulty_result = sync_cluster.sync_collection(
            source_client=self.source_client,
            target_client=mock_target,
            collection_name="faulty_col",
            batch_size=10,
            wait_for_upsert=True,
        )
        self.assertEqual(faulty_result.status, "FAILED")
        self.assertIn("Node timeout", faulty_result.error_message)

        # Ensure healthy collection sync continues normally
        healthy_result = sync_cluster.sync_collection(
            source_client=self.source_client,
            target_client=self.target_client,
            collection_name="healthy_col",
            batch_size=10,
            wait_for_upsert=True,
        )
        self.assertEqual(healthy_result.status, "SUCCESS")
        self.assertEqual(healthy_result.synced_points, 1)


if __name__ == "__main__":
    unittest.main()
