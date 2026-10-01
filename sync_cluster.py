#!/usr/bin/env python3
"""
Cluster-to-Cluster Data Synchronization for Qdrant.

Designed for issue #347 ("Local deployed cluster data sync").
Synchronizes vector collections and points between a primary Qdrant cluster
and a Disaster Recovery (DR) or secondary cluster. Because Qdrant coordinates
sharding and data replication across its cluster nodes via its consensus layer,
this automation script only needs to establish a connection to one healthy host
in the source cluster and one healthy host in the target cluster.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

from qdrant_client import QdrantClient, models

# Setup structured logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("cluster_sync")


@dataclass
class CollectionSyncResult:
    """Represents the execution outcome of a collection synchronization."""
    collection_name: str
    status: str  # "SUCCESS", "FAILED", or "SKIPPED"
    synced_points: int = 0
    duration_seconds: float = 0.0
    error_message: Optional[str] = None

    @property
    def throughput(self) -> float:
        """Calculates points transferred per second."""
        if self.duration_seconds > 0:
            return self.synced_points / self.duration_seconds
        return 0.0


def parse_arguments() -> argparse.Namespace:
    """Configures and parses CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Synchronize Qdrant collections and points between clusters.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Connections & Cluster Configuration
    parser.add_argument(
        "--source-url",
        type=str,
        default="http://localhost:6333",
        help="URL of an accessible node in the source Qdrant cluster.",
    )
    parser.add_argument(
        "--target-url",
        type=str,
        required=True,
        help="URL of an accessible node in the target Disaster Recovery (DR) Qdrant cluster.",
    )
    parser.add_argument(
        "--source-api-key",
        type=str,
        default=None,
        help="Optional API key for the source cluster.",
    )
    parser.add_argument(
        "--target-api-key",
        type=str,
        default=None,
        help="Optional API key for the target cluster.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="HTTP/gRPC connection and read timeout in seconds.",
    )
    parser.add_argument(
        "--prefer-grpc",
        action="store_true",
        default=False,
        help="Use gRPC protocol instead of HTTP REST for data transfer.",
    )

    # Data Synchronization Parameters
    parser.add_argument(
        "--collections",
        type=str,
        default=None,
        help="Comma-separated list of collection names to sync (e.g., 'col1,col2'). If omitted, all source collections are migrated.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=256,
        help="Batch size for scroll and upsert streaming operations.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum number of retries per batch on transient network or timeout errors.",
    )
    parser.add_argument(
        "--no-wait",
        dest="wait_for_upsert",
        action="store_false",
        help="Disable waiting for target cluster commit confirmation per batch (higher throughput, async indexing).",
    )
    parser.set_defaults(wait_for_upsert=True)

    parser.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help="Enable debug-level logging.",
    )

    return parser.parse_args()


def initialize_client(
    url: str,
    api_key: Optional[str] = None,
    timeout: float = 60.0,
    prefer_grpc: bool = False,
) -> QdrantClient:
    """
    Instantiates a QdrantClient with strict timeouts and configuration.
    """
    return QdrantClient(
        url=url,
        api_key=api_key,
        timeout=timeout,
        prefer_grpc=prefer_grpc,
        check_compatibility=False,
    )


def verify_connectivity(client: QdrantClient, cluster_label: str, url: str) -> None:
    """
    Validates cluster connectivity by querying collection metadata.
    Raises ConnectionError if the host is unreachable or times out.
    """
    logger.info("Verifying connectivity to %s cluster at %s...", cluster_label, url)
    try:
        collections_response = client.get_collections()
        collection_count = len(collections_response.collections)
        logger.info(
            "Connectivity confirmed for %s cluster (%d existing collection%s found).",
            cluster_label,
            collection_count,
            "" if collection_count == 1 else "s",
        )
    except Exception as exc:
        logger.error(
            "Failed to establish connection to %s cluster at %s: %s",
            cluster_label,
            url,
            exc,
        )
        raise ConnectionError(
            f"Unable to connect to {cluster_label} cluster at '{url}': {exc}"
        ) from exc


def get_target_collections(
    source_client: QdrantClient,
    requested_collections: Optional[str] = None,
) -> List[str]:
    """
    Determines the list of collection names to synchronize.
    If specific collections were requested, validates their existence.
    Otherwise, queries the source cluster for all available collections.
    """
    all_source_collections = [
        col.name for col in source_client.get_collections().collections
    ]

    if requested_collections:
        selected = [
            name.strip()
            for name in requested_collections.split(",")
            if name.strip()
        ]
        # Validate that selected collections exist on the source cluster
        missing = [name for name in selected if name not in all_source_collections]
        if missing:
            logger.warning(
                "Requested collections not found in source cluster: %s",
                missing,
            )
        valid_selected = [name for name in selected if name in all_source_collections]
        return valid_selected

    return all_source_collections


def format_vector_params_summary(
    vectors_config: Any,
) -> List[str]:
    """Formats human-readable descriptions of vector configurations."""
    descriptions: List[str] = []
    if isinstance(vectors_config, models.VectorParams):
        distance_str = (
            vectors_config.distance.value
            if hasattr(vectors_config.distance, "value")
            else str(vectors_config.distance)
        )
        descriptions.append(f"default: (size={vectors_config.size}, distance={distance_str})")
    elif isinstance(vectors_config, dict):
        for vec_name, vec_params in vectors_config.items():
            if isinstance(vec_params, models.VectorParams):
                dist_str = (
                    vec_params.distance.value
                    if hasattr(vec_params.distance, "value")
                    else str(vec_params.distance)
                )
                descriptions.append(
                    f"'{vec_name}': (size={vec_params.size}, distance={dist_str})"
                )
            else:
                descriptions.append(f"'{vec_name}': {vec_params}")
    elif vectors_config is None:
        descriptions.append("none")
    else:
        descriptions.append(str(vectors_config))

    return descriptions


def ensure_target_collection_exists(
    source_client: QdrantClient,
    target_client: QdrantClient,
    collection_name: str,
) -> models.CollectionInfo:
    """
    Fetches source collection configuration parameters (vector size, distance metric,
    shard count, replication factor, and optional storage settings). If the collection
    does not exist on the target cluster, automatically creates it using the exact
    configuration extracted from the source cluster.
    """
    source_info = source_client.get_collection(collection_name=collection_name)
    source_params = source_info.config.params

    vector_summaries = format_vector_params_summary(source_params.vectors)
    shard_count = (
        source_params.shard_number
        if source_params.shard_number is not None
        else 1
    )
    replication_factor = (
        source_params.replication_factor
        if source_params.replication_factor is not None
        else 1
    )

    logger.info(
        "Source collection '%s' configuration: vectors=[%s], shards=%s, replication_factor=%s",
        collection_name,
        ", ".join(vector_summaries),
        shard_count,
        replication_factor,
    )

    if target_client.collection_exists(collection_name=collection_name):
        logger.info(
            "Collection '%s' already exists on target cluster. Skipping creation.",
            collection_name,
        )
        return source_info

    logger.info(
        "Collection '%s' missing on target cluster. Provisioning with matching topology...",
        collection_name,
    )

    target_client.create_collection(
        collection_name=collection_name,
        vectors_config=source_params.vectors,
        sparse_vectors_config=source_params.sparse_vectors,
        shard_number=source_params.shard_number,
        sharding_method=source_params.sharding_method,
        replication_factor=source_params.replication_factor,
        write_consistency_factor=source_params.write_consistency_factor,
        on_disk_payload=source_params.on_disk_payload,
        hnsw_config=source_info.config.hnsw_config,
        quantization_config=source_info.config.quantization_config,
        optimizers_config=source_info.config.optimizer_config,
        wal_config=source_info.config.wal_config,
    )

    logger.info(
        "Successfully created collection '%s' on target cluster.",
        collection_name,
    )
    return source_info


def stream_points_for_collection(
    source_client: QdrantClient,
    target_client: QdrantClient,
    collection_name: str,
    total_points: Optional[int],
    batch_size: int,
    wait_for_upsert: bool,
    max_retries: int = 3,
) -> int:
    """
    Incrementally streams points from source to target using scroll and upsert.
    Supports both single vectors and multi/sparse vectors with payload preservation.
    Includes retry logic with backoff for transient batch failures.
    """
    next_page_offset: Optional[Any] = None
    synced_points_count: int = 0
    batch_index: int = 0

    # Initialize progress bar if tqdm is installed
    progress_bar = None
    if tqdm is not None:
        progress_bar = tqdm(
            total=total_points,
            desc=f"Syncing [{collection_name}]",
            unit="pts",
            unit_scale=True,
            dynamic_ncols=True,
            leave=True,
        )

    try:
        while True:
            # 1. Fetch batch from source cluster with retry
            records = None
            for attempt in range(1, max_retries + 1):
                try:
                    records, next_page_offset = source_client.scroll(
                        collection_name=collection_name,
                        limit=batch_size,
                        offset=next_page_offset,
                        with_payload=True,
                        with_vectors=True,
                    )
                    break
                except Exception as fetch_err:
                    if attempt < max_retries:
                        backoff = 2 ** attempt
                        logger.warning(
                            "Scroll error on '%s' batch %d (attempt %d/%d): %s. Retrying in %ds...",
                            collection_name,
                            batch_index,
                            attempt,
                            max_retries,
                            fetch_err,
                            backoff,
                        )
                        time.sleep(backoff)
                    else:
                        raise

            if not records:
                break

            # 2. Transform records into PointStruct models
            points_batch = [
                models.PointStruct(
                    id=record.id,
                    vector=record.vector if record.vector is not None else {},
                    payload=record.payload,
                )
                for record in records
            ]

            # 3. Stream batch to target cluster with retry
            for attempt in range(1, max_retries + 1):
                try:
                    target_client.upsert(
                        collection_name=collection_name,
                        points=points_batch,
                        wait=wait_for_upsert,
                    )
                    break
                except Exception as upsert_err:
                    if attempt < max_retries:
                        backoff = 2 ** attempt
                        logger.warning(
                            "Upsert error on '%s' batch %d (attempt %d/%d): %s. Retrying in %ds...",
                            collection_name,
                            batch_index,
                            attempt,
                            max_retries,
                            upsert_err,
                            backoff,
                        )
                        time.sleep(backoff)
                    else:
                        raise

            batch_size_actual = len(points_batch)
            synced_points_count += batch_size_actual
            batch_index += 1

            if progress_bar is not None:
                progress_bar.update(batch_size_actual)
            elif batch_index % 10 == 0 or next_page_offset is None:
                progress_percent = (
                    f" ({(synced_points_count / total_points * 100):.1f}%)"
                    if total_points
                    else ""
                )
                logger.info(
                    "Collection '%s': %d points migrated%s...",
                    collection_name,
                    synced_points_count,
                    progress_percent,
                )

            if next_page_offset is None:
                break

    finally:
        if progress_bar is not None:
            progress_bar.close()

    return synced_points_count


def sync_collection(
    source_client: QdrantClient,
    target_client: QdrantClient,
    collection_name: str,
    batch_size: int,
    wait_for_upsert: bool,
    max_retries: int = 3,
) -> CollectionSyncResult:
    """
    Orchestrates the replication and data streaming for a single collection.
    Guarded to ensure failures are captured and returned cleanly.
    """
    start_time = time.perf_counter()
    logger.info(">>> Beginning synchronization for collection: '%s'", collection_name)

    try:
        source_info = ensure_target_collection_exists(
            source_client=source_client,
            target_client=target_client,
            collection_name=collection_name,
        )

        total_source_points = source_info.points_count
        logger.info(
            "Collection '%s' contains %s indexed points on source cluster.",
            collection_name,
            f"{total_source_points:,}" if total_source_points is not None else "unknown",
        )

        synced_points = stream_points_for_collection(
            source_client=source_client,
            target_client=target_client,
            collection_name=collection_name,
            total_points=total_source_points,
            batch_size=batch_size,
            wait_for_upsert=wait_for_upsert,
            max_retries=max_retries,
        )

        duration = time.perf_counter() - start_time
        rate = synced_points / duration if duration > 0 else 0.0

        try:
            target_info = target_client.get_collection(collection_name=collection_name)
            target_points_count = target_info.points_count
            logger.info(
                "Synchronization complete for '%s': %d points synced in %.2fs (%.1f pts/sec). Target count: %s",
                collection_name,
                synced_points,
                duration,
                rate,
                f"{target_points_count:,}" if target_points_count is not None else "pending indexing",
            )
        except Exception:
            logger.info(
                "Synchronization complete for '%s': %d points synced in %.2fs (%.1f pts/sec).",
                collection_name,
                synced_points,
                duration,
                rate,
            )

        return CollectionSyncResult(
            collection_name=collection_name,
            status="SUCCESS",
            synced_points=synced_points,
            duration_seconds=duration,
        )

    except Exception as exc:
        duration = time.perf_counter() - start_time
        logger.error(
            "Failed to synchronize collection '%s' after %.2fs: %s",
            collection_name,
            duration,
            exc,
            exc_info=logger.isEnabledFor(logging.DEBUG),
        )
        return CollectionSyncResult(
            collection_name=collection_name,
            status="FAILED",
            synced_points=0,
            duration_seconds=duration,
            error_message=str(exc),
        )


def print_summary_report(
    results: List[CollectionSyncResult],
    source_url: str,
    target_url: str,
    total_elapsed_seconds: float,
) -> None:
    """Prints a structured execution summary report."""
    total_collections = len(results)
    successful_syncs = [r for r in results if r.status == "SUCCESS"]
    failed_syncs = [r for r in results if r.status == "FAILED"]
    total_points = sum(r.synced_points for r in successful_syncs)
    overall_throughput = (
        total_points / total_elapsed_seconds if total_elapsed_seconds > 0 else 0.0
    )

    separator = "=" * 80
    subseparator = "-" * 80

    print("\n" + separator)
    print("                     CLUSTER DATA SYNCHRONIZATION REPORT")
    print(separator)
    print(f"Source Host:          {source_url}")
    print(f"Target Host:          {target_url}")
    print(f"Total Collections:    {total_collections}")
    print(f"Successful Syncs:     {len(successful_syncs)}")
    print(f"Failed Syncs:         {len(failed_syncs)}")
    print(f"Total Points Migrated: {total_points:,}")
    print(f"Total Elapsed Time:   {total_elapsed_seconds:.2f}s")
    print(f"Overall Throughput:   {overall_throughput:,.1f} pts/sec")
    print(subseparator)
    print("Collection Details:")

    for result in results:
        if result.status == "SUCCESS":
            print(
                f"  [SUCCESS] {result.collection_name:<24} | "
                f"{result.synced_points:>10,} pts | "
                f"{result.duration_seconds:>7.2f}s | "
                f"{result.throughput:>9.1f} pts/sec"
            )
        else:
            print(
                f"  [FAILED]  {result.collection_name:<24} | "
                f"Error: {result.error_message}"
            )
    print(separator + "\n")


def main() -> int:
    """Main orchestration entry point."""
    args = parse_arguments()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    script_start_time = time.perf_counter()

    logger.info("Initializing cluster data sync automation...")
    logger.info("Source node: %s", args.source_url)
    logger.info("Target node: %s", args.target_url)
    logger.info("Batch size:  %d", args.batch_size)
    logger.info("Timeout:     %.1fs", args.timeout)

    source_client = initialize_client(
        url=args.source_url,
        api_key=args.source_api_key,
        timeout=args.timeout,
        prefer_grpc=args.prefer_grpc,
    )
    target_client = initialize_client(
        url=args.target_url,
        api_key=args.target_api_key,
        timeout=args.timeout,
        prefer_grpc=args.prefer_grpc,
    )

    try:
        verify_connectivity(source_client, "source", args.source_url)
        verify_connectivity(target_client, "target", args.target_url)
    except ConnectionError as conn_err:
        logger.error("Pre-flight cluster connectivity verification failed: %s", conn_err)
        return 1

    try:
        collections_to_sync = get_target_collections(
            source_client=source_client,
            requested_collections=args.collections,
        )
    except Exception as exc:
        logger.error("Failed to query source collections: %s", exc)
        return 1

    if not collections_to_sync:
        logger.warning("No collections found to synchronize.")
        return 0

    logger.info(
        "Identified %d collection(s) to synchronize: %s",
        len(collections_to_sync),
        collections_to_sync,
    )

    results: List[CollectionSyncResult] = []
    for collection_name in collections_to_sync:
        result = sync_collection(
            source_client=source_client,
            target_client=target_client,
            collection_name=collection_name,
            batch_size=args.batch_size,
            wait_for_upsert=args.wait_for_upsert,
            max_retries=args.max_retries,
        )
        results.append(result)

    total_elapsed = time.perf_counter() - script_start_time
    print_summary_report(
        results=results,
        source_url=args.source_url,
        target_url=args.target_url,
        total_elapsed_seconds=total_elapsed,
    )


    has_failures = any(r.status == "FAILED" for r in results)
    return 1 if has_failures else 0


if __name__ == "__main__":
    sys.exit(main())
