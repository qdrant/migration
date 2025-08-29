import argparse
from urllib.parse import urlparse
import faiss
import numpy as np
from qdrant_client import QdrantClient, models
from tqdm import tqdm
import sys
from typing import Any, List, Tuple


def get_dim(index_path: str) -> None:
    index = faiss.read_index(index_path)
    if hasattr(index, "d"):
        print(index.d)
    elif hasattr(index, "index") and hasattr(index.index, "d"):
        print(index.index.d)
    else:
        print(0)
        sys.exit(1)

def get_total(index_path: str) -> None:
    index = faiss.read_index(index_path)
    if hasattr(index, "ntotal"):
        print(index.ntotal)
    elif hasattr(index, "index") and hasattr(index.index, "ntotal"):
        print(index.index.ntotal)
    else:
        print(0)
        sys.exit(1)

def migrate(args: argparse.Namespace) -> None:
    index: Any = faiss.read_index(args.faiss_index)
    total_vectors: int = index.ntotal

    allowed_indexes: Tuple[Any, ...] = (
        faiss.IndexFlatL2,
        faiss.IndexFlatIP,
        faiss.IndexHNSWFlat,
        faiss.IndexIVFFlat,
    )
    underlying_index: Any = index.index if hasattr(index, "index") else index
    if not isinstance(faiss.downcast_index(underlying_index), allowed_indexes):
        raise TypeError(
            f"Migration is only supported for {', '.join(cls.__name__ for cls in allowed_indexes)}."
        )

    parsed_url = urlparse(args.qdrant_url)
    client: QdrantClient = QdrantClient(
        host=parsed_url.hostname,
        grpc_port=parsed_url.port if parsed_url.port else 6334,
        https=parsed_url.scheme == "https",
        api_key=args.qdrant_api_key,
        prefer_grpc=True,
    )

    with tqdm(total=total_vectors, initial=args.offset) as pbar:
        for start in range(args.offset, total_vectors, args.batch_size):
            end: int = min(start + args.batch_size, total_vectors)
            if isinstance(index, faiss.IndexIDMap):
                vectors_page: np.ndarray = index.index.reconstruct_n(start, end - start)
                ids_page: List[int] = [
                    int(index.id_map.at(i)) for i in range(start, end)
                ]
            else:
                vectors_page: np.ndarray = index.reconstruct_n(start, end - start)
                ids_page: List[int] = list(range(start, end))

            points: List[models.PointStruct] = [
                models.PointStruct(
                    id=ids_page[i],
                    vector=vectors_page[i].tolist(),
                    payload=None,
                )
                for i in range(len(ids_page))
            ]
            client.upsert(collection_name=args.collection, points=points, wait=True)
            pbar.update(len(points))
            # Print new offset to stdout for Go to capture
            print(f"OFFSET:{end}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="FAISS migration utility.")
    parser.add_argument("--action", required=True, choices=["get-dim", "get-total", "migrate"])
    parser.add_argument(
        "--faiss-index", required=True, help="Path to the FAISS index file."
    )
    parser.add_argument("--qdrant-url", help="Qdrant server URL.")
    parser.add_argument("--qdrant-api-key", default=None, help="Qdrant API key.")
    parser.add_argument("--collection", help="Qdrant collection name.")
    parser.add_argument(
        "--batch-size", type=int, default=100, help="Batch size for migration."
    )
    parser.add_argument(
        "--offset", type=int, default=0, help="Start offset for migration."
    )
    args = parser.parse_args()

    if args.action == "get-dim":
        get_dim(args.faiss_index)
    elif args.action == "get-total":
        get_total(args.faiss_index)
    elif args.action == "migrate":
        migrate(args)


if __name__ == "__main__":
    main()
