# Usage:
# python3 create_faiss_index.py <dimension> <total_vectors> <output_path> <vector1> <vector2> ...

import sys


def create_faiss_index(dimension, total_vectors, output_path, vectors_data):
    import faiss
    import numpy as np

    vectors = []
    for i in range(total_vectors):
        vector_str = vectors_data[i]
        vector = [float(x) for x in vector_str.split(",")]
        vectors.append(vector)

    vectors = np.array(vectors, dtype="float32")
    index = faiss.IndexFlatL2(dimension)
    index.add(vectors)
    faiss.write_index(index, output_path)


if __name__ == "__main__":
    dimension = int(sys.argv[1])
    total_vectors = int(sys.argv[2])
    output_path = sys.argv[3]
    vectors_data = sys.argv[4 : 4 + total_vectors]

    create_faiss_index(dimension, total_vectors, output_path, vectors_data)
