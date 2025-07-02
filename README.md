# ⛟ Migration → Qdrant

CLI tool for migrating data to [Qdrant](http://qdrant.tech) with support for resumable transfers in case of interruptions.

> [!WARNING]  
> This project is in beta. The API may change in future releases.

## Supported Sources

* Chroma
* Pinecone
* Milvus
* Weaviate
* Redis
* MongoDB
* OpenSearch
* Another Qdrant instance

## Installation

You can run this tool on any machine with connectivity to both the source and the Qdrant database. For best performance, use a machine with a fast network and minimal latency to both endpoints.

The tool is distributed as a container image, which can be run on any system with Docker, Podman or similar container runtimes. The following examples use Docker.

To get the latest container image run the following command:

```bash
docker pull registry.cloud.qdrant.io/library/qdrant-migration
```

## How To Migrate?

> Click each to expand

<details>

<summary><h3>From Chroma</h3></summary>

Migrate data from a **Chroma** database to **Qdrant**:

### 📥 Example

```bash
docker run --net=host --rm -it registry.cloud.qdrant.io/library/qdrant-migration chroma \
    --chroma.url=http://localhost:8000
    --chroma.collection 'collection-name' \
    --qdrant.url 'https://example.cloud-region.cloud-provider.cloud.qdrant.io:6334' \
    --qdrant.api-key 'optional-qdrant-api-key' \
    --qdrant.collection 'target-collection' \
    --migration.batch-size 64
```

### Chroma Options

| Flag                    | Description                                                              |
| ----------------------- | ------------------------------------------------------------------------ |
| `--chroma.collection`   | Chroma collection name.                                                  |
| `--chroma.url`          | Chroma server URL Default: `"http://localhost:8000"`                     |
| `--chroma.tenant`       | Chroma tenant. Optional.                                                 |
| `--chroma.auth-type`    | Authentication type. `"basic"` or `"token"`. Optional.                   |
| `--chroma.username`     | Username for basic authentication. Optional.                             |
| `--chroma.password`     | Password for basic authentication. Optional.                             |
| `--chroma.token`        | Token for token authentication. Optional.                                |
| `--chroma.token-header` | Token header for authentication. Optional.                               |
| `--chroma.database`     | Database name. Optional.                                                 |

### Qdrant Options

| Flag                      | Description                                                                                                      |
| ------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `--qdrant.collection`     | Target collection name.                                                                                          |
| `--qdrant.url`            | Qdrant gRPC URL. Default: `"http://localhost:6334"`                                                              |
| `--qdrant.api-key`        | Qdrant API key. Optional.                                                                                        |
| `--qdrant.dense-vector`   | Name of the dense vector in Qdrant. Default: `"dense_vector"`                                                    |
| `--qdrant.id-field`       | Field storing Chroma IDs in Qdrant. Default: `"__id__"`                                                          |
| `--qdrant.distance`       | Distance metric for the Qdrant collection. `"cosine"`, `"dot"`, `"manhattan"` or `"euclid"`. Default: `"euclid"` |
| `--qdrant.document-field` | Field storing Chroma documents in Qdrant. Default: `"document"`                                                  |

* See [Shared Migration Options](#shared-migration-options) for common migration parameters.

</details>

<details>

<summary><h3>From Pinecone</h3></summary>

Migrate data from a **Pinecone** database to **Qdrant**:

> IMPORTANT ⚠️:
> Only Pinecone serverless indexes support listing all vectors for migration. [Reference](https://docs.pinecone.io/reference/api/2025-01/data-plane/list)

### 📥 Example

```bash
docker run --net=host --rm -it registry.cloud.qdrant.io/library/qdrant-migration pinecone \
    --pinecone.index-host 'https://example-index.svc.region.pinecone.io' \
    --pinecone.index-name 'example-index' \
    --pinecone.api-key 'optional-pinecone-api-key' \
    --qdrant.url 'https://example.cloud-region.cloud-provider.cloud.qdrant.io:6334' \
    --qdrant.api-key 'optional-qdrant-api-key' \
    --qdrant.collection 'target-collection' \
    --migration.batch-size 64
```

#### Pinecone Options

| Flag                            | Description                                                     |
| ------------------------------- | --------------------------------------------------------------- |
| `--pinecone.index-name`              | Pinecone index name.                                       |
| `--pinecone.index-host`         | Pinecone index host URL (e.g., `https://your-pinecone-url`)     |
| `--pinecone.api-key`            | Pinecone API key for authentication.                            |
| `--pinecone.namespace`          | Namespace of the partition to migrate. Optional.                |
| `--pinecone.service-host`       | Pinecone service host URL. Optional.                            |

#### Qdrant Options

| Flag                            | Description                                                     |
| ------------------------------- | --------------------------------------------------------------- |
| `--qdrant.collection`           | Target collection name                                          |
| `--qdrant.url`                  | Qdrant gRPC URL. Default: `"http://localhost:6334"`             |
| `--qdrant.api-key`              | Qdrant API key                                                  |
| `--qdrant.dense-vector`         | Name of the dense vector in Qdrant. Default: `"dense_vector"`   |
| `--qdrant.sparse-vector`        | Name of the sparse vector in Qdrant. Default: `"sparse_vector"` |
| `--qdrant.id-field`             | Field storing Pinecone IDs in Qdrant. Default: `"__id__"`       |

* See [Shared Migration Options](#shared-migration-options) for common migration parameters.

</details>

<details>

<summary><h3>From Milvus</h3></summary>

Migrate data from a **Milvus** database to **Qdrant**:

### 📥 Example

```bash
docker run --net=host --rm -it registry.cloud.qdrant.io/library/qdrant-migration milvus \
    --milvus.url 'https://example.gcp-us-west1.cloud.zilliz.com' \
    --milvus.enable-tls-auth \
    --milvus.collection 'example-collection' \
    --milvus.api-key 'optional-milvus-api-key' \
    --qdrant.url 'https://example.cloud-region.cloud-provider.cloud.qdrant.io:6334' \
    --qdrant.api-key 'optional-qdrant-api-key' \
    --qdrant.collection 'target-collection' \
    --migration.batch-size 64
```

#### Milvus Options

| Flag                       | Description                                             |
| -------------------------- | ------------------------------------------------------- |
| `--milvus.url`             | Milvus URL (e.g. `https://your-milvus-hostname`)        |
| `--milvus.collection`      | Milvus collection name                                  |
| `--milvus.api-key`         | Milvus API key for authentication                       |
| `--milvus.enable-tls-auth` | Whether to enable TLS Auth                              |
| `--milvus.username`        | Username for Milvus                                     |
| `--milvus.password`        | Password for Milvus                                     |
| `--milvus.db-name`         | Optional database name                                  |
| `--milvus.server-version`  | Milvus server version                                   |
| `--milvus.partitions`      | List of partition names                                 |

#### Qdrant Options

| Flag                       | Description                                                                                                      |
| -------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `--qdrant.url`             | Qdrant gRPC URL. Default: `"http://localhost:6334"`                                                              |
| `--qdrant.collection`      | Target collection name                                                                                           |
| `--qdrant.api-key`         | Qdrant API key                                                                                                   |
| `--qdrant.distance-metric` | Map of vector field names to distance metrics (`"cosine"`,`"dot"`,`"euclid"`,`"manhattan"`). Default: `"cosine"` |

* See [Shared Migration Options](#shared-migration-options) for common migration parameters.

</details>

<details>

<summary><h3>From Weaviate</h3></summary>

Migrate data from a **Weaviate** database to **Qdrant**:

### 📥 Example

> Important ⚠️:
 > Weaviate [does not expose](https://forum.weaviate.io/t/get-vector-dimension-of-a-collection/1769/) vector dimensions and distance metric after a collection is created.
 > Therefore, you must [manually create](https://qdrant.tech/documentation/concepts/collections/#create-a-collection) a Qdrant collection before starting the migration.
 > Ensure that the **vector dimensions in Qdrant exactly match** those used in Weaviate.

```bash
docker run --net=host --rm -it registry.cloud.qdrant.io/library/qdrant-migration weaviate \
    --weaviate.host 'example.c0.asia-southeast1.gcp.weaviate.cloud' \
    --weaviate.scheme 'https' \
    --weaviate.auth-type 'apiKey' \
    --weaviate.api-key 'optional-api-key' \
    --weaviate.class-name 'ExampleClass' \
    --qdrant.url 'http://localhost:6334' \
    --qdrant.collection 'target-collection' \
    --migration.batch-size 64
```

#### Weaviate Options

| Flag                       | Description                                                                                      |
| -------------------------- | ------------------------------------------------------------------------------------------------ |
| `--weaviate.host`          | Host of the Weaviate instance (e.g. `localhost:8080`) **(required)**                             |
| `--weaviate.scheme`        | Scheme of the Weaviate instance (e.g. `http` or `https`) _(default: http)_                       |
| `--weaviate.class-name`    | Name of the Weaviate class to migrate **(required)**                                             |
| `--weaviate.auth-type`     | Authentication type _(default: none)_. Options: `none`, `apiKey`, `password`, `client`, `bearer` |
| `--weaviate.api-key`       | API key for authentication (when `auth-type` is `apiKey`)                                        |
| `--weaviate.username`      | Username for authentication (when `auth-type` is `password`)                                     |
| `--weaviate.password`      | Password for authentication (when `auth-type` is `password`)                                     |
| `--weaviate.scopes`        | Scopes for authentication (when `auth-type` is `password` or `client`)                           |
| `--weaviate.client-secret` | Client secret for authentication (when `auth-type` is `client`)                                  |
| `--weaviate.token`         | Bearer token for authentication (when `auth-type` is `bearer`)                                   |
| `--weaviate.refresh-token` | Refresh token for authentication (when `auth-type` is `bearer`)                                  |
| `--weaviate.expires-in`    | Access token expiration time in seconds (when `auth-type` is `bearer`)                           |
| `--weaviate.tenant`        | Objects belonging to which tenant to migrate                                                     |

#### Qdrant Options

| Flag                    | Description                                                                                                      |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `--qdrant.url`          | Qdrant gRPC URL. Default: `"http://localhost:6334"`                                                              |
| `--qdrant.collection`   | Target collection name                                                                                           |
| `--qdrant.api-key`      | Qdrant API key                                                                                                   |

* See [Shared Migration Options](#shared-migration-options) for common migration parameters.

</details>

<details>

<summary><h3>From Redis</h3></summary>

Migrate data from a **Redis** database to **Qdrant**:

> Important ⚠️:
> Redis does not expose vector configurations after an index is created.
> Therefore, you must [manually create](https://qdrant.tech/documentation/concepts/vectors/#named-vectors) a Qdrant collection before starting the migration.
> Ensure that the **vector names and dimensions in Qdrant exactly match** those used in Redis.

### 📥 Example

```bash
docker run --net=host --rm -it registry.cloud.qdrant.io/library/qdrant-migration milvus \
    --redis.index 'index_name' \
    --redis.addr 'localhost:6379' \
    --qdrant.url 'http://localhost:6334' \
    --qdrant.collection 'target-collection' \
    --migration.batch-size 100
```

#### Redis Options

| Flag                  | Description                                                             |
| --------------------- | ----------------------------------------------------------------------- |
| `--redis.index`       | Redis index name                                                        |
| `--redis.addr`        | Redis address in the format `host:port` (default: `localhost:6379`)     |
| `--redis.protocol`    | Redis protocol version (default: `2`)                                   |
| `--redis.password`    | Password to authenticate requests. Optional.                            |
| `--redis.username`    | Username to authenticate requests. Optional.                            |
| `--redis.client-name` | Will execute the `CLIENT SETNAME <NAME>` for each connection. Optional. |
| `--redis.db`          | Database to be selected after connecting to the server. Optional.       |
| `--redis.network`     | Redis network type (`tcp` or `unix`, default: `tcp`)                    |

#### Qdrant Options

| Flag                            | Description                                                     |
| ------------------------------- | --------------------------------------------------------------- |
| `--qdrant.url`                  | Qdrant gRPC URL. Default: `"http://localhost:6334"`             |
| `--qdrant.collection`           | Target collection name                                          |
| `--qdrant.api-key`              | Qdrant API key                                                  |
| `--qdrant.id-field`             | Field storing Redis IDs in Qdrant. Default: `"__id__"`         |

* See [Shared Migration Options](#shared-migration-options) for common migration parameters.

</details>

<details>
<summary><h3>From MongoDB</h3></summary>

Migrate data from a **MongoDB** database to **Qdrant**:

> Important ⚠️:
> You must [manually create](https://qdrant.tech/documentation/concepts/vectors/#named-vectors) a Qdrant collection before starting the migration.
> Ensure that the **vector names and dimensions in Qdrant exactly match** those used in MongoDB.

### 📥 Example

```bash
docker run --net=host --rm -it registry.cloud.qdrant.io/library/qdrant-migration mongodb \
    --mongodb.url 'mongodb+srv://<username>:<password>@<cluster-url>/mydb' \
    --mongodb.database 'mydb' \
    --mongodb.collection 'mycollection' \
    --qdrant.url 'http://localhost:6334' \
    --qdrant.collection 'target-collection' \
    --migration.batch-size 64
```

#### MongoDB Options

| Flag                   | Description               |
| ---------------------- | ------------------------- |
| `--mongodb.url`        | MongoDB connection string |
| `--mongodb.database`   | MongoDB database name     |
| `--mongodb.collection` | MongoDB collection name   |

#### Qdrant Options

| Flag                  | Description                                              |
| --------------------- | -------------------------------------------------------- |
| `--qdrant.url`        | Qdrant gRPC URL. Default: `"http://localhost:6334"`      |
| `--qdrant.collection` | Target collection name                                   |
| `--qdrant.api-key`    | Qdrant API key (optional)                                |
| `--qdrant.id-field`   | Field storing MongoDB IDs in Qdrant. Default: `"__id__"` |

See [Shared Migration Options](#shared-migration-options) for common migration parameters.

</details>

<details>
<summary><h3>From OpenSearch</h3></summary>

Migrate data from an **OpenSearch** index to **Qdrant**:

### 📥 Example

```bash
docker run --net=host --rm -it registry.cloud.qdrant.io/library/qdrant-migration opensearch \
    --opensearch.url 'https://localhost:9200' \
    --opensearch.insecure-skip-verify \
    --opensearch.index 'source-index' \
    --opensearch.username 'username' \
    --opensearch.password 'password' \
    --qdrant.url 'http://localhost:6334' \
    --qdrant.api-key 'optional-qdrant-api-key' \
    --qdrant.collection 'target-collection' \
    --migration.batch-size 64
```

#### OpenSearch Options

| Flag                                | Description                                            |
| ----------------------------------- | ------------------------------------------------------ |
| `--opensearch.url`                  | OpenSearch URL (e.g. `http://localhost:9200`).         |
| `--opensearch.index`                | OpenSearch index name.                                 |
| `--opensearch.username`             | Username for basic authentication. Optional.           |
| `--opensearch.password`             | Password for basic authentication Optional.            |
| `--opensearch.insecure-skip-verify` | Whether to skip TLS certificate verification Optional. |

#### Qdrant Options

| Flag                  | Description                                                 |
| --------------------- | ----------------------------------------------------------- |
| `--qdrant.url`        | Qdrant gRPC URL. Default: `"http://localhost:6334"`         |
| `--qdrant.collection` | Target collection name                                      |
| `--qdrant.api-key`    | Qdrant API key (optional)                                   |
| `--qdrant.id-field`   | Field storing OpenSearch IDs in Qdrant. Default: `"__id__"` |

See [Shared Migration Options](#shared-migration-options) for common migration parameters.

</details>

<details>
<summary><h3>From Another Qdrant Instance</h3></summary>

Migrate data from one **Qdrant** instance to another.

### 📥 Example

```bash
docker run --net=host --rm -it registry.cloud.qdrant.io/library/qdrant-migration qdrant \
    --source.url 'http://localhost:6334' \
    --source.collection 'source-collection' \
    --target.url 'https://example.cloud-region.cloud-provider.cloud.qdrant.io:6334' \
    --target.api-key 'qdrant-key' \
    --target.collection 'target-collection' \
    --migration.batch-size 64
```

NOTE: If the target collection already exists, its vector size and dimensions must match the source. Other settings like replication, shards can differ.

#### Source Qdrant Options

| Flag                  | Description                                                |
| --------------------- | ---------------------------------------------------------- |
| `--source.collection` | Source collection name                                     |
| `--source.url`        | Source gRPC URL. Default: `"http://localhost:6334"`        |
| `--source.api-key`    | API key for source instance                                |
| `--source.max-message-size`  | Maximum gRPC message size in bytes (default: `33554432` = 32MB). Increase if you encounter `ResourceExhausted` errors with large batches.|

#### Target Qdrant Options

| Flag                              | Description                                         |
| --------------------------------- | --------------------------------------------------- |
| `--target.collection`             | Target collection name                              |
| `--target.url`                    | Target gRPC URL. Default: `"http://localhost:6334"` |
| `--target.api-key`                | API key for target instance                         |
| `--target.ensure-payload-indexes` | Ensure payload indexes exist. Default: true         |

See [Shared Migration Options](#shared-migration-options) for shared parameters.

</details>

### Shared Migration Options

These options apply to all migrations, regardless of the source.

| Flag                                 | Description                                                          |
| ------------------------------------ | -------------------------------------------------------------------- |
| `--migration.batch-size`             | Migration batch size. Default: 50                                    |
| `--migration.restart`                | Restart migration without resuming from offset. Default: false       |
| `--migration.create-collection`      | Create the collection if it doesn't exist. Default: true             |
| `--migration.offsets-collection`     | Collection to store migration offset. Default: `"_migration_offsets"`|
