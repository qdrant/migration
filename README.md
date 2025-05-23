# ⛟ Migration → Qdrant

CLI tool for migrating data to [Qdrant](http://qdrant.tech) with support for resumable transfers in case of interruptions.

> [!WARNING]  
> This project is in beta. The API may change in future releases.

## Supported Sources

* [Chroma](https://trychroma.com/)
* [Pinecone](https://www.pinecone.io/)
* [Milvus](https://milvus.io/)
* [Weaviate](https://weaviate.io/)
* Another [Qdrant](http://qdrant.tech/) instance

## Installation

You can run this tool on any machine with connectivity to both the source and the Qdrant database. For best performance, use a machine with a fast network and minimal latency to both endpoints.

#### Binaries

Each release includes **precompiled binaries** for all major OS and CPU architectures. Download the latest one from the [Releases Page](https://github.com/qdrant/migration/releases).

#### Docker

To get the latest Docker image run the following command.

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
migration chroma \
    --chroma.url=http://localhost:8000
    --chroma.collection 'collection-name' \
    --qdrant.url 'https://example.cloud-region.cloud-provider.cloud.qdrant.io:6334' \
    --qdrant.api-key 'optional-qdrant-api-key' \
    --qdrant.collection 'target-collection' \
    --migration.batch-size 64
````

With Docker:

```bash
docker run --net=host --rm -it registry.cloud.qdrant.io/library/qdrant-migration chroma \
    --chroma.url=http://localhost:8000
    ...
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

> IMPORTANT ⚠️
> Only Pinecone serverless indexes support listing all vectors for migration. [Reference](https://docs.pinecone.io/reference/api/2025-01/data-plane/list)

### 📥 Example

```bash
migration pinecone \
    --pinecone.host 'https://example-index-12345.svc.region.pinecone.io' \
    --pinecone.api-key 'optional-pinecone-api-key' \
    --qdrant.url 'https://example.cloud-region.cloud-provider.cloud.qdrant.io:6334' \
    --qdrant.api-key 'optional-qdrant-api-key' \
    --qdrant.collection 'target-collection' \
    --migration.batch-size 64
````

With Docker:

```bash
docker run --net=host --rm -it registry.cloud.qdrant.io/library/qdrant-migration pinecone \
    --pinecone.host 'https://example-index-12345.svc.region.pinecone.io' \
    ...
```

#### Pinecone Options

| Flag                            | Description                                                     |
| ------------------------------- | --------------------------------------------------------------- |
| `--pinecone.api-key`            | Pinecone API key for authentication                             |
| `--pinecone.host`               | Pinecone index host URL (e.g., `https://your-pinecone-url`)     |
| `--pinecone.namespace`          | Namespace of the partition to migrate                           |

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
migration milvus \
    --milvus.url 'https://example.gcp-us-west1.cloud.zilliz.com' \
    --milvus.enable-tls-auth \
    --milvus.collection 'example-collection' \
    --milvus.api-key 'optional-milvus-api-key' \
    --qdrant.url 'https://example.cloud-region.cloud-provider.cloud.qdrant.io:6334' \
    --qdrant.api-key 'optional-qdrant-api-key' \
    --qdrant.collection 'target-collection' \
    --migration.batch-size 64
```

With Docker:

```bash
docker run --net=host --rm -it registry.cloud.qdrant.io/library/qdrant-migration milvus \
    --milvus.url 'https://example.gcp-us-west1.cloud.zilliz.com' \
    ...
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

| Flag                            | Description                                                     |
| ------------------------------- | --------------------------------------------------------------- |
| `--qdrant.url`                  | Qdrant gRPC URL. Default: `"http://localhost:6334"`             |
| `--qdrant.collection`           | Target collection name                                          |
| `--qdrant.api-key`              | Qdrant API key                                                  |

* See [Shared Migration Options](#shared-migration-options) for common migration parameters.

</details>

<details>

<summary><h3>From Weaviate</h3></summary>

Migrate data from a **Weaviate** database to **Qdrant**:

### 📥 Example

> Important ⚠️:
 > Weaviate does not expose vector dimensions and distance metric after a collection is created. [Reference](https://forum.weaviate.io/t/get-vector-dimension-of-a-collection/1769/).
 > Therefore, you must [manually create](https://qdrant.tech/documentation/concepts/collections/#create-a-collection) a Qdrant collection before starting the migration.
 > Ensure that the **vector dimensions in Qdrant exactly match** those used in Weaviate.

```bash
migration weaviate \
    --weaviate.host 'example.c0.asia-southeast1.gcp.weaviate.cloud' \
    --weaviate.scheme 'https' \
    --weaviate.auth-type 'apiKey' \
    --weaviate.api-key 'optional-api-key' \
    --weaviate.class-name 'ExampleClass' \
    --qdrant.url 'http://localhost:6334' \
    --qdrant.collection 'target-collection' \
    --migration.batch-size 64
```

With Docker:

```bash
docker run --net=host --rm -it registry.cloud.qdrant.io/library/qdrant-migration weaviate \
    --weaviate.host 'example.c0.asia-southeast1.gcp.weaviate.cloud' \
    ...
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
<summary><h3>From Another Qdrant Instance</h3></summary>

Migrate data from one **Qdrant** instance to another.

### 📥 Example

```bash
migration qdrant \
    --source.url 'http://localhost:6334' \
    --source.collection 'source-collection' \
    --target.url 'https://example.cloud-region.cloud-provider.cloud.qdrant.io:6334' \
    --target.api-key 'qdrant-key' \
    --target.collection 'target-collection' \
    --migration.batch-size 64
```

With Docker:

```bash
docker run --net=host --rm -it registry.cloud.qdrant.io/library/qdrant-migration qdrant \
    --source.url 'http://localhost:6334' \
    ...
```

NOTE: If the target collection already exists, its vector size and dimensions must match the source. Other settings like replication, shards can differ.

#### Source Qdrant Options

| Flag                  | Description                                                |
| --------------------- | ---------------------------------------------------------- |
| `--source.collection` | Source collection name                                     |
| `--source.url`        | Source gRPC URL. Default: `"http://localhost:6334"`        |
| `--source.api-key`    | API key for source instance                                |

#### Target Qdrant Options

| Flag                  | Description                                                |
| --------------------- | ---------------------------------------------------------- |
| `--target.collection` | Target collection name                                     |
| `--target.url`        | Target gRPC URL. Default: `"http://localhost:6334"`        |
| `--target.api-key`    | API key for target instance                                |

See [Shared Migration Options](#shared-migration-options) for shared parameters.

</details>

### Shared Migration Options

These options apply to all migrations, regardless of the source.

| Flag                                 | Description                                                          |
| ------------------------------------ | -------------------------------------------------------------------- |
| `--migration.batch-size`             | Migration batch size. Default: 50                                    |
| `--migration.restart`                | Restart migration without resuming from offset. Default: false       |
| `--migration.create-collection`      | Create the collection if it doesn't exist. Default: true             |
| `--migration.ensure-payload-indexes` | Ensure payload indexes exist. Default: true                          |
| `--migration.offsets-collection`     | Collection to store migration offset. Default: `"_migration_offsets"` |
