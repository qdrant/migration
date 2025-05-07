# ⛟ Migration → Qdrant

CLI tool for migrating data to [Qdrant](http://qdrant.tech) with support for resumable transfers in case of interruptions.

> [!WARNING]  
> This project is in beta. The API may change in future releases.

## Supported Sources

* [Milvus](https://milvus.io)
* Another [Qdrant](http://qdrant.tech) instance

## Installation

You can run this tool on any machine with connectivity to both the **source** and the Qdrant database. For best performance, use a machine with a fast network and minimal latency to both endpoints.

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

<summary><h3>From Milvus</h3></summary>

Migrate data from a **Milvus** database to **Qdrant**:

### 📥 Command Example

```bash
migration milvus \
    --milvus.url 'https://example.gcp-us-west1.cloud.zilliz.com' \
    --milvus.enable-tls-auth \
    --milvus.collection 'example-collection' \
    --milvus.db-name 'optional-db-name'
    --milvus.server-version 'optional-server-version'
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
    --milvus.enable-tls-auth \
    --milvus.collection 'example-collection' \
    --milvus.db-name 'optional-db-name'
    --milvus.server-version 'optional-server-version'
    --milvus.api-key 'optional-milvus-api-key' \
    --qdrant.url 'https://example.cloud-region.cloud-provider.cloud.qdrant.io:6334' \
    --qdrant.api-key 'optional-qdrant-api-key' \
    --qdrant.collection 'target-collection' \
    --migration.batch-size 64
```

#### Milvus Options

| Flag                       | Description                                             |
| -------------------------- | ------------------------------------------------------- |
| `--milvus.url`             | Source Milvus URL (e.g. `https://your-milvus-hostname`) |
| `--milvus.collection`      | Source collection name                                  |
| `--milvus.api-key`         | Source API key (`$SOURCE_API_KEY`)                      |
| `--milvus.enable-tls-auth` | Enable TLS Auth                                         |
| `--milvus.username`        | Username for Milvus                                     |
| `--milvus.password`        | Password for Milvus                                     |
| `--milvus.db-name`         | Milvus database name                                    |
| `--milvus.server-version`  | Server version                                          |

#### Qdrant Options

| Flag                  | Description                                                |
| --------------------- | ---------------------------------------------------------- |
| `--qdrant.url`        | Qdrant gRPC URL (e.g. `https://your-qdrant-hostname:6334`) |
| `--qdrant.collection` | Target collection name                                     |
| `--qdrant.api-key`    | Qdrant API key                                             |

#### Migration Options

| Flag                                                  | Description                                    |
| ----------------------------------------------------- | ---------------------------------------------- |
| `-b`, `--migration.batch-size=50`                     | Set batch size                                 |
| `--migration.restart`                                 | Restart migration without resuming from offset |
| `-c`, `--migration.create-collection`                 | Create the collection if it doesn't exist      |
| `--migration.ensure-payload-indexes`                  | Ensure payload indexes exist                   |
| `--migration.offsets-collection="_migration_offsets"` | Collection to store migration offset           |

</details>
<details>
<summary><h3>From Another Qdrant Instance</h3></summary>

Migrate data from one **Qdrant** instance to another.

### 📥 Command Example

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
| `--source.url`        | Source gRPC URL (e.g. `https://your-qdrant-hostname:6334`) |
| `--source.collection` | Source collection name                                     |
| `--source.api-key`    | API key for source instance                                |

#### Target Qdrant Options

| Flag                  | Description                                                |
| --------------------- | ---------------------------------------------------------- |
| `--target.url`        | Target gRPC URL (e.g. `https://your-qdrant-hostname:6334`) |
| `--target.collection` | Target collection name                                     |
| `--target.api-key`    | API key for target instance                                |

#### Migration Options

| Flag                                                  | Description                                    |
| ----------------------------------------------------- | ---------------------------------------------- |
| `-b`, `--migration.batch-size=50`                     | Set batch size                                 |
| `--migration.restart`                                 | Restart migration without resuming from offset |
| `-c`, `--migration.create-collection`                 | Create the collection if it doesn't exist      |
| `--migration.ensure-payload-indexes`                  | Ensure payload indexes exist                   |
| `--migration.offsets-collection="_migration_offsets"` | Collection to store migration offset           |

</details>
