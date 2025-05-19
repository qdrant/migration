# ⛟ Migration → Qdrant

CLI tool for migrating data to [Qdrant](http://qdrant.tech) with support for resumable transfers in case of interruptions.

Easily move your data to Qdrant from other vector storages. With support for resumable migration, even interrupted processes can continue smoothly.

Supported sources:

* Milvus
* Another Qdrant instance

## Installation

The easiest way to run the qdrant-migration tool is as a container. You can run it any machine where you have connectivity to both the source and the target Qdrant databases. For optimal performance, you should run the tool on a machine with a fast network connection and minimum latency to both databases.

To pull the latest image run:

#### Binaries

Each release includes **precompiled binaries** for all major OS and CPU architectures. Download the latest one from the [Releases Page](https://github.com/qdrant/migration/releases).

#### Docker

To get the latest Docker image run the following command.

```bash
$ docker run --net=host --rm -it registry.cloud.qdrant.io/library/qdrant-migration qdrant --help
Usage: migration qdrant --source-url=STRING --source-collection=STRING --target-url=STRING --target-collection=STRING [flags]

Migrate data from a Qdrant database to Qdrant.

Flags:
  -h, --help                                                      Show context-sensitive help.
      --debug                                                     Enable debug mode.
      --trace                                                     Enable trace mode.
      --skip-tls-verification                                     Skip TLS verification.
      --version                                                   Print version information and quit

      --source-url=STRING                                         Source gRPC URL, e.g. https://your-qdrant-hostname:6334
      --source-collection=STRING                                  Source collection
      --source-api-key=STRING                                     Source API key ($SOURCE_API_KEY)
      --target-url=STRING                                         Target gRPC URL, e.g. https://your-qdrant-hostname:6334
      --target-collection=STRING                                  Target collection
      --target-api-key=STRING                                     Target API key ($TARGET_API_KEY)
  -b, --batch-size=50                                             Batch size
  -c, --create-target-collection                                  Create the target collection if it does not exist
      --ensure-payload-indexes                                    Ensure payload indexes are created
      --migration-offsets-collection-name="_migration_offsets"    Collection where the current migration offset should be stored
      --restart-migration                                         Restart the migration and do not continue from last offset
```

## How To Migrate?

> Click each to expand

<details>

<summary><h3>From Milvus</h3></summary>

Migrate data from a **Milvus** database to **Qdrant**:

### 📥 Example

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
    ...
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

See [Shared Migration Options](#shared-migration-options) for shared parameters.

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
| `--source.url`        | Source gRPC URL (e.g. `https://your-qdrant-hostname:6334`) |
| `--source.collection` | Source collection name                                     |
| `--source.api-key`    | API key for source instance                                |

#### Target Qdrant Options

| Flag                  | Description                                                |
| --------------------- | ---------------------------------------------------------- |
| `--target.url`        | Target gRPC URL (e.g. `https://your-qdrant-hostname:6334`) |
| `--target.collection` | Target collection name                                     |
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
| `--migration.offsets-collection`     | Collection to store migration offset. Default: "_migration_offsets" |
