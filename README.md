# qdrant-migration

**Note: This project is in beta. The API may change in future releases.**

This tool helps to migrate data to Qdrant from other sources. It will stream all vectors from a collection in the source Qdrant instance to the target Qdrant instance.

The target collection can have a different replication or sharding configuration, expect the vector size and distance need to be the same.

Supported sources:

* Milvus
* Another Qdrant instances

## Installation

The easiest way to run the qdrant-migration tool is as a container. You can run it any machine where you have connectivity to both the source and the target Qdrant databases. For optimal performance, you should run the tool on a machine with a fast network connection and minimum latency to both databases.

To pull the latest image run:

```bash
$ docker pull registry.cloud.qdrant.io/library/qdrant-migration
```

In addtion, every release providies precompiled binaries for all major OS and CPU architectures. You can download the latest release from the [releases page](https://github.com/qdrant/migration/releases).

## Usage

To migrate from one Qdrant instance to another, you can provide the following parameters:

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

Example:

```bash
$ docker run --net=host --rm -it registry.cloud.qdrant.io/library/qdrant-migration qdrant \
    --source-url 'https://source-qdrant-hostname:6334' \
    --source-collection 'source-collection' \
    --target-url 'https://target-qdrant-hostname:6334' \
    --target-collection 'target-collection'
```

You can provide the API keys either as command line arguments or as environment variables:

```bash
$ docker run --net=host --rm -it \
    -e SOURCE_API_KEY='xyz' \ 
    registry.cloud.qdrant.io/library/qdrant-migration qdrant \
    --source-url 'https://source-qdrant-hostname:6334' \
    --source-collection 'source-collection' \
    --target-url 'https://target-qdrant-hostname:6334' \
    --target-collection 'target-collection' \
    --target-api-key 'abc'
```

The migration tool keeps track of a running migration. If you cancel a migration, it will be automatically resumed if you start it next. To restart a migration, run:

```bash
$ docker run --net=host --rm -it registry.cloud.qdrant.io/library/qdrant-migration qdrant \
    --source-url 'https://source-qdrant-hostname:6334' \
    --source-collection 'source-collection' \
    --target-url 'https://target-qdrant-hostname:6334' \
    --target-collection 'target-collection' \
    --restart-migration  
```

### Migration considerations

The migration tool will stream all vectors from the source collection to the target collection. If the target collection exists before starting the migration, its configuration regarding vector size and dimensions must match. The replication factor, shard configuration or on_disk settings can be different. If the target collection does not exist, you can create it by passing the `--create-target-collection` flag. When a collection is created, the payload indexes from the source are created in the target by default.

Existing vectors  in the target collection with the same ids as in the source collection will be overwritten. If you want to keep the existing vectors, you should create a new collection and migrate the vectors there.

The batch size can be adjusted with the `--batch-size` flag. The default batch size is 50, which is a good starting point for most use cases. If you experience performance issues, you can try to increase the batch size. Ideally a batch should be around 32MiB in size including vectors and payloads.

The Qdrant version of the source and target databases should be the same minor version. Differences in the patch version are fine.

## Development

### Running tests

The migration tool has two kind of tests, Golang unit tests and integration tests written with [bats](https://bats-core.readthedocs.io/).

To run the Golang tests, execute:

```bash
$ make test_unit
```

To run the integration tests, execute:

```bash
$ make test_integration
```

To run all tests, execute:

```bash
$ make test
```

### Linting

This project uses [golangci-lint](https://golangci-lint.run/) to lint the code. To run the linter, execute:

```bash
$ make lint
```

Code formatting is ensured with [gofmt](https://pkg.go.dev/cmd/gofmt). To format the code, execute:

```bash
$ make fmt
```

### Pre-commit hooks

This project uses [pre-commit](https://pre-commit.com/) to run the linter and the formatter before every commit. To install the pre-commit hooks, execute:

```bash
$ pre-commit install-hooks
```

## Releasing a new version

To release a new version create and push a release branch that follows the pattern `release/vX.Y.Z`. The release branch should be created from the `main` branch, or from the latest release branch in case of a hot fix.

A GitHub Action will then create a new release, build the binaries, push the Docker image to the registry, and create a Git tag.
