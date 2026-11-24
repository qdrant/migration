## Development

### Running tests

The migration tool has two kind of tests, Golang unit tests and integration tests written with [bats](https://bats-core.readthedocs.io/).

To run the Golang tests, execute:

```bash
make test_unit
```

To run the integration tests, execute:

```bash
make test_integration
```

To run all tests, execute:

```bash
make test
```

### Linting

This project uses [golangci-lint](https://golangci-lint.run/) to lint the code. To run the linter, execute:

```bash
make lint
```

Code formatting is ensured with [gofmt](https://pkg.go.dev/cmd/gofmt). To format the code, execute:

```bash
make fmt
```

### Pre-commit hooks

This project uses [pre-commit](https://pre-commit.com/) to run the linter and the formatter before every commit. To install the pre-commit hooks, execute:

```bash
pre-commit install-hooks
```

## Releasing a new version

To release a new version, create and push a tag named `vX.Y.Z` (for example `v1.2.3`, no leading zeros) on the commit to release:

```bash
git checkout main && git pull
git tag vX.Y.Z
git push origin vX.Y.Z
```

Releases are normally tagged on `main`. For a hot fix, create a branch from the previous release tag, `git cherry-pick -x` the fix from `main`, and tag the head of that branch.

Pushing the tag starts the release GitHub Action. It checks the tag name, builds and signs the Docker image and pushes it to the registry, publishes the release notes, and builds the binaries. Only people allowed to create `v*` tags can release.
