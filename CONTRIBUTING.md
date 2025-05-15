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

To release a new version create and push a release branch that follows the pattern `release/vX.Y.Z`. The release branch should be created from the `main` branch, or from the latest release branch in case of a hot fix.

A GitHub Action will then create a new release, build the binaries, push the Docker image to the registry, and create a Git tag.
