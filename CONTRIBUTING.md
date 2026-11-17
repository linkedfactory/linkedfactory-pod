# Contributing to LinkedFactory Pod

LinkedFactory Pod is the reference implementation of the [LinkedFactory architecture](https://github.com/linkedfactory/specification).
This guide describes how to set up a development environment, build and test Pod, and submit changes.

> **Open points** are marked with `TODO`. They still have to be agreed on by the maintainers.

## Prerequisites

- JDK 21 (CI uses Eclipse Temurin)
- Maven 3
- Docker (optional, for building and running the container image)

## Build

Pod is a plain Maven project. A full build runs from the repository root:

```sh
mvn package
```

## Run locally

```sh
cd launch/equinox
mvn test -Pconfigure -DskipTests   # create or update the launch configuration
mvn test                           # (re-)start the Pod instance
```

Pod is then available at <http://localhost:8080/linkedfactory/>.

## Set up an IDE

Any IDE that supports Java and Scala projects works.

- **IntelliJ IDEA:** `File > Project from existing sources...`
- **Eclipse:** `File > Import > Maven > Existing Maven Projects`

## Tests

```sh
mvn verify
```

The workflow [`.github/workflows/verify.yml`](.github/workflows/verify.yml) runs `mvn verify` for every push and pull request to `main`.
Make sure it passes locally before you open a pull request.

Benchmarks for KVIN ingestion are described in [docs/modules/development/pages/benchmarks.adoc](docs/modules/development/pages/benchmarks.adoc).

- `TODO` Define which changes require new or updated tests.

## Submitting changes

1. Create a branch from `main`.
2. Keep each pull request focused on one topic.
3. Update the documentation in [`docs/`](docs/) in the same pull request when behaviour, APIs, or configuration change.
4. Open a pull request against `main`.

- `TODO` Where to report bugs and discuss larger changes before starting (GitHub issues, discussions, other channel).
- `TODO` Branch naming and commit message conventions.
- `TODO` Review process: who reviews, how many approvals are needed, who merges.

## Code style

- `TODO` Formatting rules for Java and Scala (formatter configuration, line length, imports).

## License

Pod is licensed under the [Apache License 2.0](LICENSE). Contributions are accepted under the same license.

Source files carry the header from [`releng/license-header.txt`](releng/license-header.txt). To add or update the headers of Java and Scala sources, run:

```sh
mvn process-sources -Pupdate-licenses
```

- `TODO` Whether contributors have to sign a CLA or a DCO sign-off (`git commit -s`).

## Documentation

The documentation is an [Antora](https://antora.org) component written in AsciiDoc in [`docs/`](docs/), organised by Diátaxis type (how-to, reference, explanation) plus a development section.

- `TODO` How to build and preview the documentation locally and where it is published.

## Releases

Releases are made by the maintainers. [`releng/update-version.sh`](releng/update-version.sh) updates the project version, [`.github/workflows/deploy.yml`](.github/workflows/deploy.yml) publishes the Maven artifacts to GitHub Packages, and [`.github/workflows/docker.yml`](.github/workflows/docker.yml) publishes the Docker image to Docker Hub.

- `TODO` Describe the release steps and versioning scheme.

## Security issues

- `TODO` How to report vulnerabilities privately instead of opening a public issue.
