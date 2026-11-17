# Contributing to LinkedFactory Pod

Thank you for your interest in improving Pod. This file covers the essentials; details are in the development section of the documentation ([`docs/modules/development`](docs/modules/development/pages/index.adoc)).

## Prerequisites

- JDK 21
- Maven 3
- Docker (optional, for container builds)
- Node.js 22 (optional, for building the documentation)

## Build and run

```sh
mvn package                          # full build
cd launch/equinox
mvn test -Pconfigure -DskipTests     # create or update the launch configuration
mvn test                             # start Pod at http://localhost:8080/linkedfactory/
```

More: [Build and run locally](docs/modules/development/pages/build-and-run.adoc), [Repository structure](docs/modules/development/pages/repository-structure.adoc).

## Tests

```sh
mvn verify
```

CI runs the same command for every pull request (`.github/workflows/verify.yml`).

## Pull requests

1. Create a branch from `main`.
2. Keep changes focused; add or update tests.
3. Update the documentation in `docs/` in the same pull request when behaviour, APIs, or configuration change.
4. Make sure `mvn verify` and the documentation build pass.

## Documentation

The documentation is an Antora component in `docs/`. Build it locally with

```sh
npx antora@3.1 --fetch antora-playbook.yml
```

Structure and conventions: [Writing documentation](docs/modules/development/pages/documentation.adoc).
