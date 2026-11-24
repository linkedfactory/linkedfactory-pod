# Contributing to LinkedFactory Pod

LinkedFactory Pod is the reference implementation of the [LinkedFactory architecture](https://github.com/linkedfactory/specification).
This file describes how to contribute changes. Documentation for using Pod starts in the [README](README.md).

## Get started

| Topic | Page |
|---|---|
| Prerequisites, build, local instance, Docker image, IDE setup | [Build and run Pod from source](docs/modules/development/pages/build-and-run.adoc) |
| Bundles, directories, Maven profiles, CI workflows | [Repository structure](docs/modules/development/pages/repository-structure.adoc) |
| Ingestion benchmarks | [Benchmarks](docs/modules/development/pages/benchmarks.adoc) |
| Writing and building documentation | [Documentation guidelines](docs/modules/development/pages/documentation.adoc) |
| Which eniLINK revision the documentation is based on | [eniLINK source baseline](docs/modules/development/pages/upstream-baseline.adoc) |

In short: JDK 21 and Maven 3 are required, `mvn verify` builds and tests everything.

## Report bugs and propose changes

Use [GitHub issues](https://github.com/linkedfactory/linkedfactory-pod/issues) for bug reports and feature requests. Security vulnerabilities are reported privately, see [SECURITY.md](SECURITY.md).
Open an issue before starting a larger change, so the approach can be discussed first.

## Make a change

1. Create a branch from `main`. Include the issue number in the branch name, for example `gh-123-support-csv-separator`.
2. Start each commit message with the issue number, for example `GH-123 Support custom CSV separators`.
3. Keep each pull request focused on one topic.
4. Add or update tests in `src/test` of the affected bundle. Every code change is expected to come with tests.
5. Update the documentation in the same pull request if behaviour, interfaces, or configuration change. Follow the [documentation guidelines](docs/modules/development/pages/documentation.adoc).
6. Run `mvn verify` locally. CI runs the same command for every pull request to `main`.
7. Open a pull request against `main` and complete the checklist in the pull request template.

The naming and commit conventions follow those of [RDF4J](https://github.com/eclipse-rdf4j/rdf4j). They are good practice, not enforced rules.

## Review and merge

The project currently has one maintainer, who reviews and merges pull requests.
Pull requests are usually merged with rebase, and squashed where that gives a cleaner history.

## Code style

There is no formatter configuration yet. Follow the style of the surrounding code.

## License

Pod is licensed under the [Apache License 2.0](LICENSE). Contributions are accepted under the same license.

Java and Scala sources carry the header from [`releng/license-header.txt`](releng/license-header.txt). To add or update headers:

```sh
mvn process-sources -Pupdate-licenses
```

No contributor license agreement (CLA) or DCO sign-off is required.

## Documentation

The documentation in `docs/` is built with Antora and published on GitHub Pages whenever `main` changes. Pull requests that touch the documentation are built as a check. Details are in the [documentation guidelines](docs/modules/development/pages/documentation.adoc).

## Releases

Releases are made by the maintainer, either manually or through the `snapshots` branch.

- Pushing to `snapshots` publishes snapshot Maven artifacts to GitHub Packages and a Docker image to Docker Hub.
- For a release, [`releng/update-version.sh`](releng/update-version.sh) sets the release version, commits it, and creates the tag `v<version>`; it can also set the next development version:

  ```sh
  releng/update-version.sh -r 1.3.0 -d 1.4.0-SNAPSHOT
  ```

  The script does not push. Pushing the tag publishes the Maven artifacts and the Docker image.

Versions have the form `major.minor.patch`, with pre-releases such as `1.3.0-beta.1`.

## Security issues

Do not report vulnerabilities in public issues. Report them privately as described in [SECURITY.md](SECURITY.md).

## Working with coding agents

[AGENTS.md](AGENTS.md) contains the instructions for AI coding agents working in this repository, including the documentation rules.
