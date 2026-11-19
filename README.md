[![Docker](https://img.shields.io/docker/v/linkedfactory/linkedfactory-pod?label=Docker&style=flat)](https://hub.docker.com/r/linkedfactory/linkedfactory-pod)

# LinkedFactory Pod

LinkedFactory Pod is the reference implementation of the **[LinkedFactory architecture](https://github.com/linkedfactory/specification)**.

It connects descriptions of industrial assets with their time-series measurements.
The structure of a system, its components and their relationships, is described in an RDF knowledge graph.
Measurements are stored under the same component IRIs, combined with a property IRI and a timestamp.
Asset descriptions and measurements can then be queried together, for example with SPARQL.

<img style="background-color: white; padding: 15px; width: 100%; max-width: 800px" alt="LinkedFactory semantic linking" src="docs/modules/ROOT/images/lf-linking.svg">

Pod is built on the semantic platform [eniLINK](https://github.com/enilink/enilink), which provides model management, the SPARQL endpoint, and user management.

## Quickstart

Start Pod with Docker:

```sh
docker run -p 127.0.0.1:8080:8080 -v linkedfactory-pod-workspace:/linkedfactory-pod/workspace linkedfactory/linkedfactory-pod
```

Pod is then available at <http://localhost:8080/linkedfactory/>.

Write a measurement:

```sh
curl -H "Content-Type: application/json" http://localhost:8080/linkedfactory/values -d '{
  "https://example.org/Press/Frame/StrainGauge": {
    "p:force": [{ "value": 7.10096884, "time": 1541521440000 }]
  }
}'
```

Read it back:

```sh
curl -G -d "item=https://example.org/Press/Frame/StrainGauge" -d "property=p:force" http://localhost:8080/linkedfactory/values
```

> [!IMPORTANT]
> The image is configured for local development: it contains default credentials and keeps asset descriptions in memory.
> Read [Secure a deployment](docs/modules/how-to/pages/secure-deployment.adoc) before running Pod anywhere else.

## Documentation

The documentation is in [`docs/`](docs/modules/ROOT/pages/index.adoc).

| To … | Read |
|---|---|
| get a first overview | [Overview](docs/modules/ROOT/pages/index.adoc), [Quickstart](docs/modules/ROOT/pages/quickstart.adoc) |
| understand the concept and its limits | [KVIN data model](docs/modules/explanation/pages/kvin-data-model.adoc), [Architecture and limits](docs/modules/explanation/pages/architecture.adoc) |
| write and read measurements | [Ingest measurements](docs/modules/how-to/pages/ingest-measurements.adoc), [Query measurements and asset descriptions](docs/modules/how-to/pages/query-data.adoc), [Time-series API](docs/modules/reference/pages/time-series-api.adoc) |
| create models (named graphs) and describe assets | [Create and manage models](docs/modules/how-to/pages/manage-models.adoc), [Describe assets in RDF](docs/modules/how-to/pages/model-assets.adoc) |
| control who may read or change data | [Grant access to models with groups](docs/modules/how-to/pages/grant-access-with-groups.adoc), [Users, groups, and permissions](docs/modules/explanation/pages/security-model.adoc) |
| search literal values by keyword | [Enable full-text search](docs/modules/how-to/pages/enable-full-text-search.adoc) |
| run Pod for others | [Secure a deployment](docs/modules/how-to/pages/secure-deployment.adoc), [Configuration](docs/modules/reference/pages/configuration.adoc) |
| integrate a client | [HTTP interfaces](docs/modules/reference/pages/http-interfaces.adoc) |

## Contributing

To build Pod from source, run tests, or change the documentation, see [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Pod is licensed under the [Apache License 2.0](LICENSE).
