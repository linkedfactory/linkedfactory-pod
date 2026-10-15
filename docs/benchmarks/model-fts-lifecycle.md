# Large model and FTS lifecycle integration test

`ModelsFtsLuceneLifecycleIntegrationTest` exercises the deployed `/models` API against a running pod and Elasticsearch. It generates a Turtle model on disk, uploads it, checks the generated predicate's triple count and corresponding Elasticsearch FTS document count, deletes the model through `DELETE /models?model=...`, and checks that the model and all of its FTS documents are gone. The SPARQL count is restricted to the generated predicate because repository inference or model setup can expose additional statements in the graph.

The test is opt-in. It requires an isolated pod and an Elasticsearch index configured as the pod's FTS target. The test uses a unique model URI and does not create or delete the Elasticsearch index.

Configure the base URLs and run it from the repository root:

```sh
POD_INTEGRATION_BASE_URL=http://localhost:8080 \
FTS_INTEGRATION_ENDPOINT=http://localhost:9200 \
mvn -pl bundles/io.github.linkedfactory.core \
  -Dtest=ModelsFtsLifecycleIntegrationTest test
```

The defaults target `/models` on the pod and the `linkedfactory` index in Elasticsearch. Override them when needed:

```sh
mvn -pl bundles/io.github.linkedfactory.core \
  -Dtest=ModelsFtsLifecycleIntegrationTest \
  -Dmodels.integration.pod=http://localhost:8080 \
  -Dmodels.integration.elasticsearch=http://localhost:9200 \
  -Dmodels.integration.index=linkedfactory \
  -Dmodels.integration.triples=1000000 \
  -Dmodels.integration.timeout.seconds=1800 test
```

For authenticated deployments, set `POD_INTEGRATION_TOKEN` or `-Dmodels.integration.token=...`. The default workload is 100,000 triples; increase it gradually to 1,000,000 for a scale run. The test prints upload and delete durations. The generated Turtle is written to a temporary file and streamed to the pod, so the complete RDF payload is not kept in the test JVM's heap; monitor the pod's memory separately during large runs. Ensure that the pod and Elasticsearch index are dedicated to testing before running large workloads.

## Chunked uploads

By default, `models.integration.chunkTriples=0` uploads the entire model in one request.
Set a positive value to append sequential chunks to the same model using POST:

```sh
mvn -pl bundles/io.github.linkedfactory.core \
  -Dtest=ModelsFtsLifecycleIntegrationTest \
  -Dmodels.integration.pod=http://localhost:8080 \
  -Dmodels.integration.elasticsearch=http://localhost:9200 \
  -Dmodels.integration.triples=10000000 \
  -Dmodels.integration.chunkTriples=100000 \
  -Dmodels.integration.timeout.seconds=1800 test
```

This sends 100 requests of 100,000 triples. Non-divisible totals produce a smaller final
chunk. Each chunk uses distinct subjects and one temporary file, deleted after its request.
The timeout applies to each request, not the entire run. Each upload commits separately,
so this mode is not atomic; cleanup still attempts to delete the model if a chunk fails.

Set `-Dmodels.integration.verifyEachChunk=true` to check cumulative RDF and FTS counts
after each chunk (default false). Final counts and deletion cleanup are always checked.
The reported upload duration sums HTTP upload times, excluding data generation and
count verification. Elasticsearch bulk batching remains independent of these RDF chunks.
