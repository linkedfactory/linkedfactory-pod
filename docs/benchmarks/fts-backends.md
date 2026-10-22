# FTS backend load and delete benchmark

`FtsBackendLifecycleBenchmark` compares the real embedded Lucene and Elasticsearch-over-HTTP `FtsSearchService` implementations on the `LucenePoC` branch. The Lance implementation lives on a separate branch and is not included here.

Each JMH invocation loads or deletes exactly 10,000, 100,000, or 1,000,000 deterministic RDF literal statements. Each statement has a distinct subject, one of 16 predicates, a literal value, and one shared context. The same prebuilt statements are used for both backends. The benchmark submits them in commits of 5,000 statements to keep Elasticsearch bulk requests below its default HTTP payload limit. One JMH operation is the complete selected batch, so the reported unit is **milliseconds per batch**. Divide the batch size by `score / 1000` for approximate triples per second.

`load` starts with a new empty index. `delete` starts with the selected batch already indexed; that setup is outside the timed method. Both operations time the calls to `begin`, `addRemoveStatements`, and `commit`, including Lucene disk writes or Elasticsearch HTTP bulk processing. Data generation, index creation, delete preloading, count checks, refreshes, and cleanup are outside the timed method. A new index is used for every invocation. The benchmark checks that load leaves the expected document count and delete leaves zero documents. It deletes only the temporary Lucene directory and the uniquely named Elasticsearch index it created.

These timings isolate the FTS backend service. They do not include Pod model upload/download, RDF parsing, `FtsSail`, network transport to the Pod, or Elasticsearch refresh visibility. Elasticsearch must be running separately. Run both backends on the same host and report the Elasticsearch image/version, JVM, CPU, storage, and settings with results.

## Run

Use Java 21 and Maven. The comparison was checked with Elasticsearch 8.17.0. For a local container with the same settings, start it in PowerShell:

```powershell
docker run --name linkedfactory-fts-es -p 9200:9200 `
  -e 'discovery.type=single-node' `
  -e 'xpack.security.enabled=false' `
  -e 'ES_JAVA_OPTS=-Xms1g -Xmx1g' `
  docker.elastic.co/elasticsearch/elasticsearch:8.17.0
```

If that container already exists but is stopped, use `docker start linkedfactory-fts-es`. Confirm that `http://localhost:9200` responds before running JMH. Use a dedicated test Elasticsearch instance because the benchmark creates and deletes indexes. Set `FTS_BENCHMARK_ENDPOINT` if the instance is not at `http://localhost:9200`.

From the repository root, compile the reactor once:

```sh
mvn -pl bundles/io.github.linkedfactory.core -am clean install -DskipTests
```

The benchmark uses unique index names starting with `linkedfactory-fts-jmh-`.

Run the full matrix, writing JMH JSON under the ignored `target` directory:

```sh
mvn -pl bundles/io.github.linkedfactory.core -Pjmh test-compile exec:exec
```

The default run uses one JVM fork, one warmup invocation, three measured invocations, and one thread per case. Twelve cases cover two backends, three sizes, and two operations. The 1,000,000-statement cases can take several minutes and need enough free heap and Elasticsearch disk space. For a stronger comparison, run the full matrix more than once and retain each JSON result file separately.

To run a small smoke check first:

```sh
mvn -pl bundles/io.github.linkedfactory.core -Pjmh \
  -Djmh.backends=lucene,elastic -Djmh.batch.sizes=10000 \
  -Djmh.warmups=0 -Djmh.measurements=1 \
  -Djmh.result.file=target/fts-backend-smoke.json \
  test-compile exec:exec
```

The corresponding full results are written to `bundles/io.github.linkedfactory.core/target/fts-backend-jmh.json`. Compare `load` with `load` and `delete` with `delete` at the same `batchSize`. The JMH score includes every 5,000-statement commit needed to complete that batch.

In PowerShell, use `mvn.cmd` and quote each Maven `-D` property used for the smoke command, for example `'-Djmh.batch.sizes=10000'`.

Check that the JSON file contains all 12 cases before using full-run results. JMH can still write a partial result file when an individual fork fails. For a smoke run restricted to one size, expect four cases. In PowerShell:

```powershell
$results = Get-Content 'bundles/io.github.linkedfactory.core/target/fts-backend-jmh.json' -Raw | ConvertFrom-Json
if ($results.Count -ne 12) { throw "Expected 12 JMH cases, found $($results.Count)" }
```
