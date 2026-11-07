# FTS backend load, delete, and search benchmark

`FtsBackendLifecycleBenchmark` compares the real embedded Lucene and Elasticsearch-over-HTTP `FtsSearchService`/`FtsSearchBackend` implementations on the `LucenePoC` branch. The Lance implementation lives on a separate branch and is not included here.

Each JMH invocation loads, deletes, or searches exactly 10,000, 100,000, or 1,000,000 deterministic RDF literal statements, at one of two literal lengths. Each statement has a distinct subject, one of 16 predicates, and a literal value built from the same seeded, Zipfian-weighted 48-word vocabulary (so some terms are common and some rare, like real text) and one shared context. `literalLength=short` picks 2-8 words per literal (roughly 10-70 characters, the original shape of this benchmark); `literalLength=long` picks 60-100 words per literal (roughly 400-700 characters, closer to a real free-text description or note). Both use the same vocabulary and weighting, just more draws per literal, so `"fox"` (the search query term below) still appears reliably at both lengths. The same prebuilt statements are used for both backends. The benchmark submits them in commits of 5,000 statements to keep Elasticsearch bulk requests below its default HTTP payload limit.

`literalLength` is a class-level JMH `@Param`, so `load` and `delete` run at both lengths too, not only `search` — the three operations share one prebuilt `batches` field per trial, and JMH applies every `@Param` to every `@Benchmark` method in the class. This was not the primary goal (which was giving `search` a realistic long-text case) but it is a reasonable side effect: indexing cost for `load`, and the unindexed-row-removal cost for `delete`, both genuinely depend on literal size too.

`load` starts with a new empty index. `delete` and `search` start with the selected batch already indexed; that setup is outside the timed method. `load` and `delete` time the calls to `begin`, `addRemoveStatements`, and `commit`, including Lucene disk writes or Elasticsearch HTTP bulk processing; one JMH operation is the complete selected batch, so the reported unit is **milliseconds per batch**. Divide the batch size by `score / 1000` for approximate triples per second. `search` times 100 repeated keyword queries for the term `"fox"` (`limit=50`, `includeSnippet=true`) issued directly against the backend's `FtsSearchBackend.search(FtsSearchRequest)`, so the reported unit is **milliseconds per 100 queries**; divide by 100 for approximate milliseconds per query. `search` asserts every run returns at least one hit.

The Elasticsearch test index is created with an explicit mapping (`subject`/`predicate` as `keyword`, `value` as `text`, `sortValue` as `keyword`), matching the mapping `ElasticKeywordSearchBackendIntegrationTest` uses against a real cluster. This is required: `ElasticKeywordSearchBackend.search()` collapses and sorts on the bare `subject` field, which needs doc values (a `keyword` field); Elasticsearch's default dynamic mapping makes an unmapped string field `text`, where collapse/sort fail with `illegal_argument_exception: Fielddata is disabled`.

Data generation, index creation, delete/search preloading, count checks, refreshes, and cleanup are outside the timed method. A new index is used for every invocation. The benchmark checks that load/search leave the expected document count and delete leaves zero documents. It deletes only the temporary Lucene directory and the uniquely named Elasticsearch index it created.

These timings isolate the FTS backend service. They do not include Pod model upload/download, RDF parsing, `FtsSail`, network transport to the Pod, or Elasticsearch refresh visibility. Elasticsearch must be running separately. Run both backends on the same host and report the Elasticsearch image/version, JVM, CPU, storage, and settings with results.

On native Windows, the Elasticsearch-backed `load`/`delete`/`search` cases can fail with `AccessDeniedException` in `HttpFtsSearchService.writeFence` (a pre-existing Windows-only file-locking limitation unrelated to this benchmark). Run under WSL2 or Linux, which matches real CI.

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

The default run uses one JVM fork, one warmup invocation, three measured invocations, and one thread per case. Thirty-six cases cover two backends, three sizes, three operations (load, delete, search), and two literal lengths (short, long). The 1,000,000-statement long-literal cases are the slowest (Elasticsearch `load` at 1,000,000/long took over a minute per invocation in one run) and need enough free heap and Elasticsearch disk space; the full matrix takes 30-45 minutes on a typical development machine. For a stronger comparison, run the full matrix more than once and retain each JSON result file separately.

To run a small smoke check first:

```sh
mvn -pl bundles/io.github.linkedfactory.core -Pjmh \
  -Djmh.backends=lucene,elastic -Djmh.batch.sizes=10000 \
  -Djmh.warmups=0 -Djmh.measurements=1 \
  -Djmh.result.file=target/fts-backend-smoke.json \
  test-compile exec:exec
```

The corresponding full results are written to `bundles/io.github.linkedfactory.core/target/fts-backend-jmh.json`. Compare `load` with `load`, `delete` with `delete`, and `search` with `search` at the same `batchSize` and `literalLength`. The `load`/`delete` JMH score includes every 5,000-statement commit needed to complete that batch; the `search` score includes 100 queries. To restrict a run to one literal length, set `-Djmh.literal.lengths=short` or `-Djmh.literal.lengths=long`.

In PowerShell, use `mvn.cmd` and quote each Maven `-D` property used for the smoke command, for example `'-Djmh.batch.sizes=10000'`.

Check that the JSON file contains all 36 cases before using full-run results. JMH can still write a partial result file when an individual fork fails. For a smoke run restricted to one size and both literal lengths, expect twelve cases. In PowerShell:

```powershell
$results = Get-Content 'bundles/io.github.linkedfactory.core/target/fts-backend-jmh.json' -Raw | ConvertFrom-Json
if ($results.Count -ne 36) { throw "Expected 36 JMH cases, found $($results.Count)" }
```

The `jmh` profile runs `FtsBackendLifecycleBenchmarkRunner`, a small `main()` that builds JMH's `Options` via the Java API and reads `backend`/`batchSize`/`literalLength`/etc. from environment variables set by the profile. An earlier version invoked `org.openjdk.jmh.Main` directly with `-p` command-line flags; that silently dropped the `backend=...` and `batchSize=...` values on both Windows and Linux (a real bug in how exec-maven-plugin 3.5.0 builds its argument list), causing every run to default to `@Param`'s own built-in value set instead of the requested one. Verified with the full 36-case run above, including against a real Elasticsearch 8.17.0 instance.
