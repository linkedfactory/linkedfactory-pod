# FTS backend load and delete benchmark

`FtsBackendLifecycleBenchmark` compares the real embedded Lance and Elasticsearch-over-HTTP `FtsSearchService` implementations on the `Lance_PoC` branch. The Lucene implementation lives on a separate branch (`LucenePoC`) with its own copy of this benchmark and is not included here.

Each JMH invocation loads or deletes exactly 10,000, 100,000, or 1,000,000 deterministic RDF literal statements. Each statement has a distinct subject, one of 16 predicates, a literal value, and one shared context. The same prebuilt statements are used for both backends. The benchmark submits them in commits of 5,000 statements to keep Elasticsearch bulk requests below its default HTTP payload limit. One JMH operation is the complete selected batch, so the reported unit is **milliseconds per batch**. Divide the batch size by `score / 1000` for approximate triples per second.

`load` starts with a new empty dataset/index. `delete` starts with the selected batch already indexed; that setup is outside the timed method. Both operations time the calls to `begin`, `addRemoveStatements`, and `commit`, including Lance Arrow/native writes or Elasticsearch HTTP bulk processing. Data generation, dataset/index creation, delete preloading, count checks, and cleanup are outside the timed method. A fresh temporary Lance dataset or uniquely named Elasticsearch index is used for every invocation. The benchmark checks that load leaves the expected row/document count and delete leaves zero, using an independent `Dataset.open(...).countRows()` handle for Lance (mirroring how the Lucene benchmark opens its own `DirectoryReader`) and the Elasticsearch `_count` API for elastic. It deletes only the temporary Lance dataset directory and the uniquely named Elasticsearch index it created.

These timings isolate the FTS backend service. They do not include Pod model upload/download, RDF parsing, `FtsSail`, network transport to the Pod, or Elasticsearch refresh visibility. Elasticsearch must be running separately. Run both backends on the same host and report the Elasticsearch image/version, JVM, CPU, storage, and settings with results.

## Lance delete at scale is slow — read this before running the full matrix

`LanceFtsIndex.deleteRows()` already batches deletes into groups of 5,000 statements per `dataset.delete()` call (a fix for a native crash seen with much larger single OR-predicate chains). Each of those calls still performs a full, unindexed scan over the dataset to evaluate the predicate, because this schema has no scalar index on `subject`/`predicate`/`value`. Delete cost therefore grows with both the batch size and the number of 5,000-row sub-batches needed to cover it, unlike Lucene's near-constant per-document delete.

Concretely, expect the `(lance, 1000000, delete)` case alone to take on the order of 20-30 minutes **per invocation**, so roughly 1.5-2 hours for its one warmup plus three measured invocations under the default profile. The benchmark class sets an explicit `@Timeout(30, MINUTES)` on `FtsBackendLifecycleBenchmark` so JMH does not kill that iteration before it finishes (JMH's own default is 10 minutes). The full 12-case matrix took just over two hours end to end on the reference machine used to validate this benchmark. Everything other than lance/delete/1000000 completes in seconds.

## Lance requires Linux

Lance ships native JNI libraries for `linux-x86-64`, `linux-aarch64`, and `darwin-aarch64` only — there is no Windows build. Run this benchmark inside WSL2 (Ubuntu) or a native Linux/macOS host; it cannot run on Windows directly. The `jmh` profile's `FtsBackendLifecycleBenchmarkRunner` already appends `--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED` to the forked JMH JVM via `OptionsBuilder.jvmArgsAppend(...)`, matching the flag this module's surefire configuration uses for its own Arrow-backed tests — no manual JVM flags are needed to run it.

## Run

Use Java 21 and Maven. The comparison was checked with Elasticsearch 8.17.0. For a local container with the same settings, start it in a shell inside WSL2/Linux:

```sh
docker run --name linkedfactory-fts-es -p 9200:9200 \
  -e 'discovery.type=single-node' \
  -e 'xpack.security.enabled=false' \
  -e 'ES_JAVA_OPTS=-Xms1g -Xmx1g' \
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

The default run uses one JVM fork, one warmup invocation, three measured invocations, and one thread per case. Twelve cases cover two backends, three sizes, and two operations — see the warning above about how long the 1,000,000-row lance delete cases take, and make sure there is enough free disk space for the temporary Lance datasets and Elasticsearch indexes at that scale. For a stronger comparison, run the full matrix more than once and retain each JSON result file separately.

To run a small smoke check first:

```sh
mvn -pl bundles/io.github.linkedfactory.core -Pjmh \
  -Djmh.backends=lance,elastic -Djmh.batch.sizes=10000 \
  -Djmh.warmups=0 -Djmh.measurements=1 \
  -Djmh.result.file=target/fts-backend-smoke.json \
  test-compile exec:exec
```

The corresponding full results are written to `bundles/io.github.linkedfactory.core/target/fts-backend-jmh.json`. Compare `load` with `load` and `delete` with `delete` at the same `batchSize`. The JMH score includes every 5,000-statement commit needed to complete that batch.

Check that the JSON file contains all 12 cases before using full-run results. JMH can still write a partial result file when an individual fork fails. For a smoke run restricted to one size, expect four cases.

The `jmh` profile runs `FtsBackendLifecycleBenchmarkRunner`, a small `main()` that builds JMH's `Options` via the Java API and reads `backend`/`batchSize`/etc. from environment variables set by the profile, exactly as the Lucene branch's version of this benchmark does. See the Lucene branch's copy of this document for the history of why that indirection exists (a real exec-maven-plugin 3.5.0 argument-list bug, unrelated to Lance).
