# JVector embedded vector-search load, remove, and search benchmark

`VectorBackendLifecycleBenchmark` measures the embedded `JVectorIndex`/`VectorSearchService`/`VectorSearchBackend` implementation on the `JvectorPoC` branch. This module is not wired into `FtsSail` or any SPARQL vocabulary; it is a standalone in-process nearest-neighbor index, benchmarked the same way `FtsBackendLifecycleBenchmark` benchmarks the FTS backends (see `fts-backends.md`), so the two are structurally comparable even though vector search and full-text search answer different questions.

Each JMH invocation loads, removes, or searches exactly 10,000, 100,000, or 1,000,000 deterministic 768-dimension vectors (matching `google/embeddinggemma-300m`'s real output dimension), drawn from a seeded Gaussian distribution and L2-normalized to unit length, compared with cosine similarity. Each vector has a distinct subject IRI. The benchmark submits them in commits of 5,000 vectors, the same chunking convention used throughout this project's vector-search benchmarking (an earlier, unchunked version of the Lucene vector benchmark on a different branch needed far more heap for no real reason -- see that branch's history -- so this one is chunked from the start).

`load` times calls to `begin`, `upsert`, and `commit` for every chunk of the selected batch; one JMH operation is the complete batch, so the reported unit is **milliseconds per batch**. Divide the batch size by `score / 1000` for approximate vectors per second. `remove` starts with the batch already loaded (outside the timed method) and times `begin`/`remove`/`commit` for every vector, one at a time, across the same 5,000-vector commit chunks. `search` starts with the batch already loaded and times 100 repeated top-10 nearest-neighbor queries against the same fixed query vector, so the reported unit is **milliseconds per 100 queries**; divide by 100 for approximate milliseconds per query. `search` asserts every run returns at least one hit; `load`/`search` assert the expected vector count; `remove` asserts zero vectors remain.

**Known limitation, found while validating this benchmark: `remove` does not scale.** `JVectorIndex.remove()` calls `GraphIndexBuilder.removeDeletedNodes()` synchronously after every single node removal -- a deliberate correctness fix from the original PoC work (an earlier version left the graph's entry point unreachable after a delete if a replacement node was added before the next compaction). That fix re-splices the whole graph's edges around the deleted node on every call, and its cost grows with the graph's current size. Removing a batch of vectors one at a time therefore costs roughly O(n) re-splices against a graph that stays close to size n for most of the removal, i.e. a real quadratic blowup, not a fixed per-operation cost. Measured directly: removing a freshly-loaded batch of 10,000 vectors took **346 seconds** (with the Panama Vector API enabled -- see below; without it, 362 seconds, confirming this cost is not vector-math-bound). At that ratio, `remove` at 100,000 or 1,000,000 would take hours to days and was not run. **Do not run the `remove` benchmark case above 10,000 without first addressing this in `JVectorIndex`** -- it is not a benchmark-harness problem, it is a real property of the current implementation, flagged here rather than silently worked around, since fixing it (most likely by deferring `removeDeletedNodes()` to a batch-level compaction step, the same tradeoff Lance's own delete path makes for the same reason) is a real design change outside the scope of this PoC branch. `load` and `search` do not have this problem; both were measured and run normally.

`JVectorIndex` keeps every vector resident in the JVM heap for the life of the index, with no disk persistence during normal operation (`save`/`loadForSearch` exist for the one-time proof that the graph round-trips to disk, not for live persistence). 1,000,000 x 768-dimension vectors alone is roughly 2.9GB of raw vector data before graph/map overhead; make sure the JVM has enough heap before running the 1,000,000 case (a plain default heap on an 8-16GB machine should be enough, but is not guaranteed on a smaller one).

JVector's own vectorized distance computation (via the JDK's incubating Vector API / Panama) needs `--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED` on the JVM that actually runs the benchmark. `mvn test`'s surefire `argLine` already carries these flags for the unit tests, but **the JMH benchmark runs through `exec-maven-plugin`'s own `java` invocation, which does not inherit surefire's `argLine`** -- the `jmh-vector` Maven profile below passes both flags directly as JVM arguments for exactly this reason. Without them, JVector silently falls back to its non-vectorized code path (still correct, just slower): measured directly, `load` for 10,000 vectors took 24.4s without the flags and 11.6s with them, roughly a 2x difference. If a results file ever looks unexpectedly slow across the board, check the JMH console output for `WARNING: Java vector incubator module is not readable` first.

## Run

Use Java 21 and Maven. From the repository root, compile the reactor once:

```sh
mvn -pl bundles/io.github.linkedfactory.core -am clean install -DskipTests
```

Run the full matrix, writing JMH JSON under the ignored `target` directory:

```sh
mvn -pl bundles/io.github.linkedfactory.core -Pjmh-vector test-compile exec:exec
```

The default run uses one JVM fork, one warmup invocation, three measured invocations, and one thread per case. Nine cases cover three sizes and three operations (load, remove, search). Given the `remove` limitation above, do not run the default full matrix unattended until that is fixed, or override `batchSizes` to exclude the larger sizes for `remove` specifically -- there is no per-operation size override today, so the simplest safe option is to restrict `batchSizes` to `10000` for a `remove`-inclusive run, and only use `100000`/`1000000` for `load`/`search` by accepting that `remove` at that size will also run (and take a very long time) unless `JVectorIndex` is fixed first.

To run a small smoke check first (this is what was used to validate the benchmark and measure the numbers quoted above):

```sh
mvn -pl bundles/io.github.linkedfactory.core -Pjmh-vector \
  -Djmh.batch.sizes=10000 -Djmh.warmups=0 -Djmh.measurements=1 \
  -Djmh.result.file=target/vector-backend-smoke.json \
  test-compile exec:exec
```

In PowerShell, use `mvn.cmd` and quote each Maven `-D` property, for example `'-Djmh.batch.sizes=10000'`.

The corresponding results are written to `bundles/io.github.linkedfactory.core/target/vector-backend-jmh.json` (or the path given via `-Djmh.result.file`). Compare `load` with `load`, `remove` with `remove`, and `search` with `search` at the same `batchSize`. The `jmh-vector` profile runs `VectorBackendLifecycleBenchmarkRunner`, a small `main()` that builds JMH's `Options` via the Java API and reads `batchSize`/`warmups`/`measurements`/`forks`/`resultFile` from environment variables set by the profile, the same pattern `FtsBackendLifecycleBenchmarkRunner` uses and for the same reason: invoking `org.openjdk.jmh.Main` directly through `exec-maven-plugin`'s command-line arguments has previously been found to silently drop `@Param` overrides.
