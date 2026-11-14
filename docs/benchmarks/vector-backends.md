# JVector embedded vector-search load, remove, and search benchmark

`VectorBackendLifecycleBenchmark` measures the embedded `JVectorIndex`/`VectorSearchService`/`VectorSearchBackend` implementation on the `JvectorPoC` branch. This module is not wired into `FtsSail` or any SPARQL vocabulary; it is a standalone in-process nearest-neighbor index, benchmarked the same way `FtsBackendLifecycleBenchmark` benchmarks the FTS backends (see `fts-backends.md`), so the two are structurally comparable even though vector search and full-text search answer different questions.

Each JMH invocation loads, removes, or searches exactly 10,000, 100,000, or 1,000,000 deterministic 768-dimension vectors (matching `google/embeddinggemma-300m`'s real output dimension), drawn from a seeded Gaussian distribution and L2-normalized to unit length, compared with cosine similarity. Each vector has a distinct subject IRI. The benchmark submits them in commits of 5,000 vectors, the same chunking convention used throughout this project's vector-search benchmarking (an earlier, unchunked version of the Lucene vector benchmark on a different branch needed far more heap for no real reason -- see that branch's history -- so this one is chunked from the start).

`load` times calls to `begin`, `upsert`, and `commit` for every chunk of the selected batch; one JMH operation is the complete batch, so the reported unit is **milliseconds per batch**. Divide the batch size by `score / 1000` for approximate vectors per second. `remove` starts with the batch already loaded (outside the timed method) and times `begin`/`remove`/`commit` for every vector, one at a time, across the same 5,000-vector commit chunks. `search` starts with the batch already loaded and times 100 repeated top-10 nearest-neighbor queries against the same fixed query vector, so the reported unit is **milliseconds per 100 queries**; divide by 100 for approximate milliseconds per query. `search` asserts every run returns at least one hit; `load`/`search` assert the expected vector count; `remove` asserts zero vectors remain.

**`remove` no longer re-splices the whole graph on every single deletion.** It originally did: `JVectorIndex.remove()` called `GraphIndexBuilder.removeDeletedNodes()` synchronously after every node removal -- a deliberate correctness fix from the original PoC work (an earlier version left the graph's entry point unreachable after a delete if a replacement node was added before the next compaction). Reading JVector 4.0.2's own source confirmed why that was so expensive: `removeDeletedNodes()` does a full `IntStream.range(0, graph.getIdUpperBound())` scan of every node currently in the graph, not just the deleted ones, so its cost is O(graph size) regardless of how many nodes are actually pending removal. Calling it once per single deletion made removing a batch of n vectors cost roughly O(n) full-graph scans against a graph that stays close to size n for most of the removal -- a real O(n^2) blowup. Measured directly (JMH, same benchmark, before the fix): removing a freshly-loaded batch of 10,000 vectors took **345,965 ms/op** (~346 seconds).

The fix (now in `JVectorIndex`): deletions are tracked in a small pending set and `removeDeletedNodes()` is only called when that set crosses a threshold -- *except* when the just-deleted node is the graph's current entry point (checked via the public `ImmutableGraphIndex.View.entryNode()`), in which case the repair still runs immediately, exactly as before. That one case is the only one the original fix actually needed: a soft-deleted node that isn't the entry point remains a perfectly good navigation bridge until the next batch repair (JVector's own search already excludes deleted nodes from *results* via `liveNodes()`, independent of whether they've been physically removed from the graph yet), so deferring its physical removal is safe. A new regression test (`removingManyVectorsOneAtATimeKeepsRemainingOnesSearchable`) removes 50 vectors one at a time and checks the sole survivor stays searchable after every single removal, specifically to guard the entry-point case under this deferred scheme.

Measured directly after the fix (JMH, same benchmark, same machine):

| batchSize | remove, before | remove, after | speedup |
|-|-|-|-|
| 10,000 | 345,965 ms/op (346s) | 3,696 ms/op (3.7s) | ~94x |
| 100,000 | not run (would be hours) | 90,155 ms/op (90.2s) | n/a |

`remove` at 100,000 and 1,000,000 can now be run as part of the normal full matrix; it no longer needs special handling.

**The threshold is now a capped fraction of the graph's current size, not a flat count.** A flat `COMPACTION_THRESHOLD = 1,000` flushes the same size batch whether the graph has 10,000 vectors or 1,000,000, so the fixed cost of each `removeDeletedNodes()` full-graph scan gets amortized over fewer and fewer deletions as the graph grows -- worse amortization exactly where the fix matters most. The threshold is now `min(max(1,000, 10% of the graph's current size), 10,000)`: it grows with the graph (so larger graphs flush less often, in bigger batches) but is capped at 10,000 regardless of graph size. The cap exists because an uncapped percentage threshold was tried first and crashed with `OutOfMemoryError` at 1,000,000 vectors under the same heap that handled every other case fine -- a 10% threshold at that scale lets up to ~100,000 deletions accumulate before a single flush, and `removeDeletedNodes()`'s own repair step builds a transient map of candidate replacement edges sized by how many surviving nodes are affected in that one flush, so a 100x bigger batch meant a 100x bigger transient structure held in memory at once. Capping the batch size bounds that transient memory regardless of graph size, while still capturing virtually all of the amortization benefit (10,000-vector batches are already enough to amortize the scan cost well; growing them further has diminishing returns). Below 100,000 vectors the cap never applies, since 10% of the graph is already under 10,000.

Measured directly (JMH, same benchmark, same machine) comparing the flat threshold against the capped percentage threshold:

| batchSize | remove, flat threshold | remove, capped percentage threshold | speedup |
|-|-|-|-|
| 10,000 | 3,696 ms/op (3.7s) | 1,103 ms/op (1.1s) | ~3.4x |
| 100,000 | 90,155 ms/op (90.2s) | 17,464 ms/op (17.5s) | ~5.2x |

`load` and `search` are unaffected, as expected (this change only touches the delete path): `load` 10,000 = 3,958 ms/op, `load` 100,000 = 79,451 ms/op, `search` 10,000 = 4.2 ms/op, `search` 100,000 = 4.0 ms/op. An informal (non-JMH) sweep in this project's experiments repo also ran the 1,000,000 case end to end -- remove dropped from 69.2 minutes (flat threshold) to 8.6 minutes (capped percentage threshold), with no `OutOfMemoryError` -- but that number isn't from this JMH harness, so it's recorded for context here rather than in the table above.

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

The default run uses one JVM fork, one warmup invocation, three measured invocations, and one thread per case. Nine cases cover three sizes and three operations (load, remove, search). The full matrix including `remove` at every size is safe to run unattended (see the fix above); it is still a long run -- `load` and `remove` at 1,000,000 are each expected to take minutes, not seconds.

To run a small smoke check first (this is what was used to validate the benchmark and measure the numbers quoted above):

```sh
mvn -pl bundles/io.github.linkedfactory.core -Pjmh-vector \
  -Djmh.batch.sizes=10000 -Djmh.warmups=0 -Djmh.measurements=1 \
  -Djmh.result.file=target/vector-backend-smoke.json \
  test-compile exec:exec
```

In PowerShell, use `mvn.cmd` and quote each Maven `-D` property, for example `'-Djmh.batch.sizes=10000'`.

The corresponding results are written to `bundles/io.github.linkedfactory.core/target/vector-backend-jmh.json` (or the path given via `-Djmh.result.file`). Compare `load` with `load`, `remove` with `remove`, and `search` with `search` at the same `batchSize`. The `jmh-vector` profile runs `VectorBackendLifecycleBenchmarkRunner`, a small `main()` that builds JMH's `Options` via the Java API and reads `batchSize`/`warmups`/`measurements`/`forks`/`resultFile` from environment variables set by the profile, the same pattern `FtsBackendLifecycleBenchmarkRunner` uses and for the same reason: invoking `org.openjdk.jmh.Main` directly through `exec-maven-plugin`'s command-line arguments has previously been found to silently drop `@Param` overrides.
