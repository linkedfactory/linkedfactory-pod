package io.github.linkedfactory.core.rdf4j.fts.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.linkedfactory.core.rdf4j.fts.FtsSearchService;
import io.github.linkedfactory.core.rdf4j.fts.HttpFtsSearchService;
import io.github.linkedfactory.core.rdf4j.fts.LuceneFtsIndex;
import io.github.linkedfactory.core.rdf4j.fts.LuceneFtsSearchService;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.FSDirectory;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Threads(1)
@Fork(1)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class FtsBackendLifecycleBenchmark {
	private static final int COMMIT_SIZE = 5_000;
	private static final long LITERAL_SEED = 42L;
	private static final String[] VOCABULARY = {
			"the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog",
			"model", "data", "search", "index", "query", "value", "record", "field",
			"graph", "triple", "engine", "vector", "token", "term", "document", "corpus",
			"result", "system", "process", "update", "delete", "insert", "commit", "transaction",
			"batch", "stream", "cache", "memory", "storage", "network", "cluster", "node",
			"latency", "throughput", "scale", "metric", "sample", "measure", "duration", "pattern"
	};
	private static final double[] VOCABULARY_CUMULATIVE_WEIGHTS = buildCumulativeWeights();
	private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();
	private static final ObjectMapper MAPPER = new ObjectMapper();
	private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

	@Param({ "lucene", "elastic" })
	public String backend;

	@Param({ "10000", "100000", "1000000" })
	public int batchSize;

	private List<Set<Statement>> batches;
	private Path workDir;
	private String indexName;
	private String endpoint;
	private boolean elasticIndexCreated;
	private LuceneFtsIndex luceneIndex;
	private HttpFtsSearchService httpService;
	private FtsSearchService service;

	@Setup(Level.Trial)
	public void prepareStatements() {
		batches = new ArrayList<>((batchSize + COMMIT_SIZE - 1) / COMMIT_SIZE);
		IRI context = VF.createIRI("urn:fts-benchmark:model");
		Random random = new Random(LITERAL_SEED);
		for (int offset = 0; offset < batchSize; offset += COMMIT_SIZE) {
			Set<Statement> batch = new LinkedHashSet<>(COMMIT_SIZE);
			for (int i = offset; i < Math.min(offset + COMMIT_SIZE, batchSize); i++) {
				batch.add(VF.createStatement(
						VF.createIRI("urn:fts-benchmark:item:" + i),
						VF.createIRI("urn:fts-benchmark:property:" + (i % 16)),
						VF.createLiteral(randomLiteral(random)), context));
			}
			batches.add(batch);
		}
	}

	private static double[] buildCumulativeWeights() {
		double[] cumulative = new double[VOCABULARY.length];
		double sum = 0;
		for (int i = 0; i < VOCABULARY.length; i++) {
			sum += 1.0 / (i + 1);
			cumulative[i] = sum;
		}
		return cumulative;
	}

	private static String randomLiteral(Random random) {
		int wordCount = 2 + random.nextInt(7);
		StringBuilder literal = new StringBuilder();
		for (int w = 0; w < wordCount; w++) {
			if (w > 0) {
				literal.append(' ');
			}
			literal.append(pickWeightedWord(random));
		}
		return literal.toString();
	}

	private static String pickWeightedWord(Random random) {
		double target = random.nextDouble() * VOCABULARY_CUMULATIVE_WEIGHTS[VOCABULARY_CUMULATIVE_WEIGHTS.length - 1];
		int index = Arrays.binarySearch(VOCABULARY_CUMULATIVE_WEIGHTS, target);
		if (index < 0) {
			index = -index - 1;
		}
		return VOCABULARY[Math.min(index, VOCABULARY.length - 1)];
	}

	@Setup(Level.Invocation)
	public void prepareIndex(BenchmarkParams params) throws Exception {
		luceneIndex = null;
		httpService = null;
		service = null;
		elasticIndexCreated = false;
		indexName = null;
		workDir = Files.createTempDirectory("fts-backend-jmh-");
		try {
			if ("lucene".equals(backend)) {
				luceneIndex = new LuceneFtsIndex(workDir.resolve("index"));
				service = new LuceneFtsSearchService(luceneIndex);
			} else if ("elastic".equals(backend)) {
				endpoint = System.getenv().getOrDefault("FTS_BENCHMARK_ENDPOINT", "http://localhost:9200");
				endpoint = endpoint.replaceAll("/+$", "");
				indexName = "linkedfactory-fts-jmh-" + UUID.randomUUID();
				request("PUT", "/" + indexName, "{}");
				elasticIndexCreated = true;
				httpService = new HttpFtsSearchService(endpoint, "/" + indexName + "/_bulk", true,
						workDir.resolve("outbox").toString());
				service = httpService;
			} else {
				throw new IllegalArgumentException("Unsupported FTS backend: " + backend);
			}
			if (params.getBenchmark().endsWith(".delete")) {
				runBatches(false);
				if (documentCount() != batchSize) {
					throw new IllegalStateException("Delete setup did not index " + batchSize + " triples");
				}
			}
		} catch (Exception e) {
			try {
				clean();
			} catch (Exception cleanupError) {
				e.addSuppressed(cleanupError);
			}
			throw e;
		}
	}

	@Benchmark
	public void load() throws Exception {
		runBatches(false);
	}

	@Benchmark
	public void delete() throws Exception {
		runBatches(true);
	}

	@TearDown(Level.Invocation)
	public void verifyAndClean(BenchmarkParams params) throws Exception {
		try {
			int expected = params.getBenchmark().endsWith(".delete") ? 0 : batchSize;
			int actual = documentCount();
			if (actual != expected) {
				throw new IllegalStateException("Expected " + expected + " indexed triples, found " + actual);
			}
		} finally {
			clean();
		}
	}

	private void clean() throws Exception {
		try {
			if (luceneIndex != null) {
				luceneIndex.close();
			}
		} finally {
			try {
				if (httpService != null) {
					httpService.shutdown();
				}
			} finally {
				try {
					if (elasticIndexCreated) {
						request("DELETE", "/" + indexName, "");
					}
				} finally {
					try (var paths = Files.walk(workDir)) {
						for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
							Files.deleteIfExists(path);
						}
					}
				}
			}
		}
	}

	private void runBatches(boolean removing) throws Exception {
		for (Set<Statement> batch : batches) {
			service.begin();
			service.addRemoveStatements(removing ? Set.of() : batch, removing ? batch : Set.of());
			service.commit();
		}
	}

	private int documentCount() throws Exception {
		if (luceneIndex != null) {
			try (var directory = FSDirectory.open(workDir.resolve("index"));
					var reader = DirectoryReader.open(directory)) {
				return reader.numDocs();
			}
		}
		request("POST", "/" + indexName + "/_refresh", "");
		return MAPPER.readTree(request("GET", "/" + indexName + "/_count", "")).path("count").asInt(-1);
	}

	private String request(String method, String path, String body) throws IOException, InterruptedException {
		HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(endpoint + path))
				.timeout(Duration.ofSeconds(60));
		if (!body.isEmpty()) {
			builder.header("Content-Type", "application/json");
		}
		builder.method(method, body.isEmpty() ? HttpRequest.BodyPublishers.noBody()
				: HttpRequest.BodyPublishers.ofString(body));
		HttpResponse<String> response = HTTP.send(builder.build(), HttpResponse.BodyHandlers.ofString());
		if (response.statusCode() >= 300) {
			throw new IOException("Elasticsearch HTTP " + response.statusCode() + " " + path + ": " + response.body());
		}
		return response.body();
	}
}
