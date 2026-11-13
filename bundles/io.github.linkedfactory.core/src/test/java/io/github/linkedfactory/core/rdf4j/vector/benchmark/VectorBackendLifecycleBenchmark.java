package io.github.linkedfactory.core.rdf4j.vector.benchmark;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.linkedfactory.core.rdf4j.vector.JVectorIndex;
import io.github.linkedfactory.core.rdf4j.vector.JVectorSearchBackend;
import io.github.linkedfactory.core.rdf4j.vector.JVectorSearchService;
import io.github.linkedfactory.core.rdf4j.vector.VectorSearchRequest;
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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Threads(1)
@Fork(1)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class VectorBackendLifecycleBenchmark {
	private static final int COMMIT_SIZE = 5_000;
	private static final long VECTOR_SEED = 42L;
	private static final int DIMENSION = 768;
	private static final int SEARCH_QUERIES_PER_INVOCATION = 100;

	@Param({ "10000", "100000", "1000000" })
	public int batchSize;

	private List<Map<String, float[]>> batches;
	private float[] queryVector;
	private JVectorIndex index;
	private JVectorSearchService service;
	private JVectorSearchBackend searchBackend;

	@Setup(Level.Trial)
	public void prepareVectors() {
		batches = new ArrayList<>((batchSize + COMMIT_SIZE - 1) / COMMIT_SIZE);
		Random random = new Random(VECTOR_SEED);
		for (int offset = 0; offset < batchSize; offset += COMMIT_SIZE) {
			Map<String, float[]> batch = new LinkedHashMap<>(COMMIT_SIZE);
			for (int i = offset; i < Math.min(offset + COMMIT_SIZE, batchSize); i++) {
				batch.put("urn:vector-benchmark:item:" + i, randomUnitVector(random));
			}
			batches.add(batch);
		}
		queryVector = randomUnitVector(random);
	}

	private static float[] randomUnitVector(Random random) {
		float[] vector = new float[DIMENSION];
		double sumSquares = 0;
		for (int i = 0; i < DIMENSION; i++) {
			vector[i] = (float) random.nextGaussian();
			sumSquares += vector[i] * vector[i];
		}
		float norm = (float) Math.sqrt(sumSquares);
		for (int i = 0; i < DIMENSION; i++) {
			vector[i] /= norm;
		}
		return vector;
	}

	@Setup(Level.Invocation)
	public void prepareIndex(BenchmarkParams params) throws Exception {
		index = new JVectorIndex(DIMENSION, VectorSimilarityFunction.COSINE);
		service = new JVectorSearchService(index);
		searchBackend = new JVectorSearchBackend(index);
		try {
			if (params.getBenchmark().endsWith(".remove") || params.getBenchmark().endsWith(".search")) {
				runBatches(false);
				if (index.size() != batchSize) {
					throw new IllegalStateException("Setup did not index " + batchSize + " vectors");
				}
			}
		} catch (Exception e) {
			try {
				index.close();
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
	public void remove() throws Exception {
		runBatches(true);
	}

	@Benchmark
	public void search() throws Exception {
		VectorSearchRequest request = new VectorSearchRequest(queryVector, 10, null);
		int totalHits = 0;
		for (int i = 0; i < SEARCH_QUERIES_PER_INVOCATION; i++) {
			totalHits += searchBackend.search(request).size();
		}
		if (totalHits == 0) {
			throw new IllegalStateException("Expected hits for the benchmark query vector");
		}
	}

	@TearDown(Level.Invocation)
	public void verifyAndClean(BenchmarkParams params) throws Exception {
		try {
			int expected = params.getBenchmark().endsWith(".remove") ? 0 : batchSize;
			int actual = index.size();
			if (actual != expected) {
				throw new IllegalStateException("Expected " + expected + " indexed vectors, found " + actual);
			}
		} finally {
			index.close();
		}
	}

	private void runBatches(boolean removing) throws Exception {
		for (Map<String, float[]> batch : batches) {
			service.begin();
			if (removing) {
				for (String iri : batch.keySet()) {
					service.remove(iri);
				}
			} else {
				for (Map.Entry<String, float[]> entry : batch.entrySet()) {
					service.upsert(entry.getKey(), entry.getValue());
				}
			}
			service.commit();
		}
	}
}
