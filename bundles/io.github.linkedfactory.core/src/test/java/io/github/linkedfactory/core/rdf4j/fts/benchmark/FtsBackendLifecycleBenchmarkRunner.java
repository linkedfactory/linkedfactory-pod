package io.github.linkedfactory.core.rdf4j.fts.benchmark;

import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class FtsBackendLifecycleBenchmarkRunner {
	public static void main(String[] args) throws RunnerException {
		String[] backends = env("JMH_BACKENDS", "lucene,elastic").split(",");
		String[] batchSizes = env("JMH_BATCH_SIZES", "10000,100000,1000000").split(",");
		int warmups = Integer.parseInt(env("JMH_WARMUPS", "1"));
		int measurements = Integer.parseInt(env("JMH_MEASUREMENTS", "3"));
		int forks = Integer.parseInt(env("JMH_FORKS", "1"));
		String resultFile = env("JMH_RESULT_FILE", "target/fts-backend-jmh.json");

		Options options = new OptionsBuilder()
				.include(FtsBackendLifecycleBenchmark.class.getSimpleName())
				.param("backend", backends)
				.param("batchSize", batchSizes)
				.warmupIterations(warmups)
				.measurementIterations(measurements)
				.forks(forks)
				.resultFormat(ResultFormatType.JSON)
				.result(resultFile)
				.build();
		new Runner(options).run();
	}

	private static String env(String name, String fallback) {
		String value = System.getenv(name);
		return value == null || value.isBlank() ? fallback : value;
	}
}
