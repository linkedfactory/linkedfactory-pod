package io.github.linkedfactory.service.benchmark;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Compares CSV and Parquet uploads for growing payload sizes. Each request contains
 * {@code rows * CHANNEL_COUNT} tuples; large Parquet payloads span multiple row groups.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(2)
@Threads(1)
public class KvinIngestionScalingBenchmark {
	@State(Scope.Thread)
	public static class ScalingState extends KvinIngestionBenchmark.BenchmarkState {
		@Param({"5000", "100000"})
		public int rows;

		@Override
		protected List<KvinIngestionWorkload> createWorkloads() {
			return List.of(new KvinIngestionWorkload(0, rows));
		}
	}

	@Benchmark
	public void postCsv(ScalingState state) throws IOException {
		state.postCsv();
	}

	@Benchmark
	public void postParquet(ScalingState state) throws IOException {
		state.postParquet();
	}
}
