package io.github.linkedfactory.core.kvin.memtable;

import io.github.linkedfactory.core.kvin.KvinTuple;
import net.enilink.komma.core.URIs;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import scala.Array;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Warmup(iterations = 2)
@BenchmarkMode({Mode.Throughput})
@Fork(value = 1, jvmArgs = {"-Xms2G", "-Xmx2G", "-XX:+UseG1GC"})
@Measurement(iterations = 3, time = 20000, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
public class KvinMemtableBenchmark {
	Random random;
	Path tempDir;
	long startTime;
	KvinMemTable kvin;

	public KvinMemtableBenchmark() {
		this.random = new Random();
	}

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder()
				.include(KvinMemtableBenchmark.class.getSimpleName()) // adapt to control which benchmark tests to run
				.forks(1)
				.build();
		new Runner(opt).run();
	}

	@Setup(Level.Iteration)
	public void beforeClass() throws IOException {
		this.tempDir = Files.createTempDirectory("memtable-benchmark");
		this.startTime = System.currentTimeMillis();
		this.kvin = new KvinMemTable(tempDir);
	}

	@TearDown(Level.Iteration)
	public void afterClass() throws IOException {
		kvin.close();
	}

	@Benchmark
	public void loadData(Blackhole blackhole) {
		int count = 10000;
		List<KvinTuple> batch = new ArrayList<>();
		for (long i = 1; i <= count; i++) {
			long itemId = random.nextLong(10000);
			long contextId = random.nextLong(10);
			long propertyId = random.nextLong(5);
			var itemUri = URIs.createURI("http://linkedfactory.github.io/" + itemId + "/e3fabrik/rollex/" + itemId + "/measured-point-1");
			var propertyUri = URIs.createURI("property:" + propertyId);
			var contextUri = URIs.createURI("ctx:" + contextId);

			long time = System.nanoTime();

			batch.add(new KvinTuple(itemUri, propertyUri, contextUri, time, random.nextDouble()));

			if (batch.size() % 10000 == 0 || i == count) {
				kvin.put(batch);
				batch.clear();
			}
		}
	}
}

