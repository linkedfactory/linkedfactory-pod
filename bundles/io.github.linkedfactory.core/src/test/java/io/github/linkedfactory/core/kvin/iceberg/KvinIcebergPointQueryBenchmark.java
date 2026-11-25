package io.github.linkedfactory.core.kvin.iceberg;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.commons.io.FileUtils;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Point-query latency of KvinIceberg.
 *
 * <p>Data is ingested like a live system: each put covers one time window for
 * all series, so the Iceberg table consists of one compacted file plus several
 * small recent files. Run with {@link #main(String[])} from the test classpath.</p>
 */
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xmx2g", "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn"})
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class KvinIcebergPointQueryBenchmark {
	static final int ITEMS = 100;
	static final int PROPERTIES = 4;
	static final int WINDOWS = 40;
	static final int VALUES_PER_WINDOW = 25;
	static final long STEP = 10 * 60 * 1000L; // 10 minutes, so the data spans about one week
	static final long START = 1_700_000_000_000L;
	static final long END = START + (long) (WINDOWS * VALUES_PER_WINDOW - 1) * STEP;

	File directory;
	Kvin store;
	URI[] items;
	URI[] properties;
	int counter;

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder()
				.include(KvinIcebergPointQueryBenchmark.class.getSimpleName())
				.build();
		new Runner(opt).run();
	}

	@Setup(Level.Trial)
	public void setup() throws IOException {
		directory = Files.createTempDirectory("kvin-point-query").toFile();
		store = new KvinIceberg(directory.getPath());
		items = new URI[ITEMS];
		for (int i = 0; i < ITEMS; i++) {
			items[i] = URIs.createURI("http://example.org/item/" + i);
		}
		properties = new URI[PROPERTIES];
		for (int p = 0; p < PROPERTIES; p++) {
			properties[p] = URIs.createURI("http://example.org/property/" + p);
		}
		for (int w = 0; w < WINDOWS; w++) {
			List<KvinTuple> batch = new ArrayList<>(ITEMS * PROPERTIES * VALUES_PER_WINDOW);
			for (int i = 0; i < ITEMS; i++) {
				for (int p = 0; p < PROPERTIES; p++) {
					for (int v = 0; v < VALUES_PER_WINDOW; v++) {
						long time = START + (long) (w * VALUES_PER_WINDOW + v) * STEP;
						batch.add(new KvinTuple(Kvin.DEFAULT_CONTEXT, items[i], properties[p], time, 0, (double) time));
					}
				}
			}
			store.put(batch);
		}
	}

	@TearDown(Level.Trial)
	public void tearDown() throws IOException {
		store.close();
		FileUtils.deleteDirectory(directory);
	}

	private int next() {
		return (counter = counter * 1103515245 + 12345) >>> 1;
	}

	/** Latest value of one series. */
	@Benchmark
	public int latestValue(Blackhole blackhole) {
		int n = next();
		return consume(store.fetch(items[n % ITEMS], properties[(n / ITEMS) % PROPERTIES], Kvin.DEFAULT_CONTEXT, 1), blackhole);
	}

	/** Exact value at a random timestamp. */
	@Benchmark
	public int valueAtTime(Blackhole blackhole) {
		int n = next();
		long time = START + (n % (WINDOWS * VALUES_PER_WINDOW)) * STEP;
		return consume(store.fetch(items[n % ITEMS], properties[(n / ITEMS) % PROPERTIES], Kvin.DEFAULT_CONTEXT,
				time, time, 0, 0, null), blackhole);
	}

	/** Ten most recent values before a random timestamp. */
	@Benchmark
	public int recentValuesBefore(Blackhole blackhole) {
		int n = next();
		long end = START + (n % (WINDOWS * VALUES_PER_WINDOW)) * STEP;
		return consume(store.fetch(items[n % ITEMS], properties[(n / ITEMS) % PROPERTIES], Kvin.DEFAULT_CONTEXT,
				end, 0, 10, 0, null), blackhole);
	}

	private static int consume(IExtendedIterator<KvinTuple> tuples, Blackhole blackhole) {
		int count = 0;
		try (tuples) {
			while (tuples.hasNext()) {
				blackhole.consume(tuples.next());
				count++;
			}
		}
		if (count == 0) {
			throw new IllegalStateException("Benchmark query returned no values");
		}
		return count;
	}
}
