/*
 * Copyright (c) 2022 Fraunhofer IWU.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.linkedfactory.core.kvin.benchmark;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.iceberg.KvinIceberg;
import io.github.linkedfactory.core.kvin.leveldb.KvinLevelDb;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.commons.io.FileUtils;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.CommandLineOptionException;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Raw write throughput and read latency of the Kvin stores.
 * <p>
 * Each trial opens a fresh store and preloads it with {@link #PRELOAD_VALUES} values of 1000
 * items in 1000 contexts with random time gaps. {@link #write} then appends batches of
 * {@link #batchSize} values per put. Its primary score is puts per second, and the secondary
 * score {@code write:values} is values per second. {@link #read} fetches the two
 * latest values of series that were written during the preload. The data is generated with
 * fixed seeds, so every store sees the same values.
 * <p>
 * Run {@link #main(String[])} with regular JMH arguments to select benchmarks and parameters,
 * for example {@code -p store=iceberg KvinWriteReadBenchmark.write}.
 */
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xmx4g", "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn"})
@Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
public class KvinWriteReadBenchmark {
	public enum StoreType {
		leveldb {
			@Override
			Kvin open(Path dir) {
				return new KvinLevelDb(dir.toFile());
			}
		},
		iceberg {
			@Override
			Kvin open(Path dir) {
				return new KvinIceberg(dir.toAbsolutePath().toString());
			}
		};

		abstract Kvin open(Path dir);
	}

	static final int SEED = 200;
	static final int KEYS = 1000;
	static final int PRELOAD_VALUES = 1_000_000;
	static final int PRELOAD_BATCH_SIZE = 100_000;
	static final int READ_SERIES = 10_000;
	static final long START_TIME = 1478252048736L;
	static final URI VALUE_PROPERTY = URIs.createURI("property:value");

	@Param
	public StoreType store;

	/**
	 * Number of values per put in {@link #write}.
	 */
	@Param("100000")
	public int batchSize;

	Path directory;
	Kvin kvin;
	URI[] items;
	URI[] contexts;
	int[] keys;
	Random writeRandom;
	long currentTime;
	// item and context index of series written during the preload, in write order
	int[] readItems;
	int[] readContexts;
	int readIndex;

	public static void main(String[] args) throws RunnerException, CommandLineOptionException {
		CommandLineOptions cmdOptions = new CommandLineOptions(args);
		OptionsBuilder builder = new OptionsBuilder();
		builder.parent(cmdOptions);
		if (cmdOptions.getIncludes().isEmpty()) {
			builder.include(KvinWriteReadBenchmark.class.getSimpleName());
		}
		new Runner(builder.build()).run();
	}

	@Setup(Level.Trial)
	public void setup() throws IOException {
		directory = Files.createTempDirectory("kvin-write-read-benchmark");
		kvin = store.open(directory);

		Random keyRandom = new Random(SEED);
		keys = new int[KEYS];
		items = new URI[KEYS];
		contexts = new URI[KEYS];
		for (int i = 0; i < KEYS; i++) {
			keys[i] = keyRandom.nextInt(Integer.MAX_VALUE);
			// the key appears in one path segment only: komma's URI hash code XORs the segment
			// hash codes, so a key repeated in two segments would give all items the same hash
			items[i] = URIs.createURI("http://linkedfactory.github.io/e3fabrik/rollex/" + keys[i] + "/measured-point-1");
			contexts[i] = URIs.createURI("ctx:" + keys[i]);
		}

		writeRandom = new Random(SEED);
		currentTime = START_TIME;
		readItems = new int[READ_SERIES];
		readContexts = new int[READ_SERIES];
		int written = 0;
		List<KvinTuple> batch = new ArrayList<>(PRELOAD_BATCH_SIZE);
		while (written < PRELOAD_VALUES) {
			int item = writeRandom.nextInt(KEYS);
			int context = writeRandom.nextInt(KEYS);
			if (written < READ_SERIES) {
				readItems[written] = item;
				readContexts[written] = context;
			}
			batch.add(nextTuple(item, context));
			written++;
			if (batch.size() == PRELOAD_BATCH_SIZE) {
				kvin.put(batch);
				batch.clear();
			}
		}
		if (!batch.isEmpty()) {
			kvin.put(batch);
		}
	}

	@TearDown(Level.Trial)
	public void tearDown() throws IOException {
		kvin.close();
		FileUtils.deleteDirectory(directory.toFile());
	}

	private KvinTuple nextTuple(int item, int context) {
		int key = keys[item];
		Object value = key % 2 == 0 ? (Object) writeRandom.nextGaussian() : (Object) writeRandom.nextLong(100000);
		KvinTuple tuple = new KvinTuple(contexts[context], items[item], VALUE_PROPERTY, currentTime, value);
		currentTime += writeRandom.nextInt(1000);
		return tuple;
	}

	/**
	 * Counts the values written by {@link #write}, so that JMH also reports values per second.
	 */
	@State(Scope.Thread)
	@AuxCounters(AuxCounters.Type.OPERATIONS)
	public static class WriteCounters {
		public long values;

		@Setup(Level.Iteration)
		public void reset() {
			values = 0;
		}
	}

	/**
	 * Appends one batch of {@link #batchSize} values after the preloaded data.
	 */
	@Benchmark
	@BenchmarkMode(Mode.Throughput)
	@OutputTimeUnit(TimeUnit.SECONDS)
	public void write(WriteCounters counters) {
		List<KvinTuple> batch = new ArrayList<>(batchSize);
		for (int i = 0; i < batchSize; i++) {
			batch.add(nextTuple(writeRandom.nextInt(KEYS), writeRandom.nextInt(KEYS)));
		}
		kvin.put(batch);
		counters.values += batchSize;
	}

	/**
	 * Fetches the two latest values of a series written during the preload.
	 */
	@Benchmark
	@BenchmarkMode(Mode.AverageTime)
	@OutputTimeUnit(TimeUnit.MICROSECONDS)
	public int read(Blackhole blackhole) {
		int i = readIndex;
		readIndex = (readIndex + 1) % READ_SERIES;
		int count = 0;
		try (IExtendedIterator<KvinTuple> tuples = kvin.fetch(items[readItems[i]], VALUE_PROPERTY,
				contexts[readContexts[i]], currentTime, START_TIME, 2, 0, null)) {
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
