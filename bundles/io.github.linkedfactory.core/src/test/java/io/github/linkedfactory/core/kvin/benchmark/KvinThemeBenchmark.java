package io.github.linkedfactory.core.kvin.benchmark;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.iceberg.KvinIceberg;
import io.github.linkedfactory.core.kvin.leveldb.KvinLevelDb;
import io.github.linkedfactory.core.kvin.parquet.KvinParquet;
import io.github.linkedfactory.core.rdf4j.common.BaseFederatedServiceResolver;
import io.github.linkedfactory.core.rdf4j.kvin.KvinFederatedService;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.commons.io.FileUtils;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.CommandLineOptionException;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Compares Kvin stores on realistic use-case themes.
 * <p>
 * Every combination of {@link StoreType} and {@link Theme} is a separate trial: the store is
 * created and filled with the theme's data once, then the theme's SPARQL query and a
 * single-item fetch are measured against it. The data is generated with fixed seeds, so every
 * trial sees the same values. Times are epoch milliseconds.
 * <p>
 * Run {@link #main(String[])} with regular JMH arguments to select benchmarks and parameters,
 * for example {@code -p store=iceberg -p theme=machine KvinThemeBenchmark.query}.
 */
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xmx4g", "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn"})
@Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class KvinThemeBenchmark {
	public enum StoreType {
		leveldb {
			@Override
			Kvin open(Path dir) {
				return new KvinLevelDb(dir.toFile());
			}
		},
		parquet {
			@Override
			Kvin open(Path dir) {
				return new KvinParquet(dir.toAbsolutePath() + "/");
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

	public enum Theme {
		/**
		 * A production line: a work piece starts every 90 seconds and passes the sections
		 * "start", "middle" and "end", 30 minutes apart. Each work piece has 49 process
		 * parameters of mixed types. Written in one bulk import. The query finds the work pieces
		 * that are still in section "start" and returns all their values.
		 */
		process(PROCESS_NAMESPACE, "workPiece1995",
				"prefix prod: <" + PROCESS_NAMESPACE + ">\n" +
						"select * {\n" +
						"  service <kvin:> {\n" +
						"    { select distinct ?workPiece { prod:line prod:workPiece [ <kvin:value> ?workPiece ] . } }\n" +
						"    ?workPiece prod:section [ <kvin:value> \"start\" ; <kvin:limit> 1 ] .\n" +
						"    ?workPiece ?property [ <kvin:value> ?v ; <kvin:time> ?t ] .\n" +
						"  }\n" +
						"}") {
			@Override
			List<List<KvinTuple>> batches() {
				return List.of(generateProcessData(namespace, PROCESS_WORK_PIECES));
			}
		},
		/**
		 * A press-fit station: a part every 30 seconds, with an angle and a force curve of 250
		 * samples for each of six channels. All samples of a curve share the part's time stamp and
		 * are ordered by sequence number. Written in one bulk import. The query joins angle and
		 * force of all channels by time and index for six parts.
		 */
		machine(MACHINE_NAMESPACE, "part6",
				"prefix press: <" + MACHINE_NAMESPACE + ">\n" +
						"select * {\n" +
						"  values ?part { press:part6 press:part7 press:part8 press:part9 press:part10 press:part11 }\n" +
						"  service <kvin:> {\n" + machineChannelPatterns() +
						"  }\n" +
						"} order by ?part ?s") {
			@Override
			List<List<KvinTuple>> batches() {
				return List.of(generateMachineData(namespace, MACHINE_PARTS));
			}
		},
		/**
		 * Plant monitoring: 50 sensors report temperature, pressure, power and an energy
		 * counter every 5 seconds. Written in time order like live ingestion, one put per
		 * minute of data. The query is a dashboard: one-minute averages of the temperature of
		 * ten sensors over the last 30 minutes.
		 */
		sensors(SENSOR_NAMESPACE, "sensor1", sensorQuery()) {
			@Override
			List<List<KvinTuple>> batches() {
				return generateSensorData(namespace);
			}
		};

		final String namespace;
		final URI sampleItem;
		final String query;

		Theme(String namespace, String sampleItem, String query) {
			this.namespace = namespace;
			this.sampleItem = URIs.createURI(sampleItem.contains(":") ? sampleItem : namespace + sampleItem);
			this.query = query;
		}

		/**
		 * The theme's data in the order it is written; each list is one put.
		 */
		abstract List<List<KvinTuple>> batches();
	}

	// every theme produces roughly 100,000 tuples
	static final int PROCESS_WORK_PIECES = 2000;
	static final int PROCESS_PROPERTIES = 49;
	static final long PROCESS_START_INTERVAL = 90_000;
	static final long PROCESS_SECTION_DURATION = 30 * 60_000;
	static final int MACHINE_PARTS = 34;
	static final int MACHINE_CHANNELS = 6;
	static final int MACHINE_SAMPLES = 250;
	static final long MACHINE_CYCLE = 30_000;
	static final String PROCESS_NAMESPACE = "http://example.org/production/";
	static final String MACHINE_NAMESPACE = "http://example.org/pressFit/";
	static final String SENSOR_NAMESPACE = "http://example.org/plant/";
	static final int SENSORS = 50;
	static final int SENSOR_SAMPLES = 500;
	static final long SENSOR_INTERVAL = 5_000;
	static final int SENSOR_SAMPLES_PER_PUT = 12;
	static final int SENSOR_DASHBOARD_SENSORS = 10;
	static final long SENSOR_DASHBOARD_RANGE = 30 * 60_000;

	// 2023-11-14T22:13:20Z
	static final long START_TIME = 1_700_000_000_000L;
	static final long DAY = 24 * 60 * 60_000L;

	@Param
	public StoreType store;

	@Param
	public Theme theme;

	Path storeDir;
	Kvin kvin;
	SailRepository repository;
	BaseFederatedServiceResolver resolver;
	RepositoryConnection connection;
	TupleQuery query;

	public static void main(String[] args) throws RunnerException, CommandLineOptionException {
		CommandLineOptions cmdOptions = new CommandLineOptions(args);
		OptionsBuilder builder = new OptionsBuilder();
		builder.parent(cmdOptions);
		if (cmdOptions.getIncludes().isEmpty()) {
			builder.include(KvinThemeBenchmark.class.getSimpleName());
		}
		Options opt = builder.build();
		new Runner(opt).run();
	}

	@Setup(Level.Trial)
	public void setup() throws IOException {
		storeDir = Files.createTempDirectory("kvin-" + store + "-" + theme);
		kvin = store.open(storeDir);
		for (List<KvinTuple> batch : theme.batches()) {
			kvin.put(batch);
		}

		repository = new SailRepository(new MemoryStore());
		resolver = new BaseFederatedServiceResolver() {
			@Override
			protected FederatedService createService(String serviceUrl) {
				return new KvinFederatedService(kvin, this::getExecutorService, null, false);
			}
		};
		repository.setFederatedServiceResolver(resolver);
		repository.init();
		connection = repository.getConnection();
		query = connection.prepareTupleQuery(QueryLanguage.SPARQL, theme.query, theme.namespace);

		// fail early instead of measuring queries or fetches that return nothing
		if (evaluateQuery(null) == 0) {
			throw new IllegalStateException("Query of theme " + theme + " returned no results for " + store);
		}
		try (IExtendedIterator<KvinTuple> tuples = kvin.fetch(theme.sampleItem, null, Kvin.DEFAULT_CONTEXT, 0)) {
			if (!tuples.hasNext()) {
				throw new IllegalStateException("Sample item " + theme.sampleItem + " of theme " + theme
						+ " has no values in " + store);
			}
		}
	}

	@TearDown(Level.Trial)
	public void tearDown() throws IOException {
		try {
			connection.close();
			repository.shutDown();
			resolver.shutDown();
			kvin.close();
		} finally {
			FileUtils.deleteDirectory(storeDir.toFile());
		}
	}

	@Benchmark
	public void query(Blackhole blackhole) {
		blackhole.consume(evaluateQuery(blackhole));
	}

	@Benchmark
	public void fetchItem(Blackhole blackhole) {
		try (IExtendedIterator<KvinTuple> tuples = kvin.fetch(theme.sampleItem, null, Kvin.DEFAULT_CONTEXT, 0)) {
			while (tuples.hasNext()) {
				blackhole.consume(tuples.next());
			}
		}
	}

	int evaluateQuery(Blackhole blackhole) {
		int count = 0;
		try (TupleQueryResult result = query.evaluate()) {
			while (result.hasNext()) {
				var bindings = result.next();
				if (blackhole != null) {
					blackhole.consume(bindings);
				}
				count++;
			}
		}
		return count;
	}

	static String machineChannelPatterns() {
		StringBuilder patterns = new StringBuilder();
		for (int c = 1; c <= MACHINE_CHANNELS; c++) {
			String t = "?t" + c;
			patterns.append("    { ?part press:channel").append(c).append("Angle [ <kvin:value> ?a").append(c)
					.append(" ; <kvin:time> ").append(t).append(" ; <kvin:index> ?s ] ; ")
					.append("press:channel").append(c).append("Force [ <kvin:value> ?f").append(c)
					.append(" ; <kvin:time> ").append(t).append(" ; <kvin:index> ?s ] . }\n");
		}
		return patterns.toString();
	}

	static String sensorQuery() {
		long end = START_TIME + (SENSOR_SAMPLES - 1) * SENSOR_INTERVAL;
		StringBuilder sensors = new StringBuilder();
		for (int i = 1; i <= SENSOR_DASHBOARD_SENSORS; i++) {
			sensors.append(" <").append(SENSOR_NAMESPACE).append("sensor").append(i).append(">");
		}
		return "prefix s: <" + SENSOR_NAMESPACE + ">\n" +
				"select ?sensor ?t ?avg {\n" +
				"  values ?sensor {" + sensors + " }\n" +
				"  service <kvin:> {\n" +
				"    ?sensor s:temperature [ <kvin:from> " + (end - SENSOR_DASHBOARD_RANGE) + " ; <kvin:to> " + end +
				" ; <kvin:interval> 60000 ; <kvin:op> \"avg\" ; <kvin:time> ?t ; <kvin:value> ?avg ] .\n" +
				"  }\n" +
				"} order by ?sensor ?t";
	}

	static float round(double value, double resolution) {
		return (float) (Math.round(value / resolution) * resolution);
	}

	/**
	 * Process parameters of a work piece: name, kind, base value, noise, drift per work piece
	 * (tool wear) and sensor resolution.
	 */
	record ProcessParameter(String name, char kind, double base, double noise, double drift, double resolution) {
	}

	static List<ProcessParameter> processParameters() {
		Random random = new Random(100);
		List<ProcessParameter> parameters = new ArrayList<>();
		for (int p = 1; p <= PROCESS_PROPERTIES; p++) {
			parameters.add(switch (p % 7) {
				case 0 -> new ProcessParameter("meltTemperature" + p, 'f', 180 + 40 * random.nextDouble(), 0.4, 0.001, 0.1);
				case 1 -> new ProcessParameter("pressure" + p, 'f', 50 + 30 * random.nextDouble(), 0.2, -0.0005, 0.01);
				case 2 -> new ProcessParameter("spindleSpeed" + p, 'i', 1200 + 1800 * random.nextDouble(), 4, 0, 1);
				case 3 -> new ProcessParameter("torque" + p, 'f', 20 + 40 * random.nextDouble(), 0.3, 0.002, 0.1);
				case 4 -> new ProcessParameter("cycleTime" + p, 'd', 85 + 10 * random.nextDouble(), 0.5, 0, 0.001);
				case 5 -> new ProcessParameter("cycleCount" + p, 'l', 0, 0, 0, 1);
				default -> p % 2 == 0 ? new ProcessParameter("qualityOk" + p, 'b', 0.98, 0, 0, 1)
						: new ProcessParameter("toolId" + p, 's', 0, 0, 0, 1);
			});
		}
		return parameters;
	}

	/**
	 * Generates the item {@code line} that lists all work pieces, the section history of every
	 * work piece and its process parameters, recorded one per second after its start.
	 */
	static List<KvinTuple> generateProcessData(String ns, int workPieces) {
		Random random = new Random(200);
		String[] sections = {"start", "middle", "end"};
		URI ctx = Kvin.DEFAULT_CONTEXT;
		URI line = URIs.createURI(ns + "line");
		URI workPiece = URIs.createURI(ns + "workPiece");
		URI section = URIs.createURI(ns + "section");
		List<ProcessParameter> parameters = processParameters();
		URI[] properties = parameters.stream().map(p -> URIs.createURI(ns + p.name())).toArray(URI[]::new);
		// the data ends shortly after the last work piece started
		long end = START_TIME + (workPieces - 1) * PROCESS_START_INTERVAL + 5 * 60_000;

		List<KvinTuple> tuples = new ArrayList<>(workPieces * (PROCESS_PROPERTIES + 4));
		for (int piece = 0; piece < workPieces; piece++) {
			long start = START_TIME + piece * PROCESS_START_INTERVAL;
			URI item = URIs.createURI(ns + "workPiece" + piece);
			tuples.add(new KvinTuple(line, workPiece, ctx, start, 0, item));
			for (int s = 0; s < sections.length && start + s * PROCESS_SECTION_DURATION <= end; s++) {
				tuples.add(new KvinTuple(item, section, ctx, start + s * PROCESS_SECTION_DURATION, 0, sections[s]));
			}
			for (int p = 0; p < parameters.size(); p++) {
				ProcessParameter parameter = parameters.get(p);
				double value = parameter.base() + parameter.drift() * piece + parameter.noise() * random.nextGaussian();
				Object v = switch (parameter.kind()) {
					case 'f' -> round(value, parameter.resolution());
					case 'd' -> Math.round(value / parameter.resolution()) * parameter.resolution();
					case 'i' -> (int) Math.round(value);
					case 'l' -> (long) piece + 100_000;
					case 'b' -> random.nextDouble() < parameter.base();
					default -> "T-" + (100 + piece / 250);
				};
				tuples.add(new KvinTuple(item, properties[p], ctx, start + (p + 1) * 1000L, 0, v));
			}
		}
		return tuples;
	}

	/**
	 * Generates press-fit curves: per part and channel, the angle rises evenly to its end
	 * angle and the force rises quadratically to its maximum, both with sensor noise and
	 * rounded to the sensor resolution (0.01 degree, 1 N).
	 */
	static List<KvinTuple> generateMachineData(String ns, int parts) {
		Random random = new Random(200);
		URI ctx = Kvin.DEFAULT_CONTEXT;
		URI[] angles = new URI[MACHINE_CHANNELS + 1], forces = new URI[MACHINE_CHANNELS + 1];
		for (int c = 1; c <= MACHINE_CHANNELS; c++) {
			angles[c] = URIs.createURI(ns + "channel" + c + "Angle");
			forces[c] = URIs.createURI(ns + "channel" + c + "Force");
		}
		List<KvinTuple> tuples = new ArrayList<>(parts * MACHINE_CHANNELS * 2 * MACHINE_SAMPLES);
		for (int part = 0; part < parts; part++) {
			long time = START_TIME + part * MACHINE_CYCLE;
			URI item = URIs.createURI(ns + "part" + part);
			for (int c = 1; c <= MACHINE_CHANNELS; c++) {
				double endAngle = 35 + 10 * random.nextDouble();
				double maxForce = 8_000 + 4_000 * random.nextDouble();
				for (int s = 0; s < MACHINE_SAMPLES; s++) {
					double progress = s / (double) (MACHINE_SAMPLES - 1);
					float angle = round(endAngle * progress + 0.02 * random.nextGaussian(), 0.01);
					float force = round(maxForce * progress * progress + 5 * random.nextGaussian(), 1);
					tuples.add(new KvinTuple(item, angles[c], ctx, time, s, angle));
					tuples.add(new KvinTuple(item, forces[c], ctx, time, s, force));
				}
			}
		}
		return tuples;
	}

	/**
	 * Generates sensor readings in time order, one batch per minute. Temperature follows a
	 * daily cycle, pressure varies around a set point, power depends on whether the machine
	 * runs, and the energy counter (Wh) integrates the power.
	 */
	static List<List<KvinTuple>> generateSensorData(String ns) {
		Random random = new Random(300);
		URI ctx = Kvin.DEFAULT_CONTEXT;
		URI temperature = URIs.createURI(ns + "temperature");
		URI pressure = URIs.createURI(ns + "pressure");
		URI power = URIs.createURI(ns + "power");
		URI energy = URIs.createURI(ns + "energy");
		URI[] sensors = new URI[SENSORS];
		double[] baseTemperature = new double[SENSORS], basePressure = new double[SENSORS], nominalPower = new double[SENSORS];
		boolean[] running = new boolean[SENSORS];
		double[] energyWh = new double[SENSORS];
		for (int i = 0; i < SENSORS; i++) {
			sensors[i] = URIs.createURI(ns + "sensor" + (i + 1));
			baseTemperature[i] = 22 + 3 * random.nextGaussian();
			basePressure[i] = 6 + 0.2 * random.nextGaussian();
			nominalPower[i] = 5 + 10 * random.nextDouble();
			running[i] = random.nextDouble() < 0.8;
			energyWh[i] = 1_000_000 * random.nextDouble();
		}
		List<List<KvinTuple>> batches = new ArrayList<>();
		List<KvinTuple> batch = new ArrayList<>();
		for (int sample = 0; sample < SENSOR_SAMPLES; sample++) {
			long time = START_TIME + sample * SENSOR_INTERVAL;
			double daily = Math.sin(2 * Math.PI * (time % DAY) / DAY);
			for (int i = 0; i < SENSORS; i++) {
				// machines switch on or off now and then
				if (random.nextDouble() < 0.002) {
					running[i] = !running[i];
				}
				double kw = running[i] ? nominalPower[i] * (1 + 0.03 * random.nextGaussian()) : 0;
				energyWh[i] += kw * SENSOR_INTERVAL / 3_600.0;
				batch.add(new KvinTuple(sensors[i], temperature, ctx, time, 0,
						round(baseTemperature[i] + 2 * daily + (running[i] ? 1.5 : 0) + 0.05 * random.nextGaussian(), 0.1)));
				batch.add(new KvinTuple(sensors[i], pressure, ctx, time, 0,
						round(basePressure[i] + 0.03 * random.nextGaussian(), 0.01)));
				batch.add(new KvinTuple(sensors[i], power, ctx, time, 0, round(kw, 0.1)));
				batch.add(new KvinTuple(sensors[i], energy, ctx, time, 0, (long) energyWh[i]));
			}
			if ((sample + 1) % SENSOR_SAMPLES_PER_PUT == 0) {
				batches.add(batch);
				batch = new ArrayList<>();
			}
		}
		if (!batch.isEmpty()) {
			batches.add(batch);
		}
		return batches;
	}
}
