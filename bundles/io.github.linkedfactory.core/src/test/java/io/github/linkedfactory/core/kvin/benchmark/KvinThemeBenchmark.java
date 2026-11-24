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
 * single-item fetch are measured against it.
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
		 * Many work pieces with few values each. The query finds the work pieces whose latest
		 * section is "anfang" and returns all their properties.
		 */
		process("http://dm.adaproq.de/vocab/", "wp1995",
				"prefix aq: <http://dm.adaproq.de/vocab/>\n" +
						"select * {\n" +
						"  service <kvin:> {\n" +
						"    { select distinct ?wp { aq:gtc aq:workpiece [ <kvin:value> ?wp ] . } }\n" +
						"    ?wp aq:abschnitt [ <kvin:value> \"anfang\" ; <kvin:limit> 1 ] .\n" +
						"    ?wp ?property [ <kvin:value> ?v ; <kvin:time> ?t ] .\n" +
						"  }\n" +
						"}") {
			@Override
			List<KvinTuple> generate() {
				return generateProcessData(namespace, PROCESS_WORK_PIECES);
			}
		},
		/**
		 * Few parts with long series per channel. The query joins angle and force of six
		 * channels by time and index for six parts.
		 */
		machine("http://dm.adaproq.de/datamodel/ESW-M-Schraube_ABC#ESW-M-Presskraefte_1.", "part:6",
				"prefix esw: <http://dm.adaproq.de/datamodel/ESW-M-Schraube_ABC#ESW-M-Presskraefte_1.>\n" +
						"select * {\n" +
						"  values ?part { <part:6> <part:7> <part:8> <part:9> <part:10> <part:11> }\n" +
						"  service <kvin:> {\n" + machineChannelPatterns() +
						"  }\n" +
						"} order by ?part ?s") {
			@Override
			List<KvinTuple> generate() {
				return generateMachineData(namespace, MACHINE_PARTS);
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

		abstract List<KvinTuple> generate();
	}

	// both themes produce roughly 100,000 tuples
	static final int PROCESS_WORK_PIECES = 2000;
	static final int PROCESS_PROPERTIES = 50;
	static final int MACHINE_PARTS = 34;
	static final int MACHINE_CHANNELS = 6;
	static final int MACHINE_SAMPLES = 250;

	static final long START_TIME = 1653335520L;
	static final long WEEK = 604800L;

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
		kvin.put(theme.generate());

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

		// fail early instead of measuring queries that return nothing
		if (evaluateQuery(null) == 0) {
			throw new IllegalStateException("Query of theme " + theme + " returned no results for " + store);
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
			patterns.append("    { ?part esw:Channel").append(c).append(".Angle [ <kvin:value> ?a").append(c)
					.append(" ; <kvin:time> ").append(t).append(" ; <kvin:index> ?s ] ; ")
					.append("esw:Channel").append(c).append(".Force [ <kvin:value> ?f").append(c)
					.append(" ; <kvin:time> ").append(t).append(" ; <kvin:index> ?s ] . }\n");
		}
		return patterns.toString();
	}

	/**
	 * Generates the item {@code gtc} that lists all work pieces, and for every work piece a
	 * section value plus numeric properties. Time advances by one week every 100 work pieces.
	 */
	static List<KvinTuple> generateProcessData(String ns, int workPieces) {
		Random random = new Random(200);
		String[] sections = {"anfang", "mitte", "end"};
		URI ctx = Kvin.DEFAULT_CONTEXT;
		URI gtc = URIs.createURI(ns + "gtc");
		URI workpiece = URIs.createURI(ns + "workpiece");
		URI section = URIs.createURI(ns + "abschnitt");
		URI[] properties = new URI[PROCESS_PROPERTIES];
		for (int p = 1; p < PROCESS_PROPERTIES; p++) {
			properties[p] = URIs.createURI(ns + "property" + p);
		}

		List<KvinTuple> tuples = new ArrayList<>(workPieces * (PROCESS_PROPERTIES + 1));
		for (int wp = 0; wp < workPieces; wp++) {
			tuples.add(new KvinTuple(gtc, workpiece, ctx, START_TIME, wp, URIs.createURI(ns + "wp" + wp)));
		}
		long time = START_TIME;
		for (int wp = 0; wp < workPieces; wp++) {
			if (wp > 0 && wp % 100 == 0) {
				time += WEEK;
			}
			URI item = URIs.createURI(ns + "wp" + wp);
			tuples.add(new KvinTuple(item, section, ctx, time, 0, sections[random.nextInt(sections.length)]));
			for (int p = 1; p < PROCESS_PROPERTIES; p++) {
				tuples.add(new KvinTuple(item, properties[p], ctx, time, 0, random.nextFloat()));
			}
		}
		return tuples;
	}

	/**
	 * Generates parts with an angle and a force series per channel. All samples of a part share
	 * one time stamp and are distinguished by their sequence number. Time advances by one week
	 * every five parts.
	 */
	static List<KvinTuple> generateMachineData(String ns, int parts) {
		Random random = new Random(200);
		URI ctx = Kvin.DEFAULT_CONTEXT;
		List<KvinTuple> tuples = new ArrayList<>(parts * MACHINE_CHANNELS * 2 * MACHINE_SAMPLES);
		long time = START_TIME;
		for (int part = 0; part < parts; part++) {
			if (part > 0 && part % 5 == 0) {
				time += WEEK;
			}
			URI item = URIs.createURI("part:" + part);
			int seq = 0;
			for (int c = 1; c <= MACHINE_CHANNELS; c++) {
				for (String kind : new String[]{".Angle", ".Force"}) {
					URI property = URIs.createURI(ns + "Channel" + c + kind);
					for (int s = 0; s < MACHINE_SAMPLES; s++) {
						tuples.add(new KvinTuple(item, property, ctx, time, seq++, random.nextFloat()));
					}
				}
			}
		}
		return tuples;
	}
}
