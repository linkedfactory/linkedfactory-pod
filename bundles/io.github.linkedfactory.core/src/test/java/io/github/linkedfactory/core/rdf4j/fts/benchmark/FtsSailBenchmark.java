package io.github.linkedfactory.core.rdf4j.fts.benchmark;

import io.github.linkedfactory.core.rdf4j.fts.FtsSail;
import io.github.linkedfactory.core.rdf4j.fts.FtsSearchService;
import io.github.linkedfactory.core.rdf4j.fts.FtsFederatedServiceConfig;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(2)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class FtsSailBenchmark {
	@Param({ "1000", "10000" })
	int batchSize;

	private final ValueFactory vf = SimpleValueFactory.getInstance();
	private SailRepository repository;
	private SailRepositoryConnection connection;
	private List<Statement> statements;
	private BenchmarkSearchService searchService;
	private Path outbox;

	@Setup(Level.Trial)
	public void setup() throws Exception {
		searchService = new BenchmarkSearchService();
		Files.createDirectories(Path.of("target"));
		outbox = Files.createTempDirectory(Path.of("target").toAbsolutePath(), "fts-benchmark-outbox-");
		FtsSail sail = new FtsSail(searchService, outbox,
				"fts-benchmark-" + UUID.randomUUID(), FtsFederatedServiceConfig.defaults(), Set.of());
		sail.setBaseSail(new MemoryStore());
		repository = new SailRepository(sail);
		repository.init();
		connection = repository.getConnection();
		statements = new ArrayList<>(batchSize);
		for (int i = 0; i < batchSize; i++) {
			IRI subject = vf.createIRI("urn:item:" + i);
			IRI predicate = vf.createIRI("urn:prop:" + (i % 16));
			statements.add(vf.createStatement(subject, predicate, vf.createLiteral("value-" + i)));
		}
	}

	@TearDown(Level.Trial)
	public void tearDown() {
		connection.close();
		repository.shutDown();
		try (var files = Files.walk(outbox)) {
			for (Path path : files.sorted(java.util.Comparator.reverseOrder()).toList()) {
				Files.deleteIfExists(path);
			}
		} catch (Exception e) {
			throw new RuntimeException("Unable to clean benchmark FTS outbox", e);
		}
	}

	@Benchmark
	public void clearAndReplace(Blackhole blackhole) {
		connection.begin();
		connection.clear();
		addAll();
		connection.commit();
		blackhole.consume(searchService.checksum());
	}

	@Benchmark
	public void addRemoveChurn(Blackhole blackhole) {
		connection.begin();
		connection.clear();
		addAll();
		for (int i = 0; i < statements.size(); i += 2) {
			Statement statement = statements.get(i);
			connection.remove(statement.getSubject(), statement.getPredicate(), statement.getObject());
		}
		connection.commit();
		blackhole.consume(searchService.checksum());
	}

	@Benchmark
	public void clearOnly(Blackhole blackhole) {
		connection.begin();
		connection.clear();
		connection.commit();
		blackhole.consume(searchService.checksum());
	}

	private void addAll() {
		for (Statement statement : statements) {
			connection.add(statement.getSubject(), statement.getPredicate(), statement.getObject());
		}
	}

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder()
				.include(FtsSailBenchmark.class.getSimpleName())
				.build();
		new Runner(opt).run();
	}

	private static final class BenchmarkSearchService implements FtsSearchService {
		private long checksum;
		private long completedUpdates;
		private long observedUpdates;

		@Override
		public void prepareCommit(String transactionId) {
		}

		@Override
		public void addRemoveStatements(Set<Statement> added, Set<Statement> removed) {
			checksum += added.size() * 31L + removed.size();
		}

		@Override
		public void clearContexts(org.eclipse.rdf4j.model.Resource... contexts) {
			checksum += contexts == null ? 0 : contexts.length;
		}

		@Override
		public void clear() {
			checksum++;
		}

		@Override
		public synchronized void commit() {
			checksum++;
			completedUpdates++;
			notifyAll();
		}

		synchronized long checksum() {
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
			while (completedUpdates <= observedUpdates && System.nanoTime() < deadline) {
				try {
					TimeUnit.NANOSECONDS.timedWait(this, deadline - System.nanoTime());
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new IllegalStateException("Interrupted waiting for benchmark FTS update", e);
				}
			}
			if (completedUpdates <= observedUpdates) {
				throw new IllegalStateException("Timed out waiting for benchmark FTS update");
			}
			observedUpdates++;
			return checksum;
		}
	}
}
