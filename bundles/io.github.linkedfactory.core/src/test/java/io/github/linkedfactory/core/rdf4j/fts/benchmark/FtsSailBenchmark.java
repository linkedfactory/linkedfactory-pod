package io.github.linkedfactory.core.rdf4j.fts.benchmark;

import io.github.linkedfactory.core.rdf4j.fts.FtsSail;
import io.github.linkedfactory.core.rdf4j.fts.FtsSearchService;
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

	@Setup(Level.Trial)
	public void setup() {
		searchService = new BenchmarkSearchService();
		repository = new SailRepository(new FtsSail(searchService, new MemoryStore()));
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
		public void commit() {
			checksum++;
		}

		long checksum() {
			return checksum;
		}
	}
}
