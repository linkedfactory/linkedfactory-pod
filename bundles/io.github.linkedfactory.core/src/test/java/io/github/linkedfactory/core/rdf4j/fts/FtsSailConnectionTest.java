package io.github.linkedfactory.core.rdf4j.fts;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FtsSailConnectionTest {
	private final SimpleValueFactory vf = SimpleValueFactory.getInstance();

	@Test
	public void commitPushesOnlyLiteralChanges() {
		RecordingSearchService service = new RecordingSearchService();
		SailRepository repository = createRepository(service);
		try (SailRepositoryConnection connection = repository.getConnection()) {
			Statement addLiteral = vf.createStatement(
					vf.createIRI("urn:s1"),
					vf.createIRI("urn:p1"),
					vf.createLiteral("value"),
					vf.createIRI("urn:context"));
			Statement removeLiteral = vf.createStatement(
					vf.createIRI("urn:s2"),
					vf.createIRI("urn:p2"),
					vf.createLiteral("removed"),
					vf.createIRI("urn:context"));

			connection.add(removeLiteral);
			connection.commit();

			connection.begin();
			connection.add(addLiteral);
			connection.remove(removeLiteral);
			connection.add(vf.createIRI("urn:s3"),
					vf.createIRI("urn:p3"),
					vf.createBNode(),
					vf.createIRI("urn:context"));
			connection.commit();

			assertEquals(2, service.addRemoveCount);
			assertTrue(service.addedStatements.contains(removeLiteral));
			assertTrue(service.addedStatements.contains(addLiteral));
			assertEquals(1, service.removedStatements.size());
			assertTrue(service.removedStatements.contains(removeLiteral));
		} finally {
			repository.shutDown();
		}
	}

	@Test
	public void excludedModelContextsAreSkipped() {
		RecordingSearchService service = new RecordingSearchService();
		SailRepository repository = createRepository(service, Set.of("urn:model:excluded"));
		try (SailRepositoryConnection connection = repository.getConnection()) {
			Statement excluded = vf.createStatement(
					vf.createIRI("urn:s1"),
					vf.createIRI("urn:p1"),
					vf.createLiteral("skip me"),
					vf.createIRI("urn:model:excluded"));
			Statement included = vf.createStatement(
					vf.createIRI("urn:s2"),
					vf.createIRI("urn:p1"),
					vf.createLiteral("keep me"),
					vf.createIRI("urn:model:included"));

			connection.begin();
			connection.add(excluded);
			connection.add(included);
			connection.commit();

			assertEquals(1, service.addedStatements.size());
			assertTrue(service.addedStatements.contains(included));
		} finally {
			repository.shutDown();
		}
	}

	@Test
	public void clearDropsPreviouslyBufferedOperations() {
		RecordingSearchService service = new RecordingSearchService();
		SailRepository repository = createRepository(service);
		try (SailRepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			connection.add(vf.createIRI("urn:s1"), vf.createIRI("urn:p1"), vf.createLiteral("value"));
			connection.clear();
			connection.commit();

			assertEquals(1, service.clearCount);
			assertEquals(0, service.addRemoveCount);
			assertTrue(service.addedStatements.isEmpty());
			assertTrue(service.removedStatements.isEmpty());
		} finally {
			repository.shutDown();
		}
	}

	@Test
	public void addAndRemoveSameStatementCancelOut() {
		RecordingSearchService service = new RecordingSearchService();
		SailRepository repository = createRepository(service);
		try (SailRepositoryConnection connection = repository.getConnection()) {
			Statement stmt = vf.createStatement(
					vf.createIRI("urn:s1"),
					vf.createIRI("urn:p1"),
					vf.createLiteral("value"));

			connection.begin();
			connection.add(stmt);
			connection.remove(stmt);
			connection.commit();

			assertEquals(0, service.addRemoveCount);
			assertTrue(service.addedStatements.isEmpty());
			assertTrue(service.removedStatements.isEmpty());
		} finally {
			repository.shutDown();
		}
	}

	@Test
	public void largeTransactionSpillsAndCommitsAllStatements() {
		RecordingSearchService service = new RecordingSearchService();
		FtsSail sail = new FtsSail(service);
		sail.setBaseSail(new MemoryStore());
		SailRepository repository = new SailRepository(sail);
		repository.init();
		try (SailRepositoryConnection connection = repository.getConnection()) {
			FtsSailConnection ftsConnection = (FtsSailConnection) connection.getSailConnection();

			for (int i = 0; i < 10; i++) {
				ftsConnection.begin();
				ftsConnection.addStatement(vf.createIRI("urn:s" + i),
						vf.createIRI("urn:p"),
						vf.createLiteral("value" + i),
						vf.createIRI("urn:context"));
				ftsConnection.commit();
			}

			assertTrue(service.addRemoveCount >= 10);
			assertEquals(10, service.addedStatements.size());
		} finally {
			repository.shutDown();
		}
	}

	@Test
	public void rollbackDiscardsPendingChanges() {
		RecordingSearchService service = new RecordingSearchService();
		SailRepository repository = createRepository(service);
		try (SailRepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			connection.add(vf.createIRI("urn:s1"), vf.createIRI("urn:p1"), vf.createLiteral("x"));
			connection.rollback();

			assertEquals(0, service.commitCount);
			assertEquals(1, service.rollbackCount);
			assertTrue(service.addedStatements.isEmpty());
		} finally {
			repository.shutDown();
		}
	}

	@Test
	public void clearAddsClearContextOperation() {
		RecordingSearchService service = new RecordingSearchService();
		SailRepository repository = createRepository(service);
		try (SailRepositoryConnection connection = repository.getConnection()) {
			IRI ctx = vf.createIRI("urn:ctx");
			connection.begin();
			connection.clear(ctx);
			connection.commit();

			assertEquals(1, service.clearedContexts.size());
			assertEquals(ctx, service.clearedContexts.get(0)[0]);
		} finally {
			repository.shutDown();
		}
	}

	@Test
	public void clearThenReAddKeepsOnlyLatestChanges() {
		RecordingSearchService service = new RecordingSearchService();
		SailRepository repository = createRepository(service);
		try (SailRepositoryConnection connection = repository.getConnection()) {
			Statement stmt = vf.createStatement(
					vf.createIRI("urn:s1"),
					vf.createIRI("urn:p1"),
					vf.createLiteral("value"),
					vf.createIRI("urn:context"));

			connection.begin();
			connection.add(stmt);
			connection.clear();
			connection.add(stmt);
			connection.commit();

			assertEquals(1, service.clearCount);
			assertEquals(1, service.addRemoveCount);
			assertTrue(service.addedStatements.contains(stmt));
			assertTrue(service.removedStatements.isEmpty());
		} finally {
			repository.shutDown();
		}
	}

	@Test
	public void rollbackAfterMultipleWritesDiscardsBufferedChanges() {
		RecordingSearchService service = new RecordingSearchService();
		SailRepository repository = createRepository(service);
		try (SailRepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			for (int i = 0; i < 10; i++) {
				connection.add(vf.createIRI("urn:s" + i), vf.createIRI("urn:p"), vf.createLiteral("value" + i));
			}
			connection.rollback();

			assertEquals(0, service.addRemoveCount);
			assertEquals(1, service.rollbackCount);
			assertTrue(service.addedStatements.isEmpty());
		} finally {
			repository.shutDown();
		}
	}

	@Test
	public void staleSpillFilesAreCleanedOnStartup() throws Exception {
		Path stale = Files.createTempFile("fts-sail-buffer-", ".bin");
		Files.setLastModifiedTime(stale, FileTime.fromMillis(System.currentTimeMillis() - 2 * 60 * 60 * 1000L));

		RecordingSearchService service = new RecordingSearchService();
		SailRepository repository = createRepository(service);
		try {
			try (SailRepositoryConnection connection = repository.getConnection()) {
				connection.begin();
				connection.rollback();
			}
			assertTrue(Files.notExists(stale));
		} finally {
			repository.shutDown();
		}
	}

	private SailRepository createRepository(RecordingSearchService service) {
		return createRepository(service, Set.of());
	}

	private SailRepository createRepository(RecordingSearchService service, Set<String> excludedModels) {
		FtsSail sail = new FtsSail(service, FtsFederatedServiceConfig.defaults(), excludedModels);
		sail.setBaseSail(new MemoryStore());
		SailRepository repository = new SailRepository(sail);
		repository.init();
		return repository;
	}

	private static final class RecordingSearchService implements FtsSearchService {
		private final Set<Statement> addedStatements = new LinkedHashSet<>();
		private final Set<Statement> removedStatements = new LinkedHashSet<>();
		private final List<org.eclipse.rdf4j.model.Resource[]> clearedContexts = new ArrayList<>();
		private int addRemoveCount;
		private int clearCount;
		private int commitCount;
		private int rollbackCount;

		@Override
		public void addRemoveStatements(Set<Statement> added, Set<Statement> removed) {
			addRemoveCount++;
			addedStatements.addAll(added);
			removedStatements.addAll(removed);
		}

		@Override
		public void clearContexts(org.eclipse.rdf4j.model.Resource... contexts) {
			clearCount++;
			clearedContexts.add(contexts);
		}

		@Override
		public void clear() {
			clearCount++;
		}

		@Override
		public void commit() {
			commitCount++;
		}

		@Override
		public void rollback() {
			rollbackCount++;
		}
	}
}
