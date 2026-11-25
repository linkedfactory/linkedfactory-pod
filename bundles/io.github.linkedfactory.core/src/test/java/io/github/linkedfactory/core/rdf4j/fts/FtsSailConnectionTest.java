package io.github.linkedfactory.core.rdf4j.fts;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertSame;

public class FtsSailConnectionTest {
	private final SimpleValueFactory vf = SimpleValueFactory.getInstance();
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

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

			service.awaitCommits(2);
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

			service.awaitCommits(1);
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

			service.awaitCommits(1);
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
		FtsSail sail = createSail(service, Set.of());
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

			service.awaitCommits(10);
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
			assertTrue(service.addedStatements.isEmpty());
		} finally {
			repository.shutDown();
		}
	}

	@Test
	public void sparqlInsertAndDeleteDataAreIndexed() {
		RecordingSearchService service = new RecordingSearchService();
		SailRepository repository = createRepository(service);
		try (SailRepositoryConnection connection = repository.getConnection()) {
			IRI context = vf.createIRI("urn:sparql-context");
			Statement statement = vf.createStatement(vf.createIRI("urn:sparql-subject"),
					vf.createIRI("urn:sparql-predicate"), vf.createLiteral("SPARQL value"), context);

			connection.begin();
			connection.prepareUpdate(QueryLanguage.SPARQL,
					"INSERT DATA { GRAPH <urn:sparql-context> { "
							+ "<urn:sparql-subject> <urn:sparql-predicate> \"SPARQL value\" } }").execute();
			connection.commit();
			service.awaitCommits(1);
			assertTrue(service.addedStatements.contains(statement));

			connection.begin();
			connection.prepareUpdate(QueryLanguage.SPARQL,
					"DELETE DATA { GRAPH <urn:sparql-context> { "
							+ "<urn:sparql-subject> <urn:sparql-predicate> \"SPARQL value\" } }").execute();
			connection.commit();
			service.awaitCommits(2);
			assertTrue(service.removedStatements.contains(statement));
		} finally {
			repository.shutDown();
		}
	}

	@Test
	public void sparqlDefaultGraphInsertIsIndexed() {
		RecordingSearchService service = new RecordingSearchService();
		SailRepository repository = createRepository(service);
		try (SailRepositoryConnection connection = repository.getConnection()) {
			Statement statement = vf.createStatement(vf.createIRI("urn:default-subject"),
					vf.createIRI("urn:default-predicate"), vf.createLiteral("default graph value"));

			connection.begin();
			connection.prepareUpdate(QueryLanguage.SPARQL,
					"INSERT DATA { <urn:default-subject> <urn:default-predicate> \"default graph value\" }")
					.execute();
			connection.commit();

			service.awaitCommits(1);
			assertTrue(service.addedStatements.contains(statement));

			connection.begin();
			connection.prepareUpdate(QueryLanguage.SPARQL,
					"DELETE DATA { <urn:default-subject> <urn:default-predicate> \"default graph value\" }")
					.execute();
			connection.commit();
			service.awaitCommits(2);
			assertTrue(service.removedStatements.contains(statement));
		} finally {
			repository.shutDown();
		}
	}

	@Test
	public void sparqlUpdateRollbackDiscardsBufferedChanges() {
		RecordingSearchService service = new RecordingSearchService();
		SailRepository repository = createRepository(service);
		try (SailRepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			connection.prepareUpdate(QueryLanguage.SPARQL,
					"INSERT DATA { GRAPH <urn:rollback-context> { "
							+ "<urn:rollback-subject> <urn:rollback-predicate> \"discarded\" } }").execute();
			connection.rollback();

			assertEquals(0, service.commitCount);
			assertTrue(service.addedStatements.isEmpty());
		} finally {
			repository.shutDown();
		}
	}

	@Test
	public void updateMutationFailurePoisonsTransaction() throws Exception {
		MemoryStore store = new MemoryStore();
		store.init();
		NotifyingSailConnection delegate = (NotifyingSailConnection) store.getConnection();
		NotifyingSailConnection failingConnection = (NotifyingSailConnection) Proxy.newProxyInstance(
				NotifyingSailConnection.class.getClassLoader(),
				new Class<?>[] { NotifyingSailConnection.class },
				(proxy, method, args) -> {
					if (method.getName().equals("addStatement") && method.getParameterCount() == 5) {
						throw new SailException("Simulated SPARQL mutation failure");
					}
					try {
						return method.invoke(delegate, args);
					} catch (InvocationTargetException e) {
						throw e.getCause();
					}
				});
		try (FtsSailConnection connection = new FtsSailConnection(
				failingConnection, new FailingPrepareCoordinator(), Set.of())) {
			connection.begin();
			try {
				connection.addStatement(null, vf.createIRI("urn:failed-subject"),
						vf.createIRI("urn:failed-predicate"), vf.createLiteral("failed"),
						vf.createIRI("urn:failed-context"));
				fail("Expected the update mutation to fail");
			} catch (SailException expected) {
				assertEquals("Simulated SPARQL mutation failure", expected.getMessage());
			}

			try {
				connection.commit();
				fail("A transaction with a failed mutation must not commit");
			} catch (SailException expected) {
				assertTrue(expected.getMessage().contains("cannot commit after a failed mutation"));
			}
		} finally {
			store.shutDown();
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

			service.awaitCommits(1);
			assertEquals(1, service.clearedContexts.size());
			assertEquals(ctx, service.clearedContexts.get(0)[0]);
		} finally {
			repository.shutDown();
		}
	}

	@Test
	public void clearContextPreservesEarlierUpdatesInOtherContexts() {
		RecordingSearchService service = new RecordingSearchService();
		SailRepository repository = createRepository(service);
		Statement otherContext = vf.createStatement(vf.createIRI("urn:s1"), vf.createIRI("urn:p1"),
				vf.createLiteral("keep"), vf.createIRI("urn:other"));
		try (SailRepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			connection.add(otherContext);
			connection.clear(vf.createIRI("urn:cleared"));
			connection.commit();
			service.awaitCommits(1);
			assertTrue(service.addedStatements.contains(otherContext));
			assertEquals(1, service.clearCount);
		} finally {
			repository.shutDown();
		}
	}

	@Test
	public void spillChunksAreAppliedInTransactionOrder() throws Exception {
		RecordingSearchService service = new RecordingSearchService();
		MemoryStore store = new MemoryStore();
		store.init();
		FileFtsIndexUpdateCoordinator coordinator = createCoordinator(service);
		try (FtsSailConnection connection = new FtsSailConnection(
				(NotifyingSailConnection) store.getConnection(), coordinator, Set.of(), 2)) {
			IRI context = vf.createIRI("urn:context");
			Statement first = vf.createStatement(vf.createIRI("urn:first"), vf.createIRI("urn:p"),
					vf.createLiteral("first"), context);
			Statement second = vf.createStatement(vf.createIRI("urn:second"), vf.createIRI("urn:p"),
					vf.createLiteral("second"), context);
			connection.begin();
			connection.addStatement(first.getSubject(), first.getPredicate(), first.getObject(), context);
			connection.addStatement(second.getSubject(), second.getPredicate(), second.getObject(), context);
			connection.removeStatements(first.getSubject(), first.getPredicate(), first.getObject(), context);
			connection.commit();
			service.awaitCommits(1);
			assertEquals(List.of("add:2", "remove:1"), service.operationOrder);
		} finally {
			coordinator.close();
			store.shutDown();
		}
	}

	@Test
	public void wildcardRemovalSpillsWithoutCollectingAllStatementsInMemory() throws Exception {
		MemoryStore store = new MemoryStore();
		store.init();
		IRI context = vf.createIRI("urn:context");
		try (var base = store.getConnection()) {
			base.begin();
			for (int i = 0; i < 7; i++) {
				base.addStatement(vf.createIRI("urn:s" + i), vf.createIRI("urn:p"),
						vf.createLiteral("value" + i), context);
			}
			base.commit();
		}

		RecordingSearchService service = new RecordingSearchService();
		FileFtsIndexUpdateCoordinator coordinator = createCoordinator(service);
		try (FtsSailConnection connection = new FtsSailConnection(
				(NotifyingSailConnection) store.getConnection(), coordinator, Set.of(), 2)) {
			connection.begin();
			connection.removeStatements(null, null, null, context);
			connection.commit();
			service.awaitCommits(1);
			assertEquals(7, service.removedStatements.size());
			assertEquals(4, service.addRemoveCount);
		} finally {
			coordinator.close();
			store.shutDown();
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
			service.awaitCommits(1);

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
			assertTrue(service.addedStatements.isEmpty());
		} finally {
			repository.shutDown();
		}
	}

	@Test
	public void prepareFailureRollsBackRdfTransaction() throws Exception {
		MemoryStore store = new MemoryStore();
		store.init();
		try (FtsSailConnection connection = new FtsSailConnection(
				(NotifyingSailConnection) store.getConnection(), new FailingPrepareCoordinator(), Set.of())) {
			Statement statement = vf.createStatement(
					vf.createIRI("urn:s"),
					vf.createIRI("urn:p"),
					vf.createLiteral("value"),
					vf.createIRI("urn:context"));
			connection.begin();
			connection.addStatement(statement.getSubject(), statement.getPredicate(), statement.getObject(),
					statement.getContext());
			try {
				connection.commit();
				fail("Expected prepare failure");
			} catch (org.eclipse.rdf4j.sail.SailException expected) {
				assertTrue(expected.getMessage().contains("Unable to persist prepared transaction"));
			}

			try (var statements = connection.getStatements(null, null, null, false)) {
				assertTrue(!statements.hasNext());
			}
		} finally {
			store.shutDown();
		}
	}

	@Test
	public void journalMarkersAreHiddenFromRdfQueries() throws Exception {
		CountDownLatch applyEntered = new CountDownLatch(1);
		FtsSearchService service = new RecordingSearchService() {
			@Override
			public void apply(FtsIndexUpdate update) throws FtsIndexUpdateException {
				applyEntered.countDown();
				throw new FtsIndexUpdateException("Simulated temporary backend failure");
			}
		};
		SailRepository repository = createRepository(service);
		try (SailRepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			connection.add(vf.createIRI("urn:s"), vf.createIRI("urn:p"), vf.createLiteral("value"),
					vf.createIRI("urn:context"));
			connection.commit();
			assertTrue(applyEntered.await(10, TimeUnit.SECONDS));

			try (var statements = connection.getStatements(null, null, null, false)) {
				assertTrue(statements.hasNext());
				assertEquals(vf.createLiteral("value"), statements.next().getObject());
				assertFalse(statements.hasNext());
			}
			assertEquals(1, connection.getSailConnection().size());
			try (var contexts = connection.getSailConnection().getContextIDs()) {
				while (contexts.hasNext()) {
					assertFalse(FtsSailConnection.JOURNAL_CONTEXT.equals(contexts.next()));
				}
			}
		} finally {
			repository.shutDown();
		}
	}

	@Test
	public void activationFailureAfterRdfCommitRemainsRecoverable() throws Exception {
		Path outbox = temporaryFolder.newFolder().toPath();
		RecordingSearchService service = new RecordingSearchService();
		AtomicBoolean failActivation = new AtomicBoolean(true);
		FtsIndexUpdateCoordinatorFactory coordinatorFactory = (directory, identity, strategy, cleaner) ->
				new FailOnceActivationCoordinator(new FileFtsIndexUpdateCoordinator(
						directory, identity, strategy, cleaner), failActivation);
		FtsSail sail = new FtsSail(service, outbox, UUID.randomUUID().toString(),
				FtsFederatedServiceConfig.defaults(), Set.of(), coordinatorFactory);
		sail.setBaseSail(new MemoryStore());
		sail.init();

		Statement statement = vf.createStatement(vf.createIRI("urn:activation-failure"),
				vf.createIRI("urn:p"), vf.createLiteral("committed"), vf.createIRI("urn:context"));
		FtsSailConnection connection = (FtsSailConnection) sail.getConnection();
		try {
			connection.begin();
			connection.addStatement(statement.getSubject(), statement.getPredicate(),
					statement.getObject(), statement.getContext());
			try {
				connection.commit();
				fail("Expected FTS activation failure after the RDF commit");
			} catch (org.eclipse.rdf4j.sail.SailException expected) {
				assertTrue(expected.getMessage().contains("RDF transaction committed successfully"));
			}

			String transactionId;
			try (var baseConnection = sail.getBaseSail().getConnection()) {
				assertTrue(baseConnection.hasStatement(statement.getSubject(), statement.getPredicate(),
						statement.getObject(), false, statement.getContext()));
				try (var markers = baseConnection.getStatements(null,
						FtsSailConnection.JOURNAL_PREDICATE, null, false,
						FtsSailConnection.JOURNAL_CONTEXT)) {
					assertTrue(markers.hasNext());
					transactionId = ((org.eclipse.rdf4j.model.Literal) markers.next().getObject()).getLabel();
					assertFalse(markers.hasNext());
				}
			}
			assertTrue(Files.exists(outbox.resolve(transactionId + ".prepared")));

			try {
				connection.begin();
				fail("Expected the connection to reject reuse before recovery");
			} catch (org.eclipse.rdf4j.sail.SailException expected) {
				assertTrue(expected.getMessage().contains("requires recovery"));
			}
		} finally {
			connection.close();
		}

		try {
			sail.recoverFtsTransactions();
			service.awaitCommits(1);
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
			while (sail.getOutboxStatus().recordCount() != 0 && System.nanoTime() < deadline) {
				Thread.sleep(10);
			}
			assertEquals(0, sail.getOutboxStatus().recordCount());
			try (var baseConnection = sail.getBaseSail().getConnection()) {
				assertTrue(baseConnection.hasStatement(statement.getSubject(), statement.getPredicate(),
						statement.getObject(), false, statement.getContext()));
				assertFalse(baseConnection.hasStatement(null, FtsSailConnection.JOURNAL_PREDICATE,
						null, false, FtsSailConnection.JOURNAL_CONTEXT));
			}
		} finally {
			sail.shutDown();
		}
	}

	@Test
	public void staleSpillFilesAreNotDeletedWithoutTransactionOwnership() throws Exception {
		Path stale = Files.createTempFile("fts-sail-buffer-", ".bin");
		Files.setLastModifiedTime(stale, FileTime.fromMillis(System.currentTimeMillis() - 2 * 60 * 60 * 1000L));

		RecordingSearchService service = new RecordingSearchService();
		SailRepository repository = createRepository(service);
		try {
			try (SailRepositoryConnection connection = repository.getConnection()) {
				connection.begin();
				connection.rollback();
			}
			assertTrue("Startup must not delete another transaction's spill file", Files.exists(stale));
		} finally {
			repository.shutDown();
			Files.deleteIfExists(stale);
		}
	}

	@Test
	public void activationFailureRemainsPrimaryWhenCommitLockCleanupFails() throws Exception {
		MemoryStore store = new MemoryStore();
		store.init();

		ActivationAndLockCleanupFailingCoordinator coordinator =
				new ActivationAndLockCleanupFailingCoordinator();

		try (FtsSailConnection connection = new FtsSailConnection(
				(NotifyingSailConnection) store.getConnection(), coordinator, Set.of())) {

			Statement statement = vf.createStatement(
					vf.createIRI("urn:s"),
					vf.createIRI("urn:p"),
					vf.createLiteral("value"),
					vf.createIRI("urn:context"));

			connection.begin();
			connection.addStatement(
					statement.getSubject(),
					statement.getPredicate(),
					statement.getObject(),
					statement.getContext());

			try {
				connection.commit();
				fail("Expected activation failure");
			} catch (org.eclipse.rdf4j.sail.SailException expected) {
				// The activation failure must remain the primary exception.
				assertTrue(expected.getMessage().contains(
						"RDF transaction committed successfully"));

				assertSame(
						"The activation exception must remain the direct cause",
						coordinator.activationFailure,
						expected.getCause());

				// The commit-lock cleanup failure must be suppressed on the
				// original activation exception, not replace it.
				assertEquals(1, coordinator.activationFailure.getSuppressed().length);
				assertSame(
						coordinator.cleanupFailure,
						coordinator.activationFailure.getSuppressed()[0]);
			}

			// The RDF commit succeeded despite the FTS activation failure.
			try (var statements = connection.getStatements(
					statement.getSubject(),
					statement.getPredicate(),
					statement.getObject(),
					false,
					statement.getContext())) {
				assertTrue(statements.hasNext());
			}
		} finally {
			store.shutDown();
		}
	}

	@Test
	public void successfulRdfCommitIsExplicitWhenCommitLockCleanupFails() throws Exception {
		MemoryStore store = new MemoryStore();
		store.init();
		IOException cleanupFailure = new IOException("Simulated commit-lock cleanup failure");
		try (FtsSailConnection connection = new FtsSailConnection(
				(NotifyingSailConnection) store.getConnection(),
				new SuccessfulActivationCleanupFailingCoordinator(cleanupFailure), Set.of())) {
			Statement statement = vf.createStatement(vf.createIRI("urn:cleanup"),
					vf.createIRI("urn:p"), vf.createLiteral("committed"), vf.createIRI("urn:context"));
			connection.begin();
			connection.addStatement(statement.getSubject(), statement.getPredicate(),
					statement.getObject(), statement.getContext());

			try {
				connection.commit();
				fail("Expected commit-lock cleanup failure");
			} catch (SailException expected) {
				assertTrue(expected.getMessage().contains("RDF transaction committed successfully"));
				assertSame(cleanupFailure, expected.getCause());
			}

			try (var statements = connection.getStatements(statement.getSubject(), statement.getPredicate(),
					statement.getObject(), false, statement.getContext())) {
				assertTrue("The RDF transaction must remain committed", statements.hasNext());
			}
		} finally {
			store.shutDown();
		}
	}

	@Test
	public void successfulRdfCommitIsExplicitWhenBufferCleanupFails() throws Exception {
		MemoryStore store = new MemoryStore();
		store.init();
		IllegalStateException cleanupFailure = new IllegalStateException("Simulated spill cleanup failure");
		try (ResetFailureConnection connection = new ResetFailureConnection(
				(NotifyingSailConnection) store.getConnection(),
				new SuccessfulActivationCleanupFailingCoordinator(null), cleanupFailure)) {
			Statement statement = vf.createStatement(vf.createIRI("urn:buffer-cleanup"),
					vf.createIRI("urn:p"), vf.createLiteral("committed"), vf.createIRI("urn:context"));
			connection.begin();
			connection.addStatement(statement.getSubject(), statement.getPredicate(),
					statement.getObject(), statement.getContext());
			connection.failCleanupOnCommit();

			try {
				connection.commit();
				fail("Expected buffer cleanup failure");
			} catch (SailException expected) {
				assertTrue(expected.getMessage().contains("RDF transaction committed successfully"));
				assertSame(cleanupFailure, expected.getCause());
			}

			try (var statements = connection.getStatements(statement.getSubject(), statement.getPredicate(),
					statement.getObject(), false, statement.getContext())) {
				assertTrue("The RDF transaction must remain committed", statements.hasNext());
			}
		} finally {
			store.shutDown();
		}
	}

	private SailRepository createRepository(FtsSearchService service) {
		return createRepository(service, Set.of());
	}

	private SailRepository createRepository(FtsSearchService service, Set<String> excludedModels) {
		FtsSail sail = createSail(service, excludedModels);
		sail.setBaseSail(new MemoryStore());
		SailRepository repository = new SailRepository(sail);
		repository.init();
		return repository;
	}

	private FtsSail createSail(FtsIndexUpdateStrategy strategy, Set<String> excludedModels) {
		try {
			return new FtsSail(strategy, temporaryFolder.newFolder().toPath(), UUID.randomUUID().toString(),
					FtsFederatedServiceConfig.defaults(), excludedModels);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private FileFtsIndexUpdateCoordinator createCoordinator(FtsIndexUpdateStrategy strategy) throws Exception {
		FileFtsIndexUpdateCoordinator coordinator = new FileFtsIndexUpdateCoordinator(
				temporaryFolder.newFolder().toPath(), UUID.randomUUID().toString(), strategy, ignored -> {
				});
		coordinator.recover(Set.of());
		return coordinator;
	}

	private static class RecordingSearchService implements FtsSearchService {
		private final Set<Statement> addedStatements = new LinkedHashSet<>();
		private final Set<Statement> removedStatements = new LinkedHashSet<>();
		private final List<org.eclipse.rdf4j.model.Resource[]> clearedContexts = new ArrayList<>();
		private int addRemoveCount;
		private int clearCount;
		private int commitCount;
		private final List<String> operationOrder = new ArrayList<>();

		@Override
		public void prepareCommit(String transactionId) {
		}

		@Override
		public void addRemoveStatements(Set<Statement> added, Set<Statement> removed) {
			addRemoveCount++;
			operationOrder.add(added.isEmpty() ? "remove:" + removed.size() : "add:" + added.size());
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
		public synchronized void commit() {
			commitCount++;
			notifyAll();
		}

		synchronized void awaitCommits(int expected) {
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
			while (commitCount < expected && System.nanoTime() < deadline) {
				try {
					TimeUnit.NANOSECONDS.timedWait(this, deadline - System.nanoTime());
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new AssertionError("Interrupted while waiting for FTS updates", e);
				}
			}
			assertTrue("Timed out waiting for FTS updates, expected " + expected + " but got " + commitCount,
					commitCount >= expected);
		}
	}

	private static final class FailingPrepareCoordinator implements FtsIndexUpdateCoordinator {
		@Override
		public CommitLock acquireCommitLock() {
			return new CommitLock() {
				@Override
				public long nextSequence() {
					return 1;
				}

				@Override
				public void close() {
				}
			};
		}

		@Override
		public void prepare(String transactionId, long sequence, Iterable<FtsIndexOperation> operations)
				throws IOException {
			throw new IOException("Unable to persist prepared transaction");
		}

		@Override
		public void activate(String transactionId) {
		}

		@Override
		public void rollback(String transactionId) {
		}

		@Override
		public void recover(CommittedTransactions committedTransactions) {
		}

		@Override
		public FtsOutboxStatus status() {
			return new FtsOutboxStatus(0, 0, 0, -1);
		}

		@Override
		public void awaitTermination() {
		}

		@Override
		public void close() {
		}
	}

	private static final class FailOnceActivationCoordinator implements FtsIndexUpdateCoordinator {
		private final FtsIndexUpdateCoordinator delegate;
		private final AtomicBoolean failActivation;

		private FailOnceActivationCoordinator(FtsIndexUpdateCoordinator delegate, AtomicBoolean failActivation) {
			this.delegate = delegate;
			this.failActivation = failActivation;
		}

		@Override
		public CommitLock acquireCommitLock() throws Exception {
			return delegate.acquireCommitLock();
		}

		@Override
		public void prepare(String transactionId, long sequence, Iterable<FtsIndexOperation> operations)
				throws Exception {
			delegate.prepare(transactionId, sequence, operations);
		}

		@Override
		public void activate(String transactionId) throws Exception {
			if (failActivation.compareAndSet(true, false)) {
				throw new IOException("Simulated activation failure");
			}
			delegate.activate(transactionId);
		}

		@Override
		public void rollback(String transactionId) throws Exception {
			delegate.rollback(transactionId);
		}

		@Override
		public void recover(CommittedTransactions committedTransactions) throws Exception {
			delegate.recover(committedTransactions);
		}

		@Override
		public FtsOutboxStatus status() throws Exception {
			return delegate.status();
		}

		@Override
		public void awaitTermination() throws Exception {
			delegate.awaitTermination();
		}

		@Override
		public void close() throws Exception {
			delegate.close();
		}
	}

	private static final class ActivationAndLockCleanupFailingCoordinator
			implements FtsIndexUpdateCoordinator {

		private final IOException activationFailure =
				new IOException("Simulated activation failure");

		private final IOException cleanupFailure =
				new IOException("Simulated commit-lock cleanup failure");

		@Override
		public CommitLock acquireCommitLock() {
			return new CommitLock() {
				@Override
				public long nextSequence() {
					return 1;
				}

				@Override
				public void close() throws Exception {
					throw cleanupFailure;
				}
			};
		}

		@Override
		public void prepare(
				String transactionId,
				long sequence,
				Iterable<FtsIndexOperation> operations) {
		}

		@Override
		public void activate(String transactionId) throws Exception {
			throw activationFailure;
		}

		@Override
		public void rollback(String transactionId) {
		}

		@Override
		public void recover(CommittedTransactions committedTransactions) {
		}

		@Override
		public FtsOutboxStatus status() {
			return new FtsOutboxStatus(0, 0, 0, -1);
		}

		@Override
		public void awaitTermination() {
		}

		@Override
		public void close() {
		}
	}

	private static final class SuccessfulActivationCleanupFailingCoordinator
			implements FtsIndexUpdateCoordinator {
		private final IOException cleanupFailure;

		private SuccessfulActivationCleanupFailingCoordinator(IOException cleanupFailure) {
			this.cleanupFailure = cleanupFailure;
		}

		@Override
		public CommitLock acquireCommitLock() {
			return new CommitLock() {
				@Override
				public long nextSequence() {
					return 1;
				}

				@Override
				public void close() throws Exception {
					if (cleanupFailure != null) {
						throw cleanupFailure;
					}
				}
			};
		}

		@Override
		public void prepare(String transactionId, long sequence, Iterable<FtsIndexOperation> operations) {
		}

		@Override
		public void activate(String transactionId) {
		}

		@Override
		public void rollback(String transactionId) {
		}

		@Override
		public void recover(CommittedTransactions committedTransactions) {
		}

		@Override
		public FtsOutboxStatus status() {
			return new FtsOutboxStatus(0, 0, 0, -1);
		}

		@Override
		public void awaitTermination() {
		}

		@Override
		public void close() {
		}
	}

	private static final class ResetFailureConnection extends FtsSailConnection {
		private final RuntimeException cleanupFailure;
		private boolean failCleanup;

		private ResetFailureConnection(NotifyingSailConnection wrappedConnection,
				FtsIndexUpdateCoordinator coordinator, RuntimeException cleanupFailure) {
			super(wrappedConnection, coordinator, Set.of());
			this.cleanupFailure = cleanupFailure;
		}

		private void failCleanupOnCommit() {
			failCleanup = true;
		}

		@Override
		void resetBuffer() {
			if (failCleanup) {
				throw cleanupFailure;
			}
			super.resetBuffer();
		}
	}
}
