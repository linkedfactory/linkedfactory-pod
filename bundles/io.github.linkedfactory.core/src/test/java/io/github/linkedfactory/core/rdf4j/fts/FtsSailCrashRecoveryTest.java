package io.github.linkedfactory.core.rdf4j.fts;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FtsSailCrashRecoveryTest {
	private static final int CRASH_EXIT_CODE = 73;
	private static final String STORE_IDENTITY = "crash-recovery-store";

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void processTerminationAtTransactionBoundariesRecoversDurableState() throws Exception {
		for (CrashPoint point : CrashPoint.values()) {
			Path root = temporaryFolder.newFolder(point.name()).toPath();
			Path rdfStore = root.resolve("rdf-store");
			Path outbox = root.resolve("outbox");
			Path backendEffect = root.resolve("backend-effect");

			Process child = startCrashWorker(point, rdfStore, outbox, backendEffect);
			if (!child.waitFor(30, TimeUnit.SECONDS)) {
				child.destroyForcibly();
				assertTrue("Crash worker timed out at " + point, child.waitFor(10, TimeUnit.SECONDS));
			}
			String output = new String(child.getInputStream().readAllBytes(),
					java.nio.charset.StandardCharsets.UTF_8);
			assertEquals("Unexpected child exit at " + point + ":\n" + output,
					CRASH_EXIT_CODE, child.exitValue());
			assertEquals(point.hasBackendEffect, Files.exists(backendEffect));

			RecordingStrategy recoveredStrategy = new RecordingStrategy();
			FtsSail sail = new FtsSail(recoveredStrategy, outbox, STORE_IDENTITY,
					FtsFederatedServiceConfig.defaults(), Set.of());
			sail.setBaseSail(new NativeStore(rdfStore.toFile()));
			SailRepository repository = new SailRepository(sail);
			repository.init();
			try {
				try (SailRepositoryConnection connection = repository.getConnection()) {
					assertEquals(point.rdfCommitted, connection.hasStatement(statement(point), false));
				}

				long deliveryDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
				while (sail.getOutboxStatus().recordCount() != 0
						&& System.nanoTime() < deliveryDeadline) {
					Thread.sleep(10);
				}
				try (SailRepositoryConnection connection = repository.getConnection()) {
					assertFalse(connection.hasStatement(null, FtsSailConnection.JOURNAL_PREDICATE,
							null, false, FtsSailConnection.JOURNAL_CONTEXT));
				}
				awaitStableOutboxDrain(sail, point);
				assertEquals(point.replayAfterRestart ? 1 : 0, recoveredStrategy.applied.size());
				assertEquals(point.replayAfterRestart ? Set.of(1L) : Set.of(),
						Set.copyOf(recoveredStrategy.applied));
			} finally {
				repository.shutDown();
			}
		}
	}

	private static void awaitStableOutboxDrain(FtsSail sail, CrashPoint point) throws Exception {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
		long emptySince = 0;
		while (System.nanoTime() < deadline) {
			if (sail.getOutboxStatus().recordCount() == 0) {
				if (emptySince == 0) {
					emptySince = System.nanoTime();
				} else if (System.nanoTime() - emptySince >= TimeUnit.MILLISECONDS.toNanos(100)) {
					return;
				}
			} else {
				emptySince = 0;
			}
			Thread.sleep(10);
		}
		throw new AssertionError("WAL did not stably drain after recovery at " + point);
	}

	private Process startCrashWorker(CrashPoint point, Path rdfStore, Path outbox, Path backendEffect)
			throws IOException {
		String classPath = System.getProperty("surefire.test.class.path",
				System.getProperty("java.class.path"));
		ProcessBuilder builder = new ProcessBuilder(
				Path.of(System.getProperty("java.home"), "bin", "java").toString(),
				"-cp", classPath, CrashWorker.class.getName(),
				point.name(), rdfStore.toString(), outbox.toString(), backendEffect.toString());
		builder.redirectErrorStream(true);
		return builder.start();
	}

	private static Statement statement(CrashPoint point) {
		var values = SimpleValueFactory.getInstance();
		return values.createStatement(values.createIRI("urn:crash:" + point.name()),
				values.createIRI("urn:p"), values.createLiteral(point.name()),
				values.createIRI("urn:context"));
	}

	private enum CrashPoint {
		AFTER_PREPARE(false, false, false),
		AFTER_RDF_COMMIT(false, true, true),
		AFTER_ACTIVATION(false, true, true),
		AFTER_BACKEND_APPLY(true, true, true),
		AFTER_DONE_BEFORE_JOURNAL_CLEANUP(true, true, false),
		AFTER_JOURNAL_CLEANUP_BEFORE_DONE_DELETE(true, true, false);

		private final boolean hasBackendEffect;
		private final boolean rdfCommitted;
		private final boolean replayAfterRestart;

		CrashPoint(boolean hasBackendEffect, boolean rdfCommitted, boolean replayAfterRestart) {
			this.hasBackendEffect = hasBackendEffect;
			this.rdfCommitted = rdfCommitted;
			this.replayAfterRestart = replayAfterRestart;
		}
	}

	private static final class RecordingStrategy implements FtsIndexUpdateStrategy {
		private final CopyOnWriteArrayList<Long> applied = new CopyOnWriteArrayList<>();

		@Override
		public void apply(FtsIndexUpdate update) {
			applied.add(update.sequence());
		}
	}

	public static final class CrashWorker {
		private CrashWorker() {
		}

		public static void main(String[] args) throws Exception {
			CrashPoint point = CrashPoint.valueOf(args[0]);
			Path rdfStore = Path.of(args[1]);
			Path outbox = Path.of(args[2]);
			Path backendEffect = Path.of(args[3]);
			CountDownLatch keepAlive = new CountDownLatch(1);
			FtsIndexUpdateStrategy strategy = update -> {
				try {
					if (point == CrashPoint.AFTER_ACTIVATION) {
						keepAlive.await();
					}
					if (point.hasBackendEffect) {
						Files.writeString(backendEffect, Long.toString(update.sequence()));
						if (point == CrashPoint.AFTER_BACKEND_APPLY) {
							Runtime.getRuntime().halt(CRASH_EXIT_CODE);
						}
					}
				} catch (IOException | InterruptedException e) {
					if (e instanceof InterruptedException) {
						Thread.currentThread().interrupt();
					}
					throw new FtsIndexUpdateException("Crash worker strategy failed", e);
				}
			};
			FtsIndexUpdateCoordinatorFactory coordinatorFactory = (directory, storeIdentity, updateStrategy,
					journalCleaner) -> {
				FtsIndexUpdateCoordinator.JournalCleaner crashCleaner = transactionId -> {
					if (point == CrashPoint.AFTER_DONE_BEFORE_JOURNAL_CLEANUP) {
						Runtime.getRuntime().halt(CRASH_EXIT_CODE);
					}
					journalCleaner.remove(transactionId);
					if (point == CrashPoint.AFTER_JOURNAL_CLEANUP_BEFORE_DONE_DELETE) {
						Runtime.getRuntime().halt(CRASH_EXIT_CODE);
					}
				};
				FileFtsIndexUpdateCoordinator delegate = new FileFtsIndexUpdateCoordinator(
						directory, storeIdentity, updateStrategy, crashCleaner, Duration.ofSeconds(1));
				return new CrashCoordinator(delegate, point);
			};

			FtsSail sail = new FtsSail(strategy, outbox, STORE_IDENTITY,
					FtsFederatedServiceConfig.defaults(), Set.of(), coordinatorFactory);
			sail.setBaseSail(new NativeStore(rdfStore.toFile()));
			SailRepository repository = new SailRepository(sail);
			repository.init();
			try (SailRepositoryConnection connection = repository.getConnection()) {
				connection.begin();
				connection.add(statement(point));
				connection.commit();
			}
			keepAlive.await();
		}
	}

	private static final class CrashCoordinator implements FtsIndexUpdateCoordinator {
		private final FtsIndexUpdateCoordinator delegate;
		private final CrashPoint point;

		private CrashCoordinator(FtsIndexUpdateCoordinator delegate, CrashPoint point) {
			this.delegate = delegate;
			this.point = point;
		}

		@Override
		public CommitLock acquireCommitLock() throws Exception {
			return delegate.acquireCommitLock();
		}

		@Override
		public void prepare(String transactionId, long sequence, Iterable<FtsIndexOperation> operations)
				throws Exception {
			delegate.prepare(transactionId, sequence, operations);
			if (point == CrashPoint.AFTER_PREPARE) {
				Runtime.getRuntime().halt(CRASH_EXIT_CODE);
			}
		}

		@Override
		public void activate(String transactionId) throws Exception {
			if (point == CrashPoint.AFTER_RDF_COMMIT) {
				Runtime.getRuntime().halt(CRASH_EXIT_CODE);
			}
			delegate.activate(transactionId);
			if (point == CrashPoint.AFTER_ACTIVATION) {
				Runtime.getRuntime().halt(CRASH_EXIT_CODE);
			}
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
}
