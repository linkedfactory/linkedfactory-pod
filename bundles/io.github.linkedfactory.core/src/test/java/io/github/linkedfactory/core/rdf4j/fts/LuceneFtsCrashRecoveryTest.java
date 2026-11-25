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
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The generic {@link FtsSailCrashRecoveryTest} proves the WAL/coordinator's own bookkeeping
 * survives a crash using a fake, in-memory strategy. This test proves the same crash-and-restart
 * cycle against a real {@link LuceneFtsIndex}: that a real Lucene commit is durable across an
 * abrupt process kill, and that redelivering the same idempotent update after restart (the
 * coordinator's own recovery contract) does not duplicate or corrupt what Lucene has on disk.
 */
public class LuceneFtsCrashRecoveryTest {
	private static final int CRASH_EXIT_CODE = 74;
	private static final String STORE_IDENTITY = "lucene-crash-recovery-store";

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void killingTheProcessRightAfterLuceneAppliesAnUpdateStillRecoversCorrectly() throws Exception {
		Path root = temporaryFolder.newFolder().toPath();
		Path rdfStore = root.resolve("rdf-store");
		Path outbox = root.resolve("outbox");
		Path indexDir = root.resolve("lucene-index");

		Process child = startCrashWorker(rdfStore, outbox, indexDir);
		if (!child.waitFor(30, TimeUnit.SECONDS)) {
			child.destroyForcibly();
			assertTrue("Crash worker timed out", child.waitFor(10, TimeUnit.SECONDS));
		}
		String output = new String(child.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
		assertEquals("Unexpected child exit:\n" + output, CRASH_EXIT_CODE, child.exitValue());

		try (LuceneFtsIndex reopenedIndex = new LuceneFtsIndex(indexDir)) {
			LuceneFtsSearchService recoveredStrategy = new LuceneFtsSearchService(reopenedIndex);
			FtsSail sail = new FtsSail(recoveredStrategy, outbox, STORE_IDENTITY,
					FtsFederatedServiceConfig.defaults(), Set.of());
			sail.setBaseSail(new NativeStore(rdfStore.toFile()));
			SailRepository repository = new SailRepository(sail);
			repository.init();
			try {
				long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
				while (sail.getOutboxStatus().recordCount() != 0 && System.nanoTime() < deadline) {
					Thread.sleep(10);
				}
				assertEquals("WAL did not fully drain after recovery", 0, sail.getOutboxStatus().recordCount());

				LuceneFtsSearchBackend backend = new LuceneFtsSearchBackend(reopenedIndex);
				var hits = backend.search(new FtsSearchRequest("crash-recovery-fox", null, 10, null, false, null));
				assertEquals("The statement committed before the crash must be indexed and searchable "
						+ "after recovery, exactly once", 1, hits.size());
				assertEquals(statement().getSubject().stringValue(), hits.get(0).getIri());
			} finally {
				repository.shutDown();
			}
		}
	}

	private Process startCrashWorker(Path rdfStore, Path outbox, Path indexDir) throws IOException {
		String classPath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
		ProcessBuilder builder = new ProcessBuilder(
				Path.of(System.getProperty("java.home"), "bin", "java").toString(),
				"--enable-native-access=ALL-UNNAMED", "--add-modules=jdk.incubator.vector",
				"-cp", classPath, CrashWorker.class.getName(),
				rdfStore.toString(), outbox.toString(), indexDir.toString());
		builder.redirectErrorStream(true);
		return builder.start();
	}

	private static Statement statement() {
		var values = SimpleValueFactory.getInstance();
		return values.createStatement(values.createIRI("urn:crash:lucene"),
				values.createIRI("urn:p"), values.createLiteral("crash-recovery-fox"),
				values.createIRI("urn:context"));
	}

	public static final class CrashWorker {
		private CrashWorker() {
		}

		public static void main(String[] args) throws Exception {
			Path rdfStore = Path.of(args[0]);
			Path outbox = Path.of(args[1]);
			Path indexDir = Path.of(args[2]);
			CountDownLatch keepAlive = new CountDownLatch(1);

			LuceneFtsIndex index = new LuceneFtsIndex(indexDir);
			LuceneFtsSearchService delegate = new LuceneFtsSearchService(index);
			FtsIndexUpdateStrategy strategy = update -> {
				try {
					delegate.apply(update);
				} finally {
					Runtime.getRuntime().halt(CRASH_EXIT_CODE);
				}
			};

			FtsSail sail = new FtsSail(strategy, outbox, STORE_IDENTITY, FtsFederatedServiceConfig.defaults(), Set.of());
			sail.setBaseSail(new NativeStore(rdfStore.toFile()));
			SailRepository repository = new SailRepository(sail);
			repository.init();
			try (SailRepositoryConnection connection = repository.getConnection()) {
				connection.begin();
				connection.add(statement());
				connection.commit();
			}
			keepAlive.await();
		}
	}
}
