package io.github.linkedfactory.core.rdf4j.fts;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.linkedfactory.core.rdf4j.fts.elastic.HttpFtsSearchService;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FtsSailConcurrentWritersTest {
	private static final byte[] BULK_SUCCESS_RESPONSE =
			"{\"errors\":false,\"items\":[]}".getBytes(StandardCharsets.UTF_8);

	private final SimpleValueFactory vf = SimpleValueFactory.getInstance();
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();
	private HttpServer server;
	private AtomicInteger requests;

	@Before
	public void setUp() throws IOException {
		Files.createDirectories(Path.of("target"));
		requests = new AtomicInteger();
		server = HttpServer.create(new InetSocketAddress(0), 0);
		server.createContext("/fts/bulk", this::handleRequest);
		server.start();
	}

	@After
	public void tearDown() {
		if (server != null) {
			server.stop(0);
		}
	}

	@Test
	public void concurrentWritersCommitWithoutErrors() throws Exception {
		int threads = 4;
		int iterations = 25;
		ExecutorService pool = Executors.newFixedThreadPool(threads);
		CountDownLatch start = new CountDownLatch(1);
		List<Future<Void>> futures = new ArrayList<>();

		try {
			for (int t = 0; t < threads; t++) {
				final int threadIndex = t;
				Callable<Void> task = () -> {
					start.await(30, TimeUnit.SECONDS);
					Path outboxDir = Files.createTempDirectory(
							Path.of("target").toAbsolutePath(), "fts-outbox-soak-" + threadIndex);
					HttpFtsSearchService searchService = new HttpFtsSearchService(
							"http://127.0.0.1:" + server.getAddress().getPort(),
							"/fts/bulk",
							true,
							outboxDir.toString());
					FtsSail sail = new FtsSail(searchService, outboxDir.resolve("common"),
							"concurrent-store-" + threadIndex, FtsFederatedServiceConfig.defaults(), Set.of());
					sail.setBaseSail(new MemoryStore());
					SailRepository repository = new SailRepository(sail);
					repository.init();
					try {
						for (int i = 0; i < iterations; i++) {
							try (SailRepositoryConnection connection = repository.getConnection()) {
								connection.begin();
								connection.add(vf.createIRI("urn:s" + threadIndex + ":" + i),
										vf.createIRI("urn:p"),
										vf.createLiteral("value-" + threadIndex + "-" + i),
										vf.createIRI("urn:context"));
								connection.commit();
							}
						}
						long deliveryDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
						while (sail.getOutboxStatus().recordCount() != 0
								&& System.nanoTime() < deliveryDeadline) {
							Thread.sleep(10);
						}
						assertEquals("FTS WAL did not drain for writer " + threadIndex,
								0, sail.getOutboxStatus().recordCount());
						try (var files = Files.list(outboxDir)) {
							assertTrue(files.filter(path -> path.toString().endsWith(".jsonl")).findAny().isEmpty());
						}
					} finally {
						repository.shutDown();
						searchService.shutdown();
						try (var files = Files.walk(outboxDir)) {
							for (Path path : files.sorted(java.util.Comparator.reverseOrder()).toList()) {
								Files.deleteIfExists(path);
							}
						}
					}
					return null;
				};
				futures.add(pool.submit(task));
			}

			start.countDown();
			for (Future<Void> future : futures) {
				future.get(2, TimeUnit.MINUTES);
			}
			assertEquals(threads * iterations, requests.get());
		} finally {
			pool.shutdownNow();
		}
	}

	@Test
	public void remoteDeliveryIsOrderedAndDoesNotBlockLaterRdfCommit() throws Exception {
		BlockingIndexStrategy strategy = new BlockingIndexStrategy();
		Path outbox = temporaryFolder.newFolder("ordered-outbox").toPath();
		FtsSail sail = new FtsSail(strategy, outbox, "ordered-store",
				FtsFederatedServiceConfig.defaults(), Set.of());
		sail.setBaseSail(new MemoryStore());
		SailRepository repository = new SailRepository(sail);
		repository.init();
		var context = vf.createIRI("urn:context");
		var subjectA = vf.createIRI("urn:subject:a");
		var subjectB = vf.createIRI("urn:subject:b");
		var predicate = vf.createIRI("urn:p");
		var statementA = vf.createStatement(subjectA, predicate, vf.createLiteral("A"), context);
		try {
			try (SailRepositoryConnection first = repository.getConnection()) {
				first.begin();
				first.add(statementA);
				first.commit();
			}
			assertTrue(strategy.firstUpdateEntered.await(10, TimeUnit.SECONDS));

			try (SailRepositoryConnection second = repository.getConnection()) {
				second.begin();
				second.remove(statementA);
				second.add(subjectB, predicate, vf.createLiteral("B"), context);
				second.commit();
			}

			strategy.releaseFirstUpdate.countDown();
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
			while (strategy.appliedSequences.size() < 2 && System.nanoTime() < deadline) {
				Thread.sleep(10);
			}
			assertEquals(List.of(1L, 2L), strategy.appliedSequences);
			assertEquals(Set.of(subjectB.stringValue()), strategy.indexedSubjects);
		} finally {
			strategy.releaseFirstUpdate.countDown();
			repository.shutDown();
		}
	}

	@Test
	public void committedRdfAndPendingWalRecoverTogetherAfterRestart() throws Exception {
		Path storeDirectory = temporaryFolder.newFolder("persistent-rdf").toPath();
		Path outbox = temporaryFolder.newFolder("persistent-outbox").toPath();
		FailOnceIndexStrategy interrupted = new FailOnceIndexStrategy();
		FtsSail firstSail = new FtsSail(interrupted, outbox, "persistent-store",
				FtsFederatedServiceConfig.defaults(), Set.of());
		firstSail.setBaseSail(new NativeStore(storeDirectory.toFile()));
		SailRepository firstRepository = new SailRepository(firstSail);
		firstRepository.init();
		var statement = vf.createStatement(vf.createIRI("urn:subject:recover"),
				vf.createIRI("urn:p"), vf.createLiteral("recover"), vf.createIRI("urn:context"));
		try (SailRepositoryConnection connection = firstRepository.getConnection()) {
			connection.begin();
			connection.add(statement);
			connection.commit();
		}
		assertTrue(interrupted.deliveryEntered.await(10, TimeUnit.SECONDS));
		interrupted.releaseDelivery.countDown();
		firstRepository.shutDown();

		RecordingIndexStrategy recovered = new RecordingIndexStrategy();
		FtsSail secondSail = new FtsSail(recovered, outbox, "persistent-store",
				FtsFederatedServiceConfig.defaults(), Set.of());
		secondSail.setBaseSail(new NativeStore(storeDirectory.toFile()));
		SailRepository secondRepository = new SailRepository(secondSail);
		secondRepository.init();
		try {
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
			while (recovered.applied.isEmpty() && System.nanoTime() < deadline) {
				Thread.sleep(10);
			}
			assertEquals(List.of(1L), recovered.applied);
			try (SailRepositoryConnection connection = secondRepository.getConnection()) {
				assertTrue(connection.hasStatement(statement, false));
				assertEquals(1, connection.getSailConnection().size());
			}
		} finally {
			secondRepository.shutDown();
		}
	}

	@Test
	public void startupFailsWhenCommittedWalRecordIsCorrupted() throws Exception {
		Path storeDirectory = temporaryFolder.newFolder("corrupt-rdf").toPath();
		Path outbox = temporaryFolder.newFolder("corrupt-outbox").toPath();
		FailOnceIndexStrategy interrupted = new FailOnceIndexStrategy();
		FtsSail firstSail = new FtsSail(interrupted, outbox, "corrupt-store",
				FtsFederatedServiceConfig.defaults(), Set.of());
		firstSail.setBaseSail(new NativeStore(storeDirectory.toFile()));
		SailRepository firstRepository = new SailRepository(firstSail);
		firstRepository.init();
		try (SailRepositoryConnection connection = firstRepository.getConnection()) {
			connection.begin();
			connection.add(vf.createIRI("urn:subject:corrupt"), vf.createIRI("urn:p"),
					vf.createLiteral("corrupt"), vf.createIRI("urn:context"));
			connection.commit();
		}
		assertTrue(interrupted.deliveryEntered.await(10, TimeUnit.SECONDS));
		interrupted.releaseDelivery.countDown();
		firstRepository.shutDown();

		Path pending;
		try (var files = Files.list(outbox)) {
			pending = files.filter(path -> path.getFileName().toString().endsWith(".pending"))
					.findFirst().orElseThrow();
		}
		Files.writeString(pending, "corrupt WAL\n", StandardCharsets.UTF_8);

		FtsSail secondSail = new FtsSail(new RecordingIndexStrategy(), outbox, "corrupt-store",
				FtsFederatedServiceConfig.defaults(), Set.of());
		secondSail.setBaseSail(new NativeStore(storeDirectory.toFile()));
		try {
			secondSail.init();
			fail("Expected startup to reject a corrupt WAL record referenced by the RDF journal");
		} catch (SailException expected) {
			assertTrue(expected.getMessage().contains("recover"));
		} finally {
			secondSail.shutDown();
		}
	}

	@Test
	public void runtimeRecoveryCanResolvePreparedRecordAndAdvanceSequence() throws Exception {
		Path outbox = temporaryFolder.newFolder("runtime-recovery-outbox").toPath();
		RecordingIndexStrategy strategy = new RecordingIndexStrategy();
		FileFtsIndexUpdateCoordinator coordinator = new FileFtsIndexUpdateCoordinator(
				outbox, "runtime-recovery-store", strategy, ignored -> {
				});
		try {
			coordinator.recover(Set.of());
			coordinator.prepare("runtime-recovery", 9, List.of(new FtsIndexOperation.Clear()));
			assertEquals(1, coordinator.status().recordCount());
			assertEquals(0, coordinator.status().pendingRecordCount());
			assertTrue(coordinator.status().directoryBytes() >= coordinator.status().bytes());

			coordinator.recover(Set.of("runtime-recovery"));
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
			while (strategy.applied.isEmpty() && System.nanoTime() < deadline) {
				Thread.sleep(10);
			}
			assertEquals(List.of(9L), strategy.applied);
			assertTrue(coordinator.status().recoveryDurationMillis() >= 0);
			assertEquals(null, coordinator.status().lastRecoveryError());
			try (var lock = coordinator.acquireCommitLock()) {
				assertEquals(10, lock.nextSequence());
			}
		} finally {
			coordinator.close();
		}
	}

	@Test
	public void runtimeRecoverySerializesWithCommitWhileWorkerDrainsEarlierSequence() throws Exception {
		Path outbox = temporaryFolder.newFolder("concurrent-recovery-outbox").toPath();
		CountDownLatch firstApplyEntered = new CountDownLatch(1);
		CountDownLatch releaseFirstApply = new CountDownLatch(1);
		CountDownLatch recoveryStarted = new CountDownLatch(1);
		java.util.concurrent.atomic.AtomicReference<Thread> recoveryThread = new java.util.concurrent.atomic.AtomicReference<>();
		AtomicBoolean journalRead = new AtomicBoolean();
		List<Long> appliedSequences = new CopyOnWriteArrayList<>();
		FtsIndexUpdateStrategy strategy = update -> {
			if (update.sequence() == 1) {
				firstApplyEntered.countDown();
				awaitUninterruptibly(releaseFirstApply);
			}
			appliedSequences.add(update.sequence());
		};
		FileFtsIndexUpdateCoordinator coordinator = new FileFtsIndexUpdateCoordinator(
				outbox, "concurrent-recovery-store", strategy, ignored -> {
				});
		ExecutorService pool = Executors.newFixedThreadPool(2);
		try {
			coordinator.recover(Set.of());
			try (var lock = coordinator.acquireCommitLock()) {
				long sequence = lock.nextSequence();
				coordinator.prepare("earlier-update", sequence, List.of(new FtsIndexOperation.Clear()));
				coordinator.activate("earlier-update");
			}
			assertTrue(firstApplyEntered.await(10, TimeUnit.SECONDS));

			try (var lock = coordinator.acquireCommitLock()) {
				long sequence = lock.nextSequence();
				assertEquals(2, sequence);
				coordinator.prepare("recovered-update", sequence, List.of(new FtsIndexOperation.Clear()));
			}

			Future<?> recovery = pool.submit(() -> {
				recoveryThread.set(Thread.currentThread());
				recoveryStarted.countDown();
				try {
					coordinator.recover(() -> {
						journalRead.set(true);
						return Set.of("recovered-update");
					});
				} catch (Exception e) {
					throw new java.util.concurrent.CompletionException(e);
				}
			});
			assertTrue("Recovery task did not start", recoveryStarted.await(10, TimeUnit.SECONDS));
			long lockDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
			while (recoveryThread.get().getState() != Thread.State.WAITING
					&& recoveryThread.get().getState() != Thread.State.TIMED_WAITING
					&& System.nanoTime() < lockDeadline) {
				Thread.yield();
			}
			assertTrue("Recovery did not wait for the in-flight worker",
					recoveryThread.get().getState() == Thread.State.WAITING
							|| recoveryThread.get().getState() == Thread.State.TIMED_WAITING);
			assertFalse("Recovery must not read journal state before acquiring worker ownership",
					journalRead.get());
			Future<Long> concurrentCommit = pool.submit(() -> {
				try (var lock = coordinator.acquireCommitLock()) {
					long sequence = lock.nextSequence();
					coordinator.prepare("concurrent-update", sequence, List.of(new FtsIndexOperation.Clear()));
					coordinator.activate("concurrent-update");
					return sequence;
				}
			});
			Thread.sleep(100);
			assertFalse("Commit must wait behind recovery's commit lock", concurrentCommit.isDone());

			releaseFirstApply.countDown();
			recovery.get(10, TimeUnit.SECONDS);
			assertTrue("Recovery should read the RDF journal after the worker finishes", journalRead.get());
			assertEquals(3L, (long) concurrentCommit.get(10, TimeUnit.SECONDS));

			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
			while (appliedSequences.size() < 3 && System.nanoTime() < deadline) {
				Thread.sleep(10);
			}
			assertEquals(List.of(1L, 2L, 3L), appliedSequences);
			assertEquals(0, coordinator.status().recordCount());
		} finally {
			releaseFirstApply.countDown();
			pool.shutdownNow();
			coordinator.close();
		}
	}

	@Test
	public void startupRejectsDuplicateWalSequences() throws Exception {
		Path outbox = temporaryFolder.newFolder("duplicate-sequences-outbox").toPath();
		RecordingIndexStrategy strategy = new RecordingIndexStrategy();
		FileFtsIndexUpdateCoordinator coordinator = new FileFtsIndexUpdateCoordinator(
				outbox, "duplicate-sequences-store", strategy, ignored -> {
				});
		try {
			coordinator.prepare("duplicate-a", 7, List.of(new FtsIndexOperation.Clear()));
			coordinator.prepare("duplicate-b", 7, List.of(new FtsIndexOperation.Clear()));
			try {
				coordinator.recover(Set.of());
				fail("Expected recovery to reject duplicate WAL sequence numbers");
			} catch (IOException expected) {
				assertTrue(expected.getMessage().contains("Duplicate FTS WAL sequence"));
			}
			assertTrue(coordinator.status().lastRecoveryError().contains("Duplicate FTS WAL sequence"));
		} finally {
			coordinator.close();
		}
	}

	@Test
	public void recoveryValidatesAllRecordsBeforeActivatingCommittedPreparedRecords() throws Exception {
		Path outbox = temporaryFolder.newFolder("recovery-validation-outbox").toPath();
		RecordingIndexStrategy strategy = new RecordingIndexStrategy();
		FileFtsIndexUpdateCoordinator coordinator = new FileFtsIndexUpdateCoordinator(
				outbox, "recovery-validation-store", strategy, ignored -> {
				});
		try {
			coordinator.prepare("committed-prepared", 9, List.of(new FtsIndexOperation.Clear()));
			coordinator.prepare("duplicate-sequence", 9, List.of(new FtsIndexOperation.Clear()));

			try {
				coordinator.recover(Set.of("committed-prepared"));
				fail("Expected recovery to reject duplicate sequences before state transitions");
			} catch (IOException expected) {
				assertTrue(expected.getMessage().contains("Duplicate FTS WAL sequence"));
			}

			assertTrue(Files.exists(outbox.resolve("committed-prepared.prepared")));
			assertTrue(Files.exists(outbox.resolve("duplicate-sequence.prepared")));
			try (var files = Files.list(outbox)) {
				assertTrue(files.noneMatch(path -> path.getFileName().toString().endsWith(".pending")));
			}
		} finally {
			coordinator.close();
		}
	}

	@Test
	public void recoveryResumesAfterFilesystemFailureDuringPreparedRecordTransitions() throws Exception {
		Path outbox = temporaryFolder.newFolder("partial-transition-recovery-outbox").toPath();
		String firstId = "partial-transition-first";
		String secondId = "partial-transition-second";
		String blockedTarget = String.format("%020d-%s.pending", 2L, secondId);
		AtomicBoolean failTransition = new AtomicBoolean(true);
		RecordingIndexStrategy firstStrategy = new RecordingIndexStrategy();
		FileFtsIndexUpdateCoordinator first = new FileFtsIndexUpdateCoordinator(
				outbox, "partial-transition-store", firstStrategy, ignored -> {
				}, java.time.Duration.ofSeconds(1), (source, target, options) -> {
					if (target.getFileName().toString().equals(blockedTarget)
							&& failTransition.compareAndSet(true, false)) {
						throw new IOException("Simulated filesystem transition failure");
					}
					return Files.move(source, target, options);
				});
		first.prepare(firstId, 1, List.of(new FtsIndexOperation.Clear()));
		first.prepare(secondId, 2, List.of(new FtsIndexOperation.Clear()));
		try {
			try {
				first.recover(Set.of(firstId, secondId));
				fail("Expected the second WAL transition to fail");
			} catch (IOException expected) {
				assertTrue(expected.getMessage().contains("Simulated filesystem transition failure"));
			}
			assertTrue(Files.exists(outbox.resolve(String.format("%020d-%s.pending", 1L, firstId))));
			assertTrue(Files.exists(outbox.resolve(secondId + ".prepared")));
			assertEquals(2, first.status().recordCount());
		} finally {
			first.close();
		}

		RecordingIndexStrategy recovered = new RecordingIndexStrategy();
		FileFtsIndexUpdateCoordinator restarted = new FileFtsIndexUpdateCoordinator(
				outbox, "partial-transition-store", recovered, ignored -> {
				});
		try {
			restarted.recover(Set.of(firstId, secondId));
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
			while (restarted.status().recordCount() != 0 && System.nanoTime() < deadline) {
				Thread.sleep(10);
			}
			assertEquals(List.of(1L, 2L), recovered.applied);
			assertEquals(0, restarted.status().recordCount());
		} finally {
			restarted.close();
		}
	}

	@Test
	public void shutdownCancelsAfterGraceWithoutClosingAnActiveStrategy() throws Exception {
		Path outbox = temporaryFolder.newFolder("bounded-shutdown-outbox").toPath();
		CountDownLatch applyEntered = new CountDownLatch(1);
		CountDownLatch finishApply = new CountDownLatch(1);
		AtomicBoolean strategyClosed = new AtomicBoolean();
		FtsIndexUpdateStrategy strategy = new FtsIndexUpdateStrategy() {
			@Override
			public void apply(FtsIndexUpdate update) {
				applyEntered.countDown();
				boolean interrupted = false;
				while (true) {
					try {
						finishApply.await();
						break;
					} catch (InterruptedException e) {
						interrupted = true;
					}
				}
				if (interrupted) {
					Thread.currentThread().interrupt();
				}
			}

			@Override
			public void close() {
				strategyClosed.set(true);
			}
		};
		FileFtsIndexUpdateCoordinator coordinator = new FileFtsIndexUpdateCoordinator(
				outbox, "bounded-shutdown-store", strategy, ignored -> {
				}, java.time.Duration.ofMillis(50));
		coordinator.recover(Set.of());
		try (var lock = coordinator.acquireCommitLock()) {
			long sequence = lock.nextSequence();
			coordinator.prepare("shutdown-in-flight", sequence, List.of(new FtsIndexOperation.Clear()));
			coordinator.activate("shutdown-in-flight");
		}
		assertTrue(applyEntered.await(10, TimeUnit.SECONDS));

		long closeStarted = System.nanoTime();
		coordinator.close();
		assertTrue("Shutdown exceeded its bounded grace/cancellation period",
				System.nanoTime() - closeStarted < TimeUnit.SECONDS.toNanos(2));
		assertFalse("Strategy must not close while apply is still active", strategyClosed.get());
		assertEquals(1, coordinator.status().recordCount());

		finishApply.countDown();
		long closeDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		while (!strategyClosed.get() && System.nanoTime() < closeDeadline) {
			Thread.sleep(10);
		}
		assertTrue("Strategy should close after the worker finishes", strategyClosed.get());
		assertEquals(1, coordinator.status().recordCount());
	}

	@Test
	public void sailShutdownWaitsForBlockedJournalCleanerBeforeClosingBaseSail() throws Exception {
		Path outbox = temporaryFolder.newFolder("blocked-cleaner-shutdown-outbox").toPath();
		CountDownLatch cleanerEntered = new CountDownLatch(1);
		CountDownLatch releaseCleaner = new CountDownLatch(1);
		AtomicBoolean baseSailClosed = new AtomicBoolean();
		FtsIndexUpdateStrategy strategy = update -> {
		};
		FtsIndexUpdateCoordinatorFactory coordinatorFactory = (directory, storeIdentity, updateStrategy,
				journalCleaner) -> new FileFtsIndexUpdateCoordinator(
						directory, storeIdentity, updateStrategy, transactionId -> {
							cleanerEntered.countDown();
							boolean interrupted = awaitUninterruptibly(releaseCleaner);
							try {
								journalCleaner.remove(transactionId);
							} finally {
								if (interrupted) {
									Thread.currentThread().interrupt();
								}
							}
						}, java.time.Duration.ofMillis(50));
		FtsSail sail = new FtsSail(strategy, outbox, "blocked-cleaner-store",
				FtsFederatedServiceConfig.defaults(), Set.of(), coordinatorFactory);
		sail.setBaseSail(new TrackingMemoryStore(baseSailClosed));
		SailRepository repository = new SailRepository(sail);
		repository.init();
		ExecutorService shutdownExecutor = Executors.newSingleThreadExecutor();
		try {
			try (SailRepositoryConnection connection = repository.getConnection()) {
				connection.begin();
				connection.add(vf.createIRI("urn:blocked-cleaner"), vf.createIRI("urn:p"),
						vf.createLiteral("value"), vf.createIRI("urn:context"));
				connection.commit();
			}
			assertTrue("Journal cleaner did not start", cleanerEntered.await(10, TimeUnit.SECONDS));

			Future<?> shutdown = shutdownExecutor.submit(repository::shutDown);
			Thread.sleep(300);
			assertFalse("Base Sail must remain open while the journal cleaner is active",
					baseSailClosed.get());
			assertFalse("Sail shutdown must wait for the journal cleaner", shutdown.isDone());

			releaseCleaner.countDown();
			shutdown.get(10, TimeUnit.SECONDS);
			assertTrue("Base Sail should close after the journal cleaner exits", baseSailClosed.get());
		} finally {
			releaseCleaner.countDown();
			shutdownExecutor.shutdownNow();
			if (!baseSailClosed.get()) {
				repository.shutDown();
			}
		}
	}

	@Test
	public void recoveryReplaysBackendSuccessBeforeWalCompletionIdempotently() throws Exception {
		Path outbox = temporaryFolder.newFolder("success-before-done-outbox").toPath();
		Set<Long> backendState = java.util.concurrent.ConcurrentHashMap.newKeySet();
		CountDownLatch firstApply = new CountDownLatch(1);
		AtomicBoolean failAfterApplying = new AtomicBoolean(true);
		FtsIndexUpdateStrategy interruptedStrategy = update -> {
			backendState.add(update.sequence());
			if (failAfterApplying.compareAndSet(true, false)) {
				firstApply.countDown();
				throw new FtsIndexUpdateException("Simulated lost acknowledgement after backend success");
			}
		};
		FileFtsIndexUpdateCoordinator first = new FileFtsIndexUpdateCoordinator(
				outbox, "success-before-done-store", interruptedStrategy, ignored -> {
				});
		first.recover(Set.of());
		try (var lock = first.acquireCommitLock()) {
			long sequence = lock.nextSequence();
			first.prepare("success-before-done", sequence, List.of(new FtsIndexOperation.Clear()));
			first.activate("success-before-done");
		}
		assertTrue(firstApply.await(10, TimeUnit.SECONDS));
		long failureDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		while (first.status().deliveryFailureCount() == 0 && System.nanoTime() < failureDeadline) {
			Thread.sleep(10);
		}
		assertEquals(1, first.status().deliveryFailureCount());
		assertTrue(first.status().lastFailure().contains("lost acknowledgement"));
		first.close();

		List<Long> recoveredSequences = new CopyOnWriteArrayList<>();
		FtsIndexUpdateStrategy recovered = update -> {
			backendState.add(update.sequence());
			recoveredSequences.add(update.sequence());
		};
		FileFtsIndexUpdateCoordinator second = new FileFtsIndexUpdateCoordinator(
				outbox, "success-before-done-store", recovered, ignored -> {
				});
		try {
			second.recover(Set.of("success-before-done"));
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
			while (second.status().recordCount() != 0 && System.nanoTime() < deadline) {
				Thread.sleep(10);
			}
			assertEquals(Set.of(1L), backendState);
			assertEquals(List.of(1L), recoveredSequences);
			assertEquals(0, second.status().recordCount());
			assertTrue(second.status().lastSuccessfulDeliveryMillis() > 0);
			assertEquals(null, second.status().lastFailure());
		} finally {
			second.close();
		}
	}

	private void handleRequest(HttpExchange exchange) throws IOException {
		requests.incrementAndGet();
		try (InputStream in = exchange.getRequestBody()) {
			in.readAllBytes();
		}
		exchange.sendResponseHeaders(200, BULK_SUCCESS_RESPONSE.length);
		exchange.getResponseBody().write(BULK_SUCCESS_RESPONSE);
		exchange.close();
	}

	private static boolean awaitUninterruptibly(CountDownLatch latch) {
		boolean interrupted = false;
		while (true) {
			try {
				latch.await();
				return interrupted;
			} catch (InterruptedException e) {
				interrupted = true;
			}
		}
	}

	private static final class TrackingMemoryStore extends MemoryStore {
		private final AtomicBoolean closed;

		private TrackingMemoryStore(AtomicBoolean closed) {
			this.closed = closed;
		}

		@Override
		public void shutDown() throws SailException {
			super.shutDown();
			closed.set(true);
		}
	}

	private static final class BlockingIndexStrategy implements FtsIndexUpdateStrategy {
		private final CountDownLatch firstUpdateEntered = new CountDownLatch(1);
		private final CountDownLatch releaseFirstUpdate = new CountDownLatch(1);
		private final List<Long> appliedSequences = new CopyOnWriteArrayList<>();
		private final Set<String> indexedSubjects = java.util.concurrent.ConcurrentHashMap.newKeySet();

		@Override
		public void apply(FtsIndexUpdate update) throws FtsIndexUpdateException {
			if (update.sequence() == 1) {
				firstUpdateEntered.countDown();
				try {
					if (!releaseFirstUpdate.await(10, TimeUnit.SECONDS)) {
						throw new FtsIndexUpdateException("Timed out waiting to release first index update");
					}
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new FtsIndexUpdateException("Interrupted while applying first update", e);
				}
			}
			for (FtsIndexOperation operation : update.operations()) {
				if (operation instanceof FtsIndexOperation.Statements statements) {
					statements.removed().forEach(statement -> indexedSubjects.remove(
							statement.getSubject().stringValue()));
					statements.added().forEach(statement -> indexedSubjects.add(
							statement.getSubject().stringValue()));
				}
			}
			appliedSequences.add(update.sequence());
		}
	}

	private static final class FailOnceIndexStrategy implements FtsIndexUpdateStrategy {
		private final CountDownLatch deliveryEntered = new CountDownLatch(1);
		private final CountDownLatch releaseDelivery = new CountDownLatch(1);

		@Override
		public void apply(FtsIndexUpdate update) throws FtsIndexUpdateException {
			deliveryEntered.countDown();
			try {
				if (!releaseDelivery.await(10, TimeUnit.SECONDS)) {
					throw new FtsIndexUpdateException("Timed out waiting to interrupt delivery");
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new FtsIndexUpdateException("Interrupted during test delivery", e);
			}
			throw new FtsIndexUpdateException("Simulated interruption after RDF commit");
		}
	}

	private static final class RecordingIndexStrategy implements FtsIndexUpdateStrategy {
		private final List<Long> applied = new CopyOnWriteArrayList<>();

		@Override
		public void apply(FtsIndexUpdate update) {
			applied.add(update.sequence());
		}
	}
}
