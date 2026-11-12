package io.github.linkedfactory.core.rdf4j.fts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.time.Duration;

/** Per-store durable WAL and ordered, asynchronous delivery for FTS updates. */
public final class FileFtsIndexUpdateCoordinator implements FtsIndexUpdateCoordinator {
	private static final Logger logger = LoggerFactory.getLogger(FileFtsIndexUpdateCoordinator.class);
	private static final ObjectMapper MAPPER = new ObjectMapper();
	private static final ConcurrentHashMap<Path, ReentrantLock> JVM_LOCKS = new ConcurrentHashMap<>();
	private static final ConcurrentHashMap<Path, ReentrantLock> JVM_WORKER_LOCKS = new ConcurrentHashMap<>();

	private final Path directory;
	private final String storeIdentity;
	private final FtsIndexUpdateStrategy strategy;
	private final FtsIndexUpdateCoordinator.JournalCleaner journalCleaner;
	private final FileMove fileMove;
	private final ReentrantLock jvmLock;
	private final ReentrantLock jvmWorkerLock;
	private final long shutdownGraceMillis;
	private final ExecutorService worker = Executors.newSingleThreadExecutor(r -> {
		Thread thread = new Thread(r, "fts-index-outbox");
		thread.setDaemon(true);
		return thread;
	});
	private final AtomicBoolean workerScheduled = new AtomicBoolean();
	private final AtomicBoolean strategyClosed = new AtomicBoolean();
	private final AtomicLong deliveryFailureCount = new AtomicLong();
	private volatile long lastSuccessfulDeliveryMillis;
	private volatile String lastFailure;
	private volatile long lastRecoveryDurationMillis;
	private volatile String lastRecoveryError;
	private volatile boolean started;
	private volatile boolean closed;

	public FileFtsIndexUpdateCoordinator(Path directory, String storeIdentity, FtsIndexUpdateStrategy strategy,
			FtsIndexUpdateCoordinator.JournalCleaner journalCleaner) throws IOException {
		this(directory, storeIdentity, strategy, journalCleaner, Duration.ofSeconds(10));
	}

	public FileFtsIndexUpdateCoordinator(Path directory, String storeIdentity, FtsIndexUpdateStrategy strategy,
			FtsIndexUpdateCoordinator.JournalCleaner journalCleaner, Duration shutdownGracePeriod) throws IOException {
		this(directory, storeIdentity, strategy, journalCleaner, shutdownGracePeriod, Files::move);
	}

	FileFtsIndexUpdateCoordinator(Path directory, String storeIdentity, FtsIndexUpdateStrategy strategy,
			FtsIndexUpdateCoordinator.JournalCleaner journalCleaner, Duration shutdownGracePeriod,
			FileMove fileMove) throws IOException {
		if (directory == null || !directory.isAbsolute()) {
			throw new IOException("FTS outbox directory must be configured as an absolute persistent path");
		}
		if (shutdownGracePeriod == null || shutdownGracePeriod.isNegative()) {
			throw new IOException("FTS shutdown grace period must not be negative");
		}
		if (storeIdentity == null || storeIdentity.isBlank()) {
			throw new IOException("A stable FTS store identity must be configured");
		}
		Path normalizedDirectory = directory.normalize();
		this.storeIdentity = storeIdentity;
		this.strategy = strategy;
		this.journalCleaner = journalCleaner;
		this.shutdownGraceMillis = shutdownGracePeriod.toMillis();
		if (strategy == null || journalCleaner == null || fileMove == null) {
			throw new NullPointerException("FTS strategy, journal cleaner, and file mover are required");
		}
		this.fileMove = fileMove;
		Files.createDirectories(normalizedDirectory);
		this.directory = normalizedDirectory.toRealPath();
		this.jvmLock = JVM_LOCKS.computeIfAbsent(this.directory, ignored -> new ReentrantLock());
		this.jvmWorkerLock = JVM_WORKER_LOCKS.computeIfAbsent(this.directory, ignored -> new ReentrantLock());
		validateOwnership();
		validateDurabilitySupport();
	}

	@Override
	public CommitLock acquireCommitLock() throws Exception {
		return acquireCommitLock(false);
	}

	private CommitLock acquireCommitLock(boolean recovery) throws Exception {
		ensureOpen();
		jvmLock.lockInterruptibly();
		FileChannel channel = null;
		FileLock fileLock = null;
		try {
			channel = FileChannel.open(directory.resolve(".commit.lock"),
					StandardOpenOption.CREATE, StandardOpenOption.WRITE);
			fileLock = channel.lock();
			if (started && !recovery) {
				assertNoUnresolvedPreparedRecords();
			}
			return new LockedCommit(channel, fileLock);
		} catch (Exception e) {
			try {
				if (fileLock != null) {
					fileLock.release();
				}
				if (channel != null) {
					channel.close();
				}
			} catch (IOException closeError) {
				e.addSuppressed(closeError);
			} finally {
				jvmLock.unlock();
			}
			throw e;
		}
	}

	@Override
	public void prepare(String transactionId, long sequence, Iterable<FtsIndexOperation> operations)
			throws IOException {
		ensureOpen();
		validateTransactionId(transactionId);
		if (sequence <= 0) {
			throw new IOException("Invalid FTS transaction sequence: " + sequence);
		}
		Path staging = directory.resolve(transactionId + ".writing");
		Path prepared = directory.resolve(transactionId + ".prepared");
		try (BufferedWriter writer = Files.newBufferedWriter(staging, StandardCharsets.UTF_8,
				StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
			ObjectNode header = MAPPER.createObjectNode();
			header.put("version", 1);
			header.put("transactionId", transactionId);
			header.put("sequence", sequence);
			header.put("storeIdentity", storeIdentity);
			writer.write(MAPPER.writeValueAsString(header));
			writer.newLine();
			for (FtsIndexOperation operation : operations) {
				writer.write(MAPPER.writeValueAsString(encodeOperation(operation)));
				writer.newLine();
			}
		} catch (Exception e) {
			Files.deleteIfExists(staging);
			throw e instanceof IOException io ? io : new IOException("Unable to serialize FTS update", e);
		}
		forceFile(staging);
		fileMove.move(staging, prepared, StandardCopyOption.ATOMIC_MOVE);
		forceDirectory();
	}

	@Override
	public void activate(String transactionId) throws IOException {
		ensureOpen();
		Path prepared = directory.resolve(transactionId + ".prepared");
		long sequence = readSequence(prepared);
		Path pending = pendingPath(sequence, transactionId);
		fileMove.move(prepared, pending, StandardCopyOption.ATOMIC_MOVE);
		forceDirectory();
		signalWorker();
	}

	@Override
	public void rollback(String transactionId) throws IOException {
		ensureOpen();
		Files.deleteIfExists(directory.resolve(transactionId + ".prepared"));
		Files.deleteIfExists(directory.resolve(transactionId + ".writing"));
		forceDirectory();
	}

	@Override
	public synchronized void recover(CommittedTransactions committedTransactions) throws Exception {
		ensureOpen();
		long startedAt = System.nanoTime();
		try {
			// Global lock order: commit lock, worker lock, filesystem lock. The worker never takes
			// the commit lock, so recovery may wait for delivery without forming a lock cycle.
			try (CommitLock ignored = acquireCommitLock(true)) {
				recoverUnderCommitLock(committedTransactions);
			}
			lastRecoveryError = null;
			logger.info("FTS recovery completed store={} durationMs={}",
					storeIdentity, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt));
		} catch (Exception e) {
			lastRecoveryError = e.getMessage() == null ? e.getClass().getName() : e.getMessage();
			logger.error("FTS recovery failed store={} durationMs={} error={}",
					storeIdentity, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt),
					lastRecoveryError, e);
			throw e;
		} finally {
			lastRecoveryDurationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
		}
	}

	private void recoverUnderCommitLock(CommittedTransactions committedTransactions) throws Exception {
		jvmWorkerLock.lockInterruptibly();
		try (FileChannel channel = FileChannel.open(directory.resolve(".worker.lock"),
				StandardOpenOption.CREATE, StandardOpenOption.WRITE);
				FileLock ignored = channel.lock()) {
			recoverRecords(committedTransactions.read());
		} finally {
			jvmWorkerLock.unlock();
		}
	}

	private void recoverRecords(Set<String> committedTransactionIds) throws Exception {
		Set<String> known = new java.util.HashSet<>();
		Set<String> pendingIds = new java.util.HashSet<>();
		Set<String> doneIds = new java.util.HashSet<>();
		Set<Long> sequences = new HashSet<>();
		List<Path> preparedFiles = new ArrayList<>();
		Map<Path, Long> preparedSequences = new HashMap<>();
		List<Path> writingFiles = new ArrayList<>();
		long highestSequence = 0;
		try (var files = Files.list(directory)) {
			for (Path file : files.toList()) {
				String name = file.getFileName().toString();
				if (name.endsWith(".prepared")) {
					String transactionId = name.substring(0, name.length() - ".prepared".length());
					validateTransaction(file, transactionId);
					long sequence = readSequence(file);
					if (!sequences.add(sequence)) {
						throw new IOException("Duplicate FTS WAL sequence " + sequence + ": " + file);
					}
					highestSequence = Math.max(highestSequence, sequence);
					registerKnown(known, transactionId, file);
					preparedFiles.add(file);
					preparedSequences.put(file, sequence);
				} else if (name.endsWith(".pending") || name.endsWith(".done")) {
					String transactionId = transactionId(file);
					validateTransaction(file, transactionId);
					highestSequence = registerSequence(file, sequences, highestSequence);
					registerKnown(known, transactionId, file);
					if (name.endsWith(".pending")) {
						pendingIds.add(transactionId);
					} else {
						doneIds.add(transactionId);
					}
				} else if (name.endsWith(".writing")) {
					writingFiles.add(file);
				}
			}
		}
		for (String id : pendingIds) {
			if (!committedTransactionIds.contains(id)) {
				throw new IOException("Pending FTS WAL record has no committed RDF journal marker: " + id);
			}
		}
		for (String id : committedTransactionIds) {
			if (!known.contains(id)) {
				throw new IOException("Committed RDF transaction " + id + " has no FTS WAL record");
			}
		}
		readSequenceCounter();

		preparedFiles.sort(java.util.Comparator.comparingLong(preparedSequences::get));
		for (Path prepared : preparedFiles) {
			String name = prepared.getFileName().toString();
			String transactionId = name.substring(0, name.length() - ".prepared".length());
			if (committedTransactionIds.contains(transactionId)) {
				activateWithoutScheduling(transactionId, readSequence(prepared));
			} else {
				Files.deleteIfExists(prepared);
			}
		}
		for (Path writing : writingFiles) {
			Files.deleteIfExists(writing);
		}
		reconcileSequenceCounter(highestSequence);
		for (String id : doneIds) {
			Path done = findStateFile(id, ".done");
			if (committedTransactionIds.contains(id)) {
				journalCleaner.remove(id);
			}
			Files.deleteIfExists(done);
		}
		forceDirectory();
		started = true;
		signalWorker();
	}

	private void activateWithoutScheduling(String transactionId, long sequence) throws IOException {
		fileMove.move(directory.resolve(transactionId + ".prepared"), pendingPath(sequence, transactionId),
				StandardCopyOption.ATOMIC_MOVE);
	}

	private void readSequenceCounter() throws IOException {
		Path sequenceFile = directory.resolve(".sequence");
		if (!Files.exists(sequenceFile)) {
			return;
		}
		long current;
		try {
			current = Long.parseLong(Files.readString(sequenceFile, StandardCharsets.UTF_8));
		} catch (NumberFormatException e) {
			throw new IOException("Invalid FTS sequence counter", e);
		}
		if (current < 0) {
			throw new IOException("Invalid FTS sequence counter");
		}
	}

	@Override
	public void close() throws Exception {
		if (!closed) {
			closed = true;
			worker.shutdown();
		}
		boolean interrupted = Thread.interrupted();
		try {
			if (interrupted) {
				worker.shutdownNow();
			} else if (!worker.awaitTermination(shutdownGraceMillis, TimeUnit.MILLISECONDS)) {
				logger.warn("FTS outbox worker exceeded its {} ms shutdown grace period; interrupting delivery",
						shutdownGraceMillis);
				worker.shutdownNow();
				if (!worker.awaitTermination(shutdownGraceMillis, TimeUnit.MILLISECONDS)) {
					logger.error("FTS delivery is still active after cancellation; leaving strategy open. "
							+ "Unfinished WAL records remain available for recovery.");
				}
			}
		} catch (InterruptedException e) {
			interrupted = true;
			worker.shutdownNow();
		} finally {
			if (worker.isTerminated()) {
				closeStrategy();
			}
			if (interrupted || Thread.currentThread().isInterrupted()) {
				Thread.currentThread().interrupt();
			}
		}
	}

	private void closeStrategy() throws Exception {
		if (strategyClosed.compareAndSet(false, true)) {
			strategy.close();
		}
	}

	@Override
	public void awaitTermination() throws InterruptedException {
		boolean interrupted = false;
		try {
			while (!worker.isTerminated()) {
				try {
					worker.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
				} catch (InterruptedException e) {
					interrupted = true;
				}
			}
		} finally {
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
		}
	}

	@Override
	public FtsOutboxStatus status() throws IOException {
		int count = 0;
		int pendingCount = 0;
		long bytes = 0;
		long directoryBytes = 0;
		long oldestAgeMillis = 0;
		long oldestSequence = -1;
		long oldestModified = Long.MAX_VALUE;
		long now = System.currentTimeMillis();
		try (var files = Files.list(directory)) {
			for (Path path : files.toList()) {
				String name = path.getFileName().toString();
				try {
					long fileBytes = Files.size(path);
					if (Files.isRegularFile(path)) {
						directoryBytes = Math.addExact(directoryBytes, fileBytes);
					}
					if (!(name.endsWith(".prepared") || name.endsWith(".pending") || name.endsWith(".done"))) {
						continue;
					}
					long modified = Files.getLastModifiedTime(path).toMillis();
					long sequence = readSequence(path);
					count++;
					bytes = Math.addExact(bytes, fileBytes);
					if (name.endsWith(".pending")) {
						pendingCount++;
					}
					if (!name.endsWith(".done") && modified < oldestModified) {
						oldestModified = modified;
						oldestAgeMillis = Math.max(0, now - modified);
						oldestSequence = sequence;
					}
				} catch (NoSuchFileException ignored) {
					// The worker may have acknowledged and removed this record during the snapshot.
				}
			}
		}
		return new FtsOutboxStatus(count, pendingCount, bytes, directoryBytes, oldestAgeMillis, oldestSequence,
				deliveryFailureCount.get(), lastSuccessfulDeliveryMillis, lastFailure,
				lastRecoveryDurationMillis, lastRecoveryError);
	}

	private void signalWorker() {
		if (!started || closed || !workerScheduled.compareAndSet(false, true)) {
			return;
		}
		try {
			worker.execute(() -> {
			try {
				jvmWorkerLock.lockInterruptibly();
				try (FileChannel channel = FileChannel.open(directory.resolve(".worker.lock"),
						StandardOpenOption.CREATE, StandardOpenOption.WRITE);
						FileLock ignored = channel.lock()) {
					drain();
				} finally {
					jvmWorkerLock.unlock();
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} catch (IOException e) {
				logger.error("Unable to lock FTS outbox worker", e);
			} finally {
				workerScheduled.set(false);
				if (started && !closed) {
					try {
						if (nextPendingInOrder() != null) {
							signalWorker();
						}
					} catch (IOException e) {
						logger.error("Unable to inspect FTS outbox after delivery", e);
					}
				}
				if (closed) {
					try {
						closeStrategy();
					} catch (Exception e) {
						logger.error("Unable to close FTS update strategy after worker termination", e);
					}
				}
			}
			});
		} catch (RejectedExecutionException e) {
			workerScheduled.set(false);
			if (!closed) {
				throw e;
			}
		}
	}

	private void drain() {
		int retry = 0;
		String transactionId = null;
		long sequence = -1;
		while (!closed) {
			try {
				transactionId = null;
				sequence = -1;
				Path state = nextPendingInOrder();
				if (state == null) {
					return;
				}
				transactionId = transactionId(state);
				sequence = readSequence(state);
				if (state.getFileName().toString().endsWith(".done")) {
					journalCleaner.remove(transactionId);
					Files.deleteIfExists(state);
				} else {
					logger.debug("Applying FTS update store={} transaction={} sequence={}",
							storeIdentity, transactionId, sequence);
					strategy.apply(readUpdate(state));
					lastSuccessfulDeliveryMillis = System.currentTimeMillis();
					lastFailure = null;
					Path done = state.resolveSibling(state.getFileName().toString().replace(".pending", ".done"));
					fileMove.move(state, done, StandardCopyOption.ATOMIC_MOVE);
					forceDirectory();
				}
				forceDirectory();
				retry = 0;
			} catch (Exception e) {
				retry = Math.min(retry + 1, 8);
				deliveryFailureCount.incrementAndGet();
				lastFailure = e.getMessage() == null ? e.getClass().getName() : e.getMessage();
				FtsOutboxStatus status = null;
				try {
					status = status();
				} catch (IOException statusError) {
					e.addSuppressed(statusError);
				}
				if (retry == 1) {
					logger.error("FTS outbox delivery failed; retaining ordered WAL and retrying. "
									+ "store={} transaction={} sequence={} backlog={}",
							storeIdentity, transactionId, sequence, status, e);
				} else {
					logger.warn("FTS outbox retry {} failed; later updates remain blocked. "
									+ "store={} transaction={} sequence={} backlog={}: {}",
							retry, storeIdentity, transactionId, sequence, status, e.getMessage());
				}
				try {
					Thread.sleep(Math.min(60_000L, 250L << retry));
				} catch (InterruptedException interrupted) {
					Thread.currentThread().interrupt();
					return;
				}
			}
		}
	}

	private Path nextPendingInOrder() throws IOException {
		Path earliest = null;
		long lowestSequence = Long.MAX_VALUE;
		Set<Long> sequences = new java.util.HashSet<>();
		try (var files = Files.list(directory)) {
			var iterator = files.filter(path -> {
				String name = path.getFileName().toString();
				return name.endsWith(".pending") || name.endsWith(".prepared") || name.endsWith(".done");
			}).iterator();
			while (iterator.hasNext()) {
				Path state = iterator.next();
				long sequence = readSequence(state);
				if (!sequences.add(sequence)) {
					throw new IOException("Duplicate FTS WAL sequence " + sequence);
				}
				if (sequence < lowestSequence) {
					earliest = state;
					lowestSequence = sequence;
				}
			}
		}
		if (earliest == null || earliest.getFileName().toString().endsWith(".prepared")) {
			return null;
		}
		return earliest;
	}

	private FtsIndexUpdate readUpdate(Path path) throws IOException {
		try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
			JsonNode header = MAPPER.readTree(reader.readLine());
			String transactionId = header.path("transactionId").asText();
			long sequence = header.path("sequence").asLong(-1);
			if (!storeIdentity.equals(header.path("storeIdentity").asText()) || sequence <= 0
					|| !transactionId.equals(transactionId(path))) {
				throw new IOException("Invalid or foreign FTS WAL header: " + path);
			}
		}
		return new FtsIndexUpdate(transactionId(path), readSequence(path), () -> operationIterator(path));
	}

	private Iterator<FtsIndexOperation> operationIterator(Path path) {
		try {
			BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
			reader.readLine();
			return new Iterator<>() {
				private String nextLine = readLine();

				private String readLine() {
					try {
						return reader.readLine();
					} catch (IOException e) {
						try {
							reader.close();
						} catch (IOException closeError) {
							e.addSuppressed(closeError);
						}
						throw new IllegalStateException("Unable to read FTS WAL", e);
					}
				}

				@Override
				public boolean hasNext() {
					if (nextLine == null) {
						try {
							reader.close();
						} catch (IOException e) {
							throw new IllegalStateException("Unable to close FTS WAL", e);
						}
					}
					return nextLine != null;
				}

				@Override
				public FtsIndexOperation next() {
					if (!hasNext()) {
						throw new java.util.NoSuchElementException();
					}
					String current = nextLine;
					nextLine = readLine();
					try {
						return decodeOperation(MAPPER.readTree(current));
					} catch (IOException e) {
						throw new IllegalStateException("Invalid FTS WAL operation", e);
					}
				}
			};
		} catch (IOException e) {
			throw new IllegalStateException("Unable to open FTS WAL", e);
		}
	}

	private ObjectNode encodeOperation(FtsIndexOperation operation) throws IOException {
		ObjectNode result = MAPPER.createObjectNode();
		if (operation instanceof FtsIndexOperation.Clear) {
			result.put("kind", "clear");
		} else if (operation instanceof FtsIndexOperation.ClearContexts clear) {
			result.put("kind", "clearContexts");
			ArrayNode contexts = result.putArray("contexts");
			for (Resource context : clear.contexts()) {
				contexts.add(encodeResource(context));
			}
		} else if (operation instanceof FtsIndexOperation.Statements statements) {
			result.put("kind", "statements");
			ArrayNode added = result.putArray("added");
			ArrayNode removed = result.putArray("removed");
			for (Statement statement : statements.added()) {
				added.add(encodeStatement(statement));
			}
			for (Statement statement : statements.removed()) {
				removed.add(encodeStatement(statement));
			}
		} else {
			throw new IOException("Unsupported FTS operation: " + operation);
		}
		return result;
	}

	private FtsIndexOperation decodeOperation(JsonNode node) throws IOException {
		return switch (node.path("kind").asText()) {
			case "clear" -> new FtsIndexOperation.Clear();
			case "clearContexts" -> {
				if (!node.path("contexts").isArray()) {
					throw new IOException("Invalid clearContexts operation in FTS WAL");
				}
				List<Resource> contexts = new ArrayList<>();
				for (JsonNode context : node.path("contexts")) {
					contexts.add(decodeResource(context));
				}
				yield new FtsIndexOperation.ClearContexts(contexts);
			}
			case "statements" -> {
				if (!node.path("added").isArray() || !node.path("removed").isArray()) {
					throw new IOException("Invalid statements operation in FTS WAL");
				}
				Set<Statement> added = new java.util.LinkedHashSet<>();
				Set<Statement> removed = new java.util.LinkedHashSet<>();
				for (JsonNode statement : node.path("added")) {
					added.add(decodeStatement(statement));
				}
				for (JsonNode statement : node.path("removed")) {
					removed.add(decodeStatement(statement));
				}
				yield new FtsIndexOperation.Statements(added, removed);
			}
			default -> throw new IOException("Unknown FTS operation type: " + node.path("kind").asText());
		};
	}

	private ObjectNode encodeStatement(Statement statement) throws IOException {
		ObjectNode node = MAPPER.createObjectNode();
		node.set("subject", encodeResource(statement.getSubject()));
		node.put("predicate", statement.getPredicate().stringValue());
		node.set("object", encodeValue(statement.getObject()));
		if (statement.getContext() != null) {
			node.set("context", encodeResource(statement.getContext()));
		}
		return node;
	}

	private Statement decodeStatement(JsonNode node) throws IOException {
		if (!node.isObject() || !node.path("predicate").isTextual()) {
			throw new IOException("Invalid RDF statement in FTS WAL");
		}
		Resource subject = decodeResource(node.path("subject"));
		IRI predicate;
		try {
			predicate = SimpleValueFactory.getInstance().createIRI(node.path("predicate").asText());
		} catch (IllegalArgumentException e) {
			throw new IOException("Invalid predicate IRI in FTS WAL", e);
		}
		if (node.path("object").isMissingNode()) {
			throw new IOException("Missing RDF object in FTS WAL");
		}
		Value object = decodeValue(node.path("object"));
		Resource context = node.has("context") ? decodeResource(node.path("context")) : null;
		return SimpleValueFactory.getInstance().createStatement(subject, predicate, object, context);
	}

	private ObjectNode encodeResource(Resource resource) {
		ObjectNode node = MAPPER.createObjectNode();
		node.put("kind", resource instanceof BNode ? "bnode" : "iri");
		node.put("value", resource instanceof BNode bnode ? bnode.getID() : resource.stringValue());
		return node;
	}

	private Resource decodeResource(JsonNode node) throws IOException {
		if (!node.isObject() || !node.path("value").isTextual()) {
			throw new IOException("Invalid RDF resource in FTS WAL");
		}
		return switch (node.path("kind").asText()) {
			case "iri" -> {
				try {
					yield SimpleValueFactory.getInstance().createIRI(node.path("value").asText());
				} catch (IllegalArgumentException e) {
					throw new IOException("Invalid RDF resource IRI in FTS WAL", e);
				}
			}
			case "bnode" -> SimpleValueFactory.getInstance().createBNode(node.path("value").asText());
			default -> throw new IOException("Invalid RDF resource in FTS WAL");
		};
	}

	private ObjectNode encodeValue(Value value) throws IOException {
		ObjectNode node = MAPPER.createObjectNode();
		if (value instanceof IRI iri) {
			node.put("kind", "iri");
			node.put("value", iri.stringValue());
		} else if (value instanceof Literal literal) {
			node.put("kind", "literal");
			node.put("value", literal.getLabel());
			node.put("datatype", literal.getDatatype().stringValue());
			literal.getLanguage().ifPresent(language -> node.put("language", language));
		} else {
			throw new IOException("Unsupported RDF value in FTS WAL: " + value);
		}
		return node;
	}

	private Value decodeValue(JsonNode node) throws IOException {
		if (!node.isObject() || !node.path("value").isTextual()) {
			throw new IOException("Invalid RDF value in FTS WAL");
		}
		if ("iri".equals(node.path("kind").asText())) {
			try {
				return SimpleValueFactory.getInstance().createIRI(node.path("value").asText());
			} catch (IllegalArgumentException e) {
				throw new IOException("Invalid object IRI in FTS WAL", e);
			}
		}
		if ("literal".equals(node.path("kind").asText())) {
			if (node.has("language") && !node.path("language").isTextual()
					|| !node.has("language") && !node.path("datatype").isTextual()) {
				throw new IOException("Missing literal datatype in FTS WAL");
			}
			String language = node.path("language").asText(null);
			try {
				return language == null
						? SimpleValueFactory.getInstance().createLiteral(node.path("value").asText(),
								SimpleValueFactory.getInstance().createIRI(node.path("datatype").asText()))
						: SimpleValueFactory.getInstance().createLiteral(node.path("value").asText(), language);
			} catch (IllegalArgumentException e) {
				throw new IOException("Invalid literal in FTS WAL", e);
			}
		}
		throw new IOException("Invalid RDF value in FTS WAL");
	}

	private void validateTransaction(Path file, String transactionId) throws IOException {
		try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
			JsonNode header = MAPPER.readTree(reader.readLine());
			if (header == null || !header.isObject() || header.path("version").asInt(-1) != 1
					|| !transactionId.equals(header.path("transactionId").asText())
					|| !storeIdentity.equals(header.path("storeIdentity").asText())
					|| header.path("sequence").asLong(-1) <= 0) {
				throw new IOException("Invalid or foreign FTS WAL record: " + file);
			}
			String operation;
			while ((operation = reader.readLine()) != null) {
				JsonNode node = MAPPER.readTree(operation);
				if (node == null || !node.isObject()) {
					throw new IOException("Invalid FTS WAL operation in " + file);
				}
				decodeOperation(node);
			}
		}
	}

	private long readSequence(Path file) throws IOException {
		try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
			String line = reader.readLine();
			JsonNode header = line == null ? null : MAPPER.readTree(line);
			long sequence = header == null || !header.isObject()
					? -1
					: header.path("sequence").asLong(-1);
			if (sequence <= 0) {
				throw new IOException("Invalid FTS WAL sequence in " + file);
			}
			return sequence;
		}
	}

	private String transactionId(Path file) {
		String name = file.getFileName().toString();
		if (name.endsWith(".prepared")) {
			return name.substring(0, name.length() - ".prepared".length());
		}
		int separator = name.indexOf('-');
		if (separator <= 0) {
			throw new IllegalStateException("Invalid FTS WAL filename: " + name);
		}
		int suffix = name.lastIndexOf('.');
		return name.substring(separator + 1, suffix);
	}

	private Path pendingPath(long sequence, String transactionId) {
		return directory.resolve(String.format("%020d-%s.pending", sequence, transactionId));
	}

	private Path findStateFile(String transactionId, String suffix) throws IOException {
		try (var files = Files.list(directory)) {
			return files.filter(path -> path.getFileName().toString().endsWith(suffix))
					.filter(path -> transactionId(path).equals(transactionId))
					.findFirst().orElse(null);
		}
	}

	private void validateOwnership() throws IOException {
		Path owner = directory.resolve(".store-identity");
		if (Files.exists(owner)) {
			String recorded = Files.readString(owner, StandardCharsets.UTF_8);
			if (!storeIdentity.equals(recorded)) {
				throw new IOException("FTS outbox directory belongs to another store: " + directory);
			}
			return;
		}
		try (var files = Files.list(directory)) {
			if (files.findAny().isPresent()) {
				throw new IOException("FTS outbox has data but no ownership manifest: " + directory);
			}
		}
		try {
			Files.writeString(owner, storeIdentity, StandardCharsets.UTF_8,
					StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
		} catch (java.nio.file.FileAlreadyExistsException race) {
			if (!storeIdentity.equals(Files.readString(owner, StandardCharsets.UTF_8))) {
				throw new IOException("FTS outbox directory belongs to another store: " + directory, race);
			}
			return;
		}
		forceFile(owner);
		forceDirectory();
	}

	private void validateDurabilitySupport() throws IOException {
		Path source = directory.resolve(".durability-" + UUID.randomUUID() + ".tmp");
		Path target = source.resolveSibling(source.getFileName() + ".moved");
		try {
			Files.writeString(source, "fts", StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW,
					StandardOpenOption.WRITE);
			forceFile(source);
			Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
			forceDirectory();
		} finally {
			Files.deleteIfExists(source);
			Files.deleteIfExists(target);
			forceDirectory();
		}
	}

	private void registerKnown(Set<String> known, String transactionId, Path file) throws IOException {
		if (!known.add(transactionId)) {
			throw new IOException("Duplicate FTS WAL records for transaction " + transactionId + ": " + file);
		}
	}

	private long registerSequence(Path file, Set<Long> sequences, long highestSequence) throws IOException {
		long sequence = readSequence(file);
		if (!sequences.add(sequence)) {
			throw new IOException("Duplicate FTS WAL sequence " + sequence + ": " + file);
		}
		return Math.max(highestSequence, sequence);
	}

	private void reconcileSequenceCounter(long highestSequence) throws IOException {
		Path sequenceFile = directory.resolve(".sequence");
		long current = 0;
		if (Files.exists(sequenceFile)) {
			try {
				current = Long.parseLong(Files.readString(sequenceFile, StandardCharsets.UTF_8));
			} catch (NumberFormatException e) {
				throw new IOException("Invalid FTS sequence counter", e);
			}
			if (current < 0) {
				throw new IOException("Invalid FTS sequence counter");
			}
		}
		if (current < highestSequence) {
			writeSequenceCounter(highestSequence);
		}
	}

	private void assertNoUnresolvedPreparedRecords() throws IOException {
		try (var files = Files.list(directory)) {
			Path unresolved = files.filter(path -> path.getFileName().toString().endsWith(".prepared"))
					.findFirst().orElse(null);
			if (unresolved != null) {
				throw new IOException("FTS WAL requires recovery before accepting later commits: " + unresolved);
			}
		}
	}

	private long nextSequence() throws IOException {
		Path file = directory.resolve(".sequence");
		long current;
		try {
			current = Files.exists(file) ? Long.parseLong(Files.readString(file, StandardCharsets.UTF_8)) : 0L;
		} catch (NumberFormatException e) {
			throw new IOException("Invalid FTS sequence counter", e);
		}
		if (current < 0) {
			throw new IOException("Invalid FTS sequence counter");
		}
		long next = Math.addExact(current, 1L);
		writeSequenceCounter(next);
		return next;
	}

	private void writeSequenceCounter(long sequence) throws IOException {
		Path temp = directory.resolve(".sequence-" + UUID.randomUUID() + ".tmp");
		Files.writeString(temp, Long.toString(sequence), StandardCharsets.UTF_8,
				StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
		forceFile(temp);
		Files.move(temp, directory.resolve(".sequence"),
				StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
		forceDirectory();
	}

	private void forceFile(Path file) throws IOException {
		try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
			channel.force(true);
		}
	}

	private void forceDirectory() throws IOException {
		try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
			channel.force(true);
		}
	}

	private static void validateTransactionId(String transactionId) throws IOException {
		if (transactionId == null || !transactionId.matches("[A-Za-z0-9._-]+")) {
			throw new IOException("Invalid FTS transaction ID");
		}
	}

	private void ensureOpen() throws IOException {
		if (closed) {
			throw new IOException("FTS update coordinator is closed");
		}
	}

	private final class LockedCommit implements CommitLock {
		private final FileChannel channel;
		private final FileLock lock;
		private boolean released;

		private LockedCommit(FileChannel channel, FileLock lock) {
			this.channel = channel;
			this.lock = lock;
		}

		@Override
		public long nextSequence() throws IOException {
			if (released) {
				throw new IOException("FTS commit lock is closed");
			}
			return FileFtsIndexUpdateCoordinator.this.nextSequence();
		}

		@Override
		public void close() throws IOException {
			if (released) {
				return;
			}
			released = true;
			IOException failure = null;
			try {
				lock.release();
			} catch (IOException e) {
				failure = e;
			}
			try {
				channel.close();
			} catch (IOException e) {
				if (failure == null) {
					failure = e;
				} else {
					failure.addSuppressed(e);
				}
			} finally {
				jvmLock.unlock();
			}
			if (failure != null) {
				throw failure;
			}
		}
	}

	@FunctionalInterface
	interface FileMove {
		Path move(Path source, Path target, CopyOption... options) throws IOException;
	}

}
