package io.github.linkedfactory.core.rdf4j.fts;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class FtsAsyncFlushWorker implements AutoCloseable {
	private static final Logger logger = LoggerFactory.getLogger(FtsAsyncFlushWorker.class);

	public static final String PROP_OUTBOX_DIR = "fts.outboxDir";
	public static final String PROP_MAX_ATTEMPTS = "fts.maxAttempts";
	public static final String PROP_RETRY_BACKOFF_MS = "fts.retryBackoffMillis";
	public static final String PROP_MAX_STALENESS_MS = "fts.maxStalenessMillis";
	public static final String PROP_FLUSH_INTERVAL_MS = "fts.flushIntervalMillis";
	public static final String PROP_BATCH_SIZE = "fts.batchSize";

	private static final int DEFAULT_MAX_ATTEMPTS = 5;
	private static final long DEFAULT_RETRY_BACKOFF_MS = 100L;
	private static final long DEFAULT_MAX_STALENESS_MS = 60_000L;
	private static final long DEFAULT_FLUSH_INTERVAL_MS = 1000L;
	private static final int DEFAULT_BATCH_SIZE = 100;

	private final FtsSearchBackend backend;
	private final FtsOutbox outbox;
	private final ScheduledExecutorService scheduler;
	private final ExecutorService workerPool;
	private final AtomicBoolean running = new AtomicBoolean(false);

	private final int batchSize;
	private final Duration retryBackoff;
	private final Duration maxStaleness;

	public FtsAsyncFlushWorker(FtsSearchBackend backend, Path outboxDir) {
		this(backend, new FtsOutbox(outboxDir, DEFAULT_MAX_ATTEMPTS, DEFAULT_RETRY_BACKOFF_MS, DEFAULT_MAX_STALENESS_MS));
	}

	public FtsAsyncFlushWorker(FtsSearchBackend backend, FtsOutbox outbox) {
		this(backend, outbox, DEFAULT_BATCH_SIZE, outbox.retryBackoffMillis(), DEFAULT_MAX_STALENESS_MS, DEFAULT_FLUSH_INTERVAL_MS);
	}

	public FtsAsyncFlushWorker(FtsSearchBackend backend, FtsOutbox outbox, int batchSize, long retryBackoffMs, long maxStalenessMs, long flushIntervalMs) {
		this.backend = backend;
		this.outbox = outbox;
		this.batchSize = batchSize;
		this.retryBackoff = Duration.ofMillis(retryBackoffMs);
		this.maxStaleness = Duration.ofMillis(maxStalenessMs);

		int nThreads = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
		this.workerPool = Executors.newFixedThreadPool(nThreads, r -> {
			Thread t = new Thread(r, "fts-async-flush-worker");
			t.setDaemon(true);
			return t;
		});

		this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
			Thread t = new Thread(r, "fts-async-flush-scheduler");
			t.setDaemon(true);
			return t;
		});
	}

	public void start() {
		if (!running.compareAndSet(false, true)) {
			logger.warn("FtsAsyncFlushWorker already started");
			return;
		}

		replayUnapplied();
		scheduler.scheduleWithFixedDelay(this::flushBatch, 0, DEFAULT_FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);

		logger.info("FtsAsyncFlushWorker started with batch size={}, retry backoff={}ms, max staleness={}ms", 
			batchSize, retryBackoff.toMillis(), maxStaleness.toMillis());
	}

	public void shutdown() {
		if (!running.compareAndSet(true, false)) {
			logger.warn("FtsAsyncFlushWorker already stopped");
			return;
		}

		scheduler.shutdown();
		workerPool.shutdown();

		try {
			if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
				scheduler.shutdownNow();
			}
			if (!workerPool.awaitTermination(5, TimeUnit.SECONDS)) {
				workerPool.shutdownNow();
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			workerPool.shutdownNow();
		}

		try {
			outbox.close();
		} catch (Exception e) {
			logger.warn("Failed to close outbox", e);
		}

		logger.info("FtsAsyncFlushWorker stopped");
	}

	private void replayUnapplied() {
		List<FtsOutbox.FtsMutation> pending = outbox.getPendingOrFailedMutations();
		if (pending.isEmpty()) {
			logger.info("No pending mutations to replay");
			return;
		}

		logger.info("Replaying {} pending mutations from outbox", pending.size());

		for (int i = 0; i < pending.size(); i += batchSize) {
			int end = Math.min(i + batchSize, pending.size());
			List<FtsOutbox.FtsMutation> batch = pending.subList(i, end);
			applyBatch(batch, true);
		}

		outbox.clearApplied();
	}

	private void flushBatch() {
		if (!running.get()) {
			return;
		}

		try {
			List<FtsOutbox.FtsMutation> pending = outbox.getPendingOrFailedMutations();
			if (pending.isEmpty()) {
				return;
			}

			logger.debug("Processing {} pending mutations", pending.size());

			for (int i = 0; i < pending.size(); i += batchSize) {
				int end = Math.min(i + batchSize, pending.size());
				List<FtsOutbox.FtsMutation> batch = pending.subList(i, end);
				applyBatch(batch, false);
			}

			outbox.clearApplied();

		} catch (Exception e) {
			logger.error("Failed to flush FTS batch", e);
		}
	}

	private void applyBatch(List<FtsOutbox.FtsMutation> batch, boolean isReplay) {
		if (batch.isEmpty()) {
			return;
		}

		Map<String, SubjectBatch> batchBySubject = new HashMap<>();
		for (FtsOutbox.FtsMutation mutation : batch) {
			String subject = mutation.subject();
			if (subject == null) {
				applyNonSubjectMutation(mutation, isReplay);
				continue;
			}

			batchBySubject.computeIfAbsent(subject, k -> new SubjectBatch()).add(mutation);
		}

		for (SubjectBatch subjectBatch : batchBySubject.values()) {
			applySubjectBatch(subjectBatch, isReplay);
		}
	}

	private void applyNonSubjectMutation(FtsOutbox.FtsMutation mutation, boolean isReplay) {
		if ("clear".equals(mutation.operationType())) {
			applyClear(mutation, isReplay);
		} else if ("clearContext".equals(mutation.operationType())) {
			applyClearContext(mutation, isReplay);
		}
	}

	private void applySubjectBatch(SubjectBatch batch, boolean isReplay) {
		Set<FtsOutbox.FtsMutation> toAdd = batch.getToAdd();
		Set<FtsOutbox.FtsMutation> toRemove = batch.getToRemove();

		Set<Statement> addedStatements = toAdd.stream()
			.map(this::toStatement)
			.filter(s -> s != null)
			.collect(Collectors.toSet());

		Set<Statement> removedStatements = toRemove.stream()
			.map(this::toStatement)
			.filter(s -> s != null)
			.collect(Collectors.toSet());

		if (addedStatements.isEmpty() && removedStatements.isEmpty()) {
			batch.getMutations().forEach(m -> outbox.markApplied(m.id()));
			return;
		}

		boolean success = applyWithRetry(addedStatements, removedStatements, isReplay);

		if (success) {
			batch.getMutations().forEach(m -> outbox.markApplied(m.id()));
		} else {
			batch.getMutations().forEach(m -> outbox.markFailed(m.id()));
		}
	}

	private Statement toStatement(FtsOutbox.FtsMutation m) {
		ValueFactory vf = SimpleValueFactory.getInstance();
		Resource subject = vf.createIRI(m.subject());
		IRI predicate = vf.createIRI(m.predicate());

		Value object;
		if ("iri".equals(m.objectType())) {
			object = vf.createIRI(m.object());
		} else {
			if (m.language() != null) {
				object = m.datatype() != null 
					? vf.createLiteral(m.object(), vf.createIRI(m.datatype()))
					: vf.createLiteral(m.object(), m.language());
			} else {
				object = m.datatype() != null
					? vf.createLiteral(m.object(), vf.createIRI(m.datatype()))
					: vf.createLiteral(m.object());
			}
		}

		Resource context = m.context() != null ? vf.createIRI(m.context()) : null;
		return vf.createStatement(subject, predicate, object, context);
	}

	private boolean applyWithRetry(Set<Statement> added, Set<Statement> removed, boolean isReplay) {
		int attempt = 0;
		long currentBackoff = retryBackoff.toMillis();

		while (attempt < outbox.maxAttempts()) {
			attempt++;
			try {
				backend.addRemoveStatements(added, removed);
				return true;
			} catch (Exception e) {
				logger.warn("FTS backend error (attempt {}/{}): {}", attempt, outbox.maxAttempts(), e.getMessage());
				
				if (attempt >= outbox.maxAttempts()) {
					logger.error("FTS backend failed after {} attempts", outbox.maxAttempts());
					return false;
				}

				try {
					Thread.sleep(currentBackoff + (long)(Math.random() * currentBackoff));
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
					return false;
				}
				currentBackoff *= 2;
			}
		}
		return false;
	}

	private void applyClear(FtsOutbox.FtsMutation mutation, boolean isReplay) {
		boolean success = applyBackendOperation(() -> {
			backend.clear();
			return null;
		}, mutation, isReplay);

		if (success) {
			outbox.markApplied(mutation.id());
		} else {
			outbox.markFailed(mutation.id());
		}
	}

	private void applyClearContext(FtsOutbox.FtsMutation mutation, boolean isReplay) {
		List<Resource> contexts = new ArrayList<>();
		String payload = mutation.payload();
		int start = payload.indexOf("\"contexts\":[");
		if (start >= 0) {
			int end = payload.indexOf(']', start);
			if (end >= 0) {
				String contextsStr = payload.substring(start + 12, end);
				for (String ctx : contextsStr.split(",")) {
					ctx = ctx.trim().replace("\"", "");
					if (!ctx.isEmpty()) {
						contexts.add(SimpleValueFactory.getInstance().createIRI(ctx));
					}
				}
			}
		}

		boolean success = applyBackendOperation(() -> {
			backend.clearContexts(contexts.toArray(new Resource[0]));
			return null;
		}, mutation, isReplay);

		if (success) {
			outbox.markApplied(mutation.id());
		} else {
			outbox.markFailed(mutation.id());
		}
	}

	private <T> T applyBackendOperation(BackendOperation<T> op, FtsOutbox.FtsMutation mutation, boolean isReplay) {
		int attempt = 0;
		long currentBackoff = retryBackoff.toMillis();

		while (attempt < outbox.maxAttempts()) {
			attempt++;
			try {
				T result = op.execute();
				return result;
			} catch (Exception e) {
				logger.warn("FTS backend error for {} (attempt {}/{}): {}", 
					mutation.operationType(), attempt, outbox.maxAttempts(), e.getMessage());

				if (attempt >= outbox.maxAttempts()) {
					logger.error("FTS backend failed after {} attempts for mutation {}", 
						outbox.maxAttempts(), mutation.id());
					return null;
				}

				try {
					Thread.sleep(currentBackoff + (long)(Math.random() * currentBackoff));
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
					return null;
				}
				currentBackoff *= 2;
			}
		}
		return null;
	}

	@FunctionalInterface
	private interface BackendOperation<T> {
		T execute() throws Exception;
	}

	private static class SubjectBatch {
		private final List<FtsOutbox.FtsMutation> mutations = new ArrayList<>();
		private final Map<String, FtsOutbox.FtsMutation> adds = new HashMap<>();
		private final Map<String, FtsOutbox.FtsMutation> removes = new HashMap<>();

		void add(FtsOutbox.FtsMutation mutation) {
			mutations.add(mutation);
			String field = mutation.predicate();
			if ("add".equals(mutation.operationType())) {
				adds.put(field, mutation);
			} else if ("remove".equals(mutation.operationType())) {
				removes.put(field, mutation);
			}
		}

		Set<FtsOutbox.FtsMutation> getToAdd() {
			return Set.copyOf(adds.values());
		}

		Set<FtsOutbox.FtsMutation> getToRemove() {
			return Set.copyOf(removes.values());
		}

		List<FtsOutbox.FtsMutation> getMutations() {
			return mutations;
		}
	}

	@Override
	public void close() {
		shutdown();
	}

	public long getPendingCount() {
		return outbox.getPendingCount();
	}

	public long getFailedCount() {
		return outbox.getFailedCount();
	}

	public long getDeadLetterCount() {
		return outbox.getDeadLetterCount();
	}

	public List<FtsOutbox.FtsMutation> getDeadLetterMutations() {
		return outbox.getDeadLetterMutations();
	}
}
