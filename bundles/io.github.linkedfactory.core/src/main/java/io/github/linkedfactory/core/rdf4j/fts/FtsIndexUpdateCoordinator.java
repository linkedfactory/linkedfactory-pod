package io.github.linkedfactory.core.rdf4j.fts;

import java.util.Set;

/**
 * Coordinates durable transaction handoff and ordered strategy delivery.
 * Implementations own WAL persistence, acknowledgement, retries, and recovery;
 * backend strategies only apply idempotent logical updates. Delivery policy is a
 * coordinator concern and may be asynchronous for remote indexes or synchronous
 * for local indexes.
 */
public interface FtsIndexUpdateCoordinator extends AutoCloseable {
	CommitLock acquireCommitLock() throws Exception;

	void prepare(String transactionId, long sequence, Iterable<FtsIndexOperation> operations) throws Exception;

	void activate(String transactionId) throws Exception;

	void rollback(String transactionId) throws Exception;

	void recover(CommittedTransactions committedTransactions) throws Exception;

	default void recover(Set<String> committedTransactionIds) throws Exception {
		recover(() -> committedTransactionIds);
	}

	FtsOutboxStatus status() throws Exception;

	/**
	 * Waits until this coordinator can no longer invoke its strategy or journal cleaner.
	 * Call after {@link #close()} before shutting down resources used by either callback.
	 */
	void awaitTermination() throws Exception;

	@FunctionalInterface
	interface JournalCleaner {
		void remove(String transactionId) throws Exception;
	}

	@FunctionalInterface
	interface CommittedTransactions {
		Set<String> read() throws Exception;
	}

	interface CommitLock extends AutoCloseable {
		long nextSequence() throws Exception;

		@Override
		void close() throws Exception;
	}

	@Override
	void close() throws Exception;
}
