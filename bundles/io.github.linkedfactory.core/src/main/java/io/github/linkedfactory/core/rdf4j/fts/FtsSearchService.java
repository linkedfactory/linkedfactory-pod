package io.github.linkedfactory.core.rdf4j.fts;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;

import java.util.Set;

/**
 * Receives transaction-scoped FTS changes in RDF transaction order. Implementations
 * must preserve operation order across bounded batches. {@link FtsSailConnection}
 * calls {@link #prepareCommit(String)} before committing RDF and {@link #commit(String)}
 * afterwards; recovery uses the RDF transaction journal to decide which staged
 * transactions are eligible for replay.
 */
@Deprecated
public interface FtsSearchService extends FtsIndexUpdateStrategy {
	FtsSearchService NOOP = new FtsSearchService() {
		@Override
		public void prepareCommit(String transactionId) {
		}

		@Override
		public void preservePreparedTransaction(String transactionId) {
		}

		@Override
		public void recoverCommittedTransactions(Set<String> committedTransactionIds) {
		}
	};

	default void begin() throws Exception {
	}

	/**
	 * Receives bounded, transaction-ordered batches. Implementations must preserve
	 * call order so spilled batches have the same effect as one in-memory batch.
	 */
	default void addRemoveStatements(Set<Statement> added, Set<Statement> removed) throws Exception {
	}

	default void clearContexts(Resource... contexts) throws Exception {
	}

	default void clear() throws Exception {
	}

	/**
	 * Durably stages the current transaction before RDF commits. The staged work
	 * must remain recoverable until commit or rollback resolves the transaction.
	 */
	default void prepareCommit(String transactionId) throws Exception {
		throw new UnsupportedOperationException(
				"FTS search service must implement transaction staging before it can be used with FtsSail");
	}

	/**
	 * Retains staged work if the RDF store reports an uncertain commit outcome.
	 * Recovery will use the durable RDF journal to commit or discard that work.
	 */
	default void preservePreparedTransaction(String transactionId) throws Exception {
		throw new UnsupportedOperationException(
				"FTS search service cannot preserve work after an uncertain RDF commit");
	}

	default void commit() throws Exception {
	}

	/**
	 * Activates a transaction staged by {@link #prepareCommit(String)}.
	 */
	default void commit(String transactionId) throws Exception {
		commit();
	}

	/** Replays committed transactions and discards staged transactions without markers. */
	default void recoverCommittedTransactions(Set<String> committedTransactionIds) throws Exception {
		if (!committedTransactionIds.isEmpty()) {
			throw new UnsupportedOperationException(
					"Search service cannot recover committed FTS transactions: " + committedTransactionIds);
		}
	}

	default void rollback() throws Exception {
	}

	@Override
	default void apply(FtsIndexUpdate update) throws FtsIndexUpdateException {
		try {
			begin();
			for (FtsIndexOperation operation : update.operations()) {
				if (operation instanceof FtsIndexOperation.Statements statements) {
					addRemoveStatements(statements.added(), statements.removed());
				} else if (operation instanceof FtsIndexOperation.ClearContexts clear) {
					clearContexts(clear.contexts().toArray(Resource[]::new));
				} else if (operation instanceof FtsIndexOperation.Clear) {
					clear();
				}
			}
			commit();
		} catch (Exception e) {
			try {
				rollback();
			} catch (Exception rollbackError) {
				e.addSuppressed(rollbackError);
			}
			throw new FtsIndexUpdateException("Unable to apply FTS update " + update.transactionId(), e);
		}
	}
}
