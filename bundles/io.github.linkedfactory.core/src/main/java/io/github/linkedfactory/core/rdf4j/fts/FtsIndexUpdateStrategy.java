package io.github.linkedfactory.core.rdf4j.fts;

public interface FtsIndexUpdateStrategy extends AutoCloseable {
	/**
	 * Applies one committed logical update. The coordinator owns transaction markers,
	 * durable storage, retries, ordering, and recovery; this method must be idempotent
	 * because delivery can be repeated after an uncertain backend acknowledgement.
	 * Coordinators may invoke a strategy asynchronously or synchronously according
	 * to the selected delivery policy.
	 */
	void apply(FtsIndexUpdate update) throws FtsIndexUpdateException;

	@Override
	default void close() throws Exception {
	}
}
