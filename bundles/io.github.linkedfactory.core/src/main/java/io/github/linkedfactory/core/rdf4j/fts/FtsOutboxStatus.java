package io.github.linkedfactory.core.rdf4j.fts;

/** Snapshot of durable FTS records awaiting acknowledgement or cleanup. */
public record FtsOutboxStatus(int recordCount, int pendingRecordCount, long bytes, long directoryBytes,
		long oldestAgeMillis, long oldestSequence, long deliveryFailureCount, long lastSuccessfulDeliveryMillis,
		String lastFailure, long recoveryDurationMillis, String lastRecoveryError) {
	public FtsOutboxStatus(int recordCount, long bytes, long oldestAgeMillis, long oldestSequence,
			long deliveryFailureCount, long lastSuccessfulDeliveryMillis, String lastFailure) {
		this(recordCount, recordCount, bytes, bytes, oldestAgeMillis, oldestSequence,
				deliveryFailureCount, lastSuccessfulDeliveryMillis, lastFailure, 0, null);
	}

	public FtsOutboxStatus(int recordCount, long bytes, long oldestAgeMillis, long oldestSequence) {
		this(recordCount, recordCount, bytes, bytes, oldestAgeMillis, oldestSequence, 0, 0, null, 0, null);
	}
}
