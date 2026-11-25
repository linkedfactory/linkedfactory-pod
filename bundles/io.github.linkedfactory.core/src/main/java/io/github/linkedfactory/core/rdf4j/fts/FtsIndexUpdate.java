package io.github.linkedfactory.core.rdf4j.fts;

import java.util.Objects;

public record FtsIndexUpdate(String transactionId, long sequence, Iterable<FtsIndexOperation> operations) {
	public FtsIndexUpdate {
		Objects.requireNonNull(transactionId, "transactionId");
		Objects.requireNonNull(operations, "operations");
		if (sequence < 0) {
			throw new IllegalArgumentException("sequence must not be negative");
		}
	}
}
