package io.github.linkedfactory.core.kvin.records;

/**
 * A KVIN value with numeric IDs. Records are ordered by context, item, and property,
 * then by descending time and sequence number.
 */
public record KvinRecord(long contextId, long itemId, long propertyId, long time, int seqNr, Object value) implements Comparable<KvinRecord> {
	public KvinRecord() {
		this(-1, -1, -1, 0, 0, null);
	}

	@Override
	public int compareTo(KvinRecord o) {
		long diffL = contextId - o.contextId;
		if (diffL != 0) {
			return diffL > 0 ? 1 : -1;
		}
		diffL = itemId - o.itemId;
		if (diffL != 0) {
			return diffL > 0 ? 1 : -1;
		}
		diffL = propertyId - o.propertyId;
		if (diffL != 0) {
			return diffL > 0 ? 1 : -1;
		}
		diffL = time - o.time;
		if (diffL != 0) {
			// time is reverse
			return -diffL > 0 ? 1 : -1;
		}
		int diff = seqNr - o.seqNr;
		if (diff != 0) {
			// seqNr is reverse
			return -diff;
		}
		return 0;
	}
}
