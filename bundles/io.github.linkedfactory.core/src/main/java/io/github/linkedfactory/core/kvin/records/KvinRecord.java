package io.github.linkedfactory.core.kvin.records;

public record KvinRecord(long itemId, long contextId, long propertyId, long time, int seqNr, Object value) implements Comparable<KvinRecord> {
	public KvinRecord() {
		this(-1, -1, -1, 0, 0, null);
	}

	@Override
	public int compareTo(KvinRecord o) {
		long diffL = itemId - o.itemId;
		if (diffL != 0) {
			return diffL > 0 ? 1 : -1;
		}
		diffL = contextId - o.contextId;
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
