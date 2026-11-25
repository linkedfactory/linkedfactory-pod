package io.github.linkedfactory.core.kvin.iceberg;

/**
 * One row of the KVIN value table with primitive key fields and its value
 * (the first non-null value column, still undecoded for {@code valueObject}).
 */
record KvinRow(long itemId, long contextId, long propertyId, long time, int seqNr, Object value) {
}
