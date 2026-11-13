package io.github.linkedfactory.core.rdf4j.vector;

public interface VectorSearchService {
	default void begin() throws Exception {
	}

	default void upsert(String subjectIri, float[] vector) throws Exception {
	}

	default void remove(String subjectIri) throws Exception {
	}

	default void clear() throws Exception {
	}

	default void commit() throws Exception {
	}

	default void rollback() throws Exception {
	}
}
