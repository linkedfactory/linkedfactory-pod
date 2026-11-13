package io.github.linkedfactory.core.rdf4j.vector;

import java.util.List;

public interface VectorSearchBackend extends AutoCloseable {
	List<VectorSearchHit> search(VectorSearchRequest request) throws Exception;

	@Override
	default void close() throws Exception {
	}
}
