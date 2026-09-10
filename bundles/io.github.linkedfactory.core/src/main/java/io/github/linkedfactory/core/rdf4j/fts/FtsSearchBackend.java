package io.github.linkedfactory.core.rdf4j.fts;

import java.util.List;

public interface FtsSearchBackend extends AutoCloseable {
	List<FtsSearchHit> search(FtsSearchRequest request) throws Exception;

	@Override
	default void close() throws Exception {
	}
}
