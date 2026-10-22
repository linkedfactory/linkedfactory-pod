package io.github.linkedfactory.core.rdf4j.fts;

import org.eclipse.rdf4j.model.Statement;

import java.util.List;
import java.util.Set;

public interface FtsSearchBackend extends FtsSearchService, AutoCloseable {
	List<FtsSearchHit> search(FtsSearchRequest request) throws Exception;

	/**
	 * Add and remove statements atomically.
	 * The backend should compute the net effect (remove statements that were added and then removed).
	 */
	default void addRemoveStatements(Set<Statement> added, Set<Statement> removed) throws Exception {
	}

	default void clearContexts(org.eclipse.rdf4j.model.Resource... contexts) throws Exception {
	}

	default void clear() throws Exception {
	}

	@Override
	default void close() throws Exception {
	}
}
