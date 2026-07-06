package io.github.linkedfactory.core.rdf4j.fts;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;

import java.util.Set;

public interface FtsSearchService {
	FtsSearchService NOOP = new FtsSearchService() {
	};

	default void begin() throws Exception {
	}

	default void addRemoveStatements(Set<Statement> added, Set<Statement> removed) throws Exception {
	}

	default void clearContexts(Resource... contexts) throws Exception {
	}

	default void clear() throws Exception {
	}

	default void commit() throws Exception {
	}

	default void rollback() throws Exception {
	}
}
