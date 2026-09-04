package io.github.linkedfactory.core.rdf4j.fts;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;

import java.util.Set;

public interface FtsSearchService {
	FtsSearchService NOOP = new FtsSearchService() {
	};

	default void begin() throws Exception {
	}

	/*
	  a single add/remove to preserve transaction semantics and reduce index churn.
      This lets the search backend resolve net effects atomically (e.g., statement removed then re-added in same tx cancels out), avoids intermediate inconsistent states, and is much more efficient than many tiny calls. It mirrors RDF4J LuceneSail’s pattern for exactly that reason.
	 */
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
