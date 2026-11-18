package io.github.linkedfactory.core.rdf4j.fts;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;

import java.util.List;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public sealed interface FtsIndexOperation {
	record Statements(Set<Statement> added, Set<Statement> removed) implements FtsIndexOperation {
		public Statements {
			added = Collections.unmodifiableSet(new LinkedHashSet<>(added));
			removed = Collections.unmodifiableSet(new LinkedHashSet<>(removed));
		}
	}

	record ClearContexts(List<Resource> contexts) implements FtsIndexOperation {
		public ClearContexts {
			contexts = List.copyOf(contexts);
		}
	}

	record Clear() implements FtsIndexOperation {
	}
}
