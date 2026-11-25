package io.github.linkedfactory.core.rdf4j.fts;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LuceneFtsSearchService implements FtsSearchService {
	private final LuceneFtsIndex index;
	private final String registryKey;

	public LuceneFtsSearchService(LuceneFtsIndex index) {
		this(index, null);
	}

	public LuceneFtsSearchService(LuceneFtsIndex index, String registryKey) {
		this.index = index;
		this.registryKey = registryKey;
	}

	@Override
	public void addRemoveStatements(Set<Statement> added, Set<Statement> removed) throws IOException {
		Map<String, List<LuceneFtsIndex.FieldValue>> addedEntries = new LinkedHashMap<>();
		Map<String, List<LuceneFtsIndex.FieldValue>> removedEntries = new LinkedHashMap<>();
		for (Statement statement : added) {
			append(addedEntries, statement);
		}
		for (Statement statement : removed) {
			append(removedEntries, statement);
		}
		index.applyAddRemove(addedEntries, removedEntries);
	}

	@Override
	public void clearContexts(Resource... contexts) throws IOException {
		if (contexts == null) {
			return;
		}
		Set<String> contextValues = new LinkedHashSet<>();
		for (Resource context : contexts) {
			if (context != null) {
				contextValues.add(context.stringValue());
			}
		}
		index.clearContexts(contextValues);
	}

	@Override
	public void clear() throws IOException {
		index.clearAll();
	}

	@Override
	public void commit() throws IOException {
		index.commit();
	}

	@Override
	public void close() throws IOException {
		if (registryKey != null) {
			LuceneFtsIndexRegistry.release(registryKey);
		}
	}

	private static void append(Map<String, List<LuceneFtsIndex.FieldValue>> target, Statement statement) {
		Value object = statement.getObject();
		boolean iri = object instanceof IRI;
		if (!iri && !(object instanceof Literal)) {
			return;
		}
		String value;
		String language = null;
		String datatype = null;
		if (iri) {
			value = object.stringValue();
		} else {
			Literal literal = (Literal) object;
			value = literal.getLabel();
			language = literal.getLanguage().orElse(null);
			datatype = literal.getDatatype() == null ? null : literal.getDatatype().stringValue();
		}
		String context = statement.getContext() == null ? null : statement.getContext().stringValue();
		LuceneFtsIndex.FieldValue entry = new LuceneFtsIndex.FieldValue(
				statement.getPredicate().stringValue(), value, iri, context, language, datatype);
		target.computeIfAbsent(statement.getSubject().stringValue(), key -> new ArrayList<>()).add(entry);
	}
}
