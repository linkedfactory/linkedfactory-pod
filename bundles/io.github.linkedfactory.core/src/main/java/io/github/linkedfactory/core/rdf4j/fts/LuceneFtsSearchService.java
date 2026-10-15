package io.github.linkedfactory.core.rdf4j.fts;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LuceneFtsSearchService implements FtsSearchService {
	private final LuceneFtsIndex index;
	private final ThreadLocal<Transaction> txState = new ThreadLocal<>();

	public LuceneFtsSearchService(LuceneFtsIndex index) {
		this.index = index;
	}

	@Override
	public void begin() {
		txState.set(new Transaction());
	}

	@Override
	public void addRemoveStatements(Set<Statement> added, Set<Statement> removed) {
		Transaction tx = state();
		for (Statement statement : added) {
			tx.add(statement);
		}
		for (Statement statement : removed) {
			tx.remove(statement);
		}
	}

	@Override
	public void clearContexts(Resource... contexts) {
		state().clearContexts(contexts);
	}

	@Override
	public void clear() {
		state().clearAll();
	}

	@Override
	public void commit() throws Exception {
		Transaction tx = txState.get();
		txState.remove();
		if (tx == null) {
			return;
		}
		if (tx.clearAll) {
			index.clearAll();
		}
		if (!tx.clearedContexts.isEmpty()) {
			index.clearContexts(tx.clearedContexts);
		}
		index.applyAddRemove(tx.added, tx.removed);
		index.commit();
	}

	@Override
	public void rollback() {
		txState.remove();
	}

	private Transaction state() {
		Transaction tx = txState.get();
		if (tx == null) {
			tx = new Transaction();
			txState.set(tx);
		}
		return tx;
	}

	private static final class Transaction {
		private final Map<String, List<LuceneFtsIndex.FieldValue>> added = new LinkedHashMap<>();
		private final Map<String, List<LuceneFtsIndex.FieldValue>> removed = new LinkedHashMap<>();
		private final Set<String> clearedContexts = new LinkedHashSet<>();
		private boolean clearAll;

		void add(Statement statement) {
			append(added, statement);
		}

		void remove(Statement statement) {
			append(removed, statement);
		}

		void clearAll() {
			clearAll = true;
			added.clear();
			removed.clear();
			clearedContexts.clear();
		}

		void clearContexts(Resource... contexts) {
			if (contexts == null) {
				return;
			}
			for (Resource context : contexts) {
				if (context != null) {
					clearedContexts.add(context.stringValue());
				}
			}
		}

		private void append(Map<String, List<LuceneFtsIndex.FieldValue>> target, Statement statement) {
			Value object = statement.getObject();
			boolean iri = object instanceof IRI;
			if (!iri && !(object instanceof Literal)) {
				return;
			}
			String value = iri ? object.stringValue() : ((Literal) object).getLabel();
			String context = statement.getContext() == null ? null : statement.getContext().stringValue();
			LuceneFtsIndex.FieldValue entry = new LuceneFtsIndex.FieldValue(
					statement.getPredicate().stringValue(), value, iri, context);
			target.computeIfAbsent(statement.getSubject().stringValue(), key -> new ArrayList<>()).add(entry);
		}
	}
}
