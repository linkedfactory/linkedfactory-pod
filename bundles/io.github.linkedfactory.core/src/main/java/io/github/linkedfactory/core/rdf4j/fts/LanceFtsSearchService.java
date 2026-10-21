package io.github.linkedfactory.core.rdf4j.fts;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class LanceFtsSearchService implements FtsSearchService {
	private final LanceFtsIndex index;
	private final ThreadLocal<Transaction> txState = new ThreadLocal<>();

	public LanceFtsSearchService(LanceFtsIndex index) {
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
		private final List<LanceFtsIndex.FieldValue> added = new ArrayList<>();
		private final List<LanceFtsIndex.FieldValue> removed = new ArrayList<>();
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

		private void append(List<LanceFtsIndex.FieldValue> target, Statement statement) {
			Value object = statement.getObject();
			boolean iri = object instanceof IRI;
			if (!iri && !(object instanceof Literal)) {
				return;
			}
			String value = iri ? object.stringValue() : ((Literal) object).getLabel();
			String context = statement.getContext() == null ? null : statement.getContext().stringValue();
			target.add(new LanceFtsIndex.FieldValue(
					statement.getSubject().stringValue(), statement.getPredicate().stringValue(), value, iri, context));
		}
	}
}
