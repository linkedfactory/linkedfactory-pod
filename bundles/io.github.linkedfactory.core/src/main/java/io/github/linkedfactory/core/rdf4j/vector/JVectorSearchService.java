package io.github.linkedfactory.core.rdf4j.vector;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class JVectorSearchService implements VectorSearchService {
	private final JVectorIndex index;
	private final ThreadLocal<Transaction> transaction = new ThreadLocal<>();

	public JVectorSearchService(JVectorIndex index) {
		this.index = index;
	}

	@Override
	public void begin() {
		transaction.set(new Transaction());
	}

	@Override
	public void upsert(String subjectIri, float[] vector) {
		transaction.get().upserts.put(subjectIri, vector);
		transaction.get().removals.remove(subjectIri);
	}

	@Override
	public void remove(String subjectIri) {
		transaction.get().removals.add(subjectIri);
		transaction.get().upserts.remove(subjectIri);
	}

	@Override
	public void clear() {
		transaction.get().clearAll = true;
		transaction.get().upserts.clear();
		transaction.get().removals.clear();
	}

	@Override
	public void commit() {
		Transaction t = transaction.get();
		if (t.clearAll) {
			index.clear();
		}
		for (String removed : t.removals) {
			index.remove(removed);
		}
		for (Map.Entry<String, float[]> entry : t.upserts.entrySet()) {
			index.upsert(entry.getKey(), entry.getValue());
		}
		transaction.remove();
	}

	@Override
	public void rollback() {
		transaction.remove();
	}

	private static final class Transaction {
		final Map<String, float[]> upserts = new LinkedHashMap<>();
		final Set<String> removals = new LinkedHashSet<>();
		boolean clearAll;
	}
}
