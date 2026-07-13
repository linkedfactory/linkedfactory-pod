package io.github.linkedfactory.core.rdf4j.fts;

import io.github.linkedfactory.core.rdf4j.common.query.CompositeBindingSet;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.DistinctIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.common.iteration.SingletonIteration;
import org.eclipse.rdf4j.common.iteration.UnionIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.query.algebra.evaluation.util.QueryEvaluationUtil;
import org.eclipse.rdf4j.query.algebra.helpers.collectors.StatementPatternCollector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class FtsFederatedService implements FederatedService {
	private static final ValueFactory VF = SimpleValueFactory.getInstance();

	private final FtsSearchBackend backend;
	private volatile boolean initialized = false;

	public FtsFederatedService(FtsSearchBackend backend) {
		this.backend = backend;
	}

	@Override
	public boolean ask(Service service, BindingSet bindings, String baseUri) throws QueryEvaluationException {
		final CloseableIteration<BindingSet, QueryEvaluationException> iter = evaluate(service,
				new SingletonIteration<>(bindings), baseUri);
		try {
			while (iter.hasNext()) {
				BindingSet bs = iter.next();
				String firstVar = service.getBindingNames().iterator().next();
				return QueryEvaluationUtil.getEffectiveBooleanValue(bs.getValue(firstVar));
			}
		} finally {
			iter.close();
		}
		return false;
	}

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(Service service,
			CloseableIteration<BindingSet, QueryEvaluationException> bindings, String baseUri) throws QueryEvaluationException {
		if (!bindings.hasNext()) {
			return new EmptyIteration<>();
		}

		FtsPattern pattern = extractPattern(service);
		List<CloseableIteration<BindingSet, QueryEvaluationException>> resultIters = new ArrayList<>();
		while (bindings.hasNext()) {
			BindingSet input = bindings.next();
			FtsSearchRequest request = pattern.toRequest(input);
			if (request == null) {
				continue;
			}

			List<FtsSearchHit> hits;
			try {
				hits = backend.search(request);
			} catch (Exception e) {
				throw new QueryEvaluationException(e);
			}

			List<BindingSet> rows = new ArrayList<>(hits.size());
			for (FtsSearchHit hit : hits) {
				QueryBindingSet row = new QueryBindingSet(input);
				IRI iriValue = VF.createIRI(hit.getIri());

				if (pattern.subjectVar.hasValue() && !pattern.subjectVar.getValue().equals(iriValue)) {
					continue;
				}
				if (!pattern.subjectVar.hasValue()) {
					Value existing = input.getValue(pattern.subjectVar.getName());
					if (existing != null && !existing.equals(iriValue)) {
						continue;
					}
					row.setBinding(pattern.subjectVar.getName(), iriValue);
				}

				if (pattern.scoreVar != null && !pattern.scoreVar.hasValue() && hit.getScore() != null) {
					row.setBinding(pattern.scoreVar.getName(), VF.createLiteral(hit.getScore()));
				}
				if (pattern.snippetVar != null && !pattern.snippetVar.hasValue() && hit.getSnippet() != null) {
					row.setBinding(pattern.snippetVar.getName(), VF.createLiteral(hit.getSnippet()));
				}
				rows.add(row);
			}
			resultIters.add(toIteration(rows));
		}

		if (resultIters.isEmpty()) {
			return new EmptyIteration<>();
		}
		return resultIters.size() > 1 ? new DistinctIteration<>(new UnionIteration<>(resultIters)) : resultIters.get(0);
	}

	@Override
	public void initialize() throws QueryEvaluationException {
		initialized = true;
	}

	@Override
	public boolean isInitialized() {
		return initialized;
	}

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> select(Service service, Set<String> projectionVars,
			BindingSet bindings, String baseUri) throws QueryEvaluationException {
		final CloseableIteration<BindingSet, QueryEvaluationException> iter = evaluate(service,
				new SingletonIteration<>(bindings), baseUri);
		if (service.getBindingNames().equals(projectionVars)) {
			return iter;
		}

		return new CloseableIteration<>() {
			@Override
			public boolean hasNext() throws QueryEvaluationException {
				return iter.hasNext();
			}

			@Override
			public BindingSet next() throws QueryEvaluationException {
				CompositeBindingSet projected = new CompositeBindingSet(bindings);
				BindingSet result = iter.next();
				for (String var : projectionVars) {
					Value v = result.getValue(var);
					projected.addBinding(var, v);
				}
				return projected;
			}

			@Override
			public void remove() throws QueryEvaluationException {
				iter.remove();
			}

			@Override
			public void close() throws QueryEvaluationException {
				iter.close();
			}
		};
	}

	@Override
	public void shutdown() throws QueryEvaluationException {
		try {
			backend.close();
		} catch (Exception e) {
			throw new QueryEvaluationException(e);
		}
	}

	private FtsPattern extractPattern(Service service) throws QueryEvaluationException {
		StatementPattern keywords = null;
		StatementPattern score = null;
		StatementPattern snippet = null;
		StatementPattern field = null;
		StatementPattern limit = null;
		StatementPattern boost = null;

		for (StatementPattern stmt : StatementPatternCollector.process(service.getArg())) {
			Value predicate = stmt.getPredicateVar().getValue();
			if (FTS.KEYWORDS.equals(predicate)) {
				keywords = stmt;
			}
		}
		if (keywords == null) {
			throw new QueryEvaluationException("SERVICE <fts:> must include ?iri fts:keywords ...");
		}

		for (StatementPattern stmt : StatementPatternCollector.process(service.getArg())) {
			if (!sameSubject(keywords.getSubjectVar(), stmt.getSubjectVar())) {
				continue;
			}
			Value predicate = stmt.getPredicateVar().getValue();
			if (FTS.SCORE.equals(predicate)) {
				score = stmt;
			} else if (FTS.SNIPPET.equals(predicate)) {
				snippet = stmt;
			} else if (FTS.FIELD.equals(predicate)) {
				field = stmt;
			} else if (FTS.LIMIT.equals(predicate)) {
				limit = stmt;
			} else if (FTS.BOOST.equals(predicate)) {
				boost = stmt;
			}
		}
		return new FtsPattern(keywords.getSubjectVar(), keywords.getObjectVar(),
				score == null ? null : score.getObjectVar(),
				snippet == null ? null : snippet.getObjectVar(),
				field == null ? null : field.getObjectVar(),
				limit == null ? null : limit.getObjectVar(),
				boost == null ? null : boost.getObjectVar());
	}

	private boolean sameSubject(Var a, Var b) {
		if (a.hasValue() || b.hasValue()) {
			return a.hasValue() && b.hasValue() && a.getValue().equals(b.getValue());
		}
		return a.getName().equals(b.getName());
	}

	private static CloseableIteration<BindingSet, QueryEvaluationException> toIteration(List<BindingSet> results) {
		return new CloseableIteration<>() {
			private final Iterator<BindingSet> iterator = results.iterator();

			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public BindingSet next() {
				return iterator.next();
			}

			@Override
			public void remove() {
				iterator.remove();
			}

			@Override
			public void close() {
			}
		};
	}

	private static final class FtsPattern {
		private final Var subjectVar;
		private final Var keywordsVar;
		private final Var scoreVar;
		private final Var snippetVar;
		private final Var fieldVar;
		private final Var limitVar;
		private final Var boostVar;

		private FtsPattern(Var subjectVar, Var keywordsVar, Var scoreVar, Var snippetVar, Var fieldVar, Var limitVar, Var boostVar) {
			this.subjectVar = subjectVar;
			this.keywordsVar = keywordsVar;
			this.scoreVar = scoreVar;
			this.snippetVar = snippetVar;
			this.fieldVar = fieldVar;
			this.limitVar = limitVar;
			this.boostVar = boostVar;
		}

		private FtsSearchRequest toRequest(BindingSet input) throws QueryEvaluationException {
			Value keywordsValue = resolveValue(keywordsVar, input);
			if (keywordsValue == null) {
				return null;
			}
			String keywords;
			if (keywordsValue instanceof Literal) {
				keywords = ((Literal) keywordsValue).getLabel();
			} else {
				keywords = keywordsValue.stringValue();
			}
			if (keywords == null || keywords.isBlank()) {
				return null;
			}

			String field = null;
			Value fieldValue = resolveValue(fieldVar, input);
			if (fieldValue != null) {
				field = fieldValue.stringValue();
			}

			int limit = 0;
			Value limitValue = resolveValue(limitVar, input);
			if (limitValue instanceof Literal) {
				try {
					limit = ((Literal) limitValue).intValue();
				} catch (NumberFormatException e) {
					throw new QueryEvaluationException("Invalid fts:limit value: " + limitValue, e);
				}
			}

			Double boost = null;
			Value boostValue = resolveValue(boostVar, input);
			if (boostValue instanceof Literal) {
				try {
					boost = ((Literal) boostValue).doubleValue();
				} catch (NumberFormatException e) {
					throw new QueryEvaluationException("Invalid fts:boost value: " + boostValue, e);
				}
			}

			String iriFilter = null;
			Value subjectValue = resolveValue(subjectVar, input);
			if (subjectValue instanceof IRI) {
				iriFilter = subjectValue.stringValue();
			}

			return new FtsSearchRequest(keywords, field, limit, boost, snippetVar != null, iriFilter);
		}

		private Value resolveValue(Var var, BindingSet input) {
			if (var == null) {
				return null;
			}
			if (var.hasValue()) {
				return var.getValue();
			}
			return input.getValue(var.getName());
		}
	}
}
