package io.github.linkedfactory.core.rdf4j.aas.query;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryEvaluationStep;

public class AasFetchEvaluationStep implements QueryEvaluationStep {

	protected final AasEvaluationStrategy strategy;
	protected final AasFetch fetch;

	public AasFetchEvaluationStep(AasEvaluationStrategy strategy, AasFetch fetch) {
		this.strategy = strategy;
		this.fetch = fetch;
	}

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(BindingSet bs) {
		final Value subjectValue = AasEvaluationStrategy.getVarValue(fetch.getStatement().getSubjectVar(), bs);
		if (subjectValue != null && subjectValue.stringValue().startsWith("aas-api:")) {
			// this is an API call
			return strategy.evaluateFetch(bs, fetch.params, fetch.getStatement());
		}
		// this is a prioritized call that may need to access a remote API
		// the API access is handled by the evaluation strategy
		return strategy.evaluate(fetch.getStatement(), bs);
	}
}