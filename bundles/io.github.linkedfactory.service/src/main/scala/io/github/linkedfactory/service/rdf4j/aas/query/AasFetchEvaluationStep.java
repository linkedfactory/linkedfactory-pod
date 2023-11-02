package io.github.linkedfactory.service.rdf4j.aas.query;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
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
        return new AasEvaluationUtil(strategy.getAasClient()).evaluate(strategy.getValueFactory(), bs,
                fetch.params, fetch.getStatement());
    }
}