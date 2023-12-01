package io.github.linkedfactory.core.rdf4j.kvin.query;

import io.github.linkedfactory.core.rdf4j.kvin.KvinEvaluationStrategy;
import io.github.linkedfactory.core.rdf4j.kvin.KvinEvaluationUtil;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryEvaluationStep;

public class KvinFetchEvaluationStep implements QueryEvaluationStep {

    protected final KvinEvaluationStrategy strategy;
    protected final KvinFetch fetch;
    protected final KvinEvaluationUtil evalUtil;

    public KvinFetchEvaluationStep(KvinEvaluationStrategy strategy, KvinFetch fetch) {
        this.strategy = strategy;
        this.fetch = fetch;
        this.evalUtil = new KvinEvaluationUtil(strategy.getKvin());
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(BindingSet bs) {
        return evalUtil
            .evaluate(strategy.getValueFactory(), bs, fetch.params, fetch.getStatement());
    }
}