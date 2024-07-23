package io.github.linkedfactory.core.rdf4j.kvin.query;

import io.github.linkedfactory.core.rdf4j.kvin.KvinEvaluationStrategy;
import io.github.linkedfactory.core.rdf4j.kvin.KvinEvaluationUtil;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryEvaluationStep;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryEvaluationContext;

public class KvinFetchEvaluationStep implements QueryEvaluationStep {

    protected final KvinEvaluationStrategy strategy;
    protected final KvinFetch fetch;
    protected final KvinEvaluationUtil evalUtil;
    protected final QueryEvaluationContext context;

    public KvinFetchEvaluationStep(KvinEvaluationStrategy strategy, KvinFetch fetch, QueryEvaluationContext context) {
        this.strategy = strategy;
        this.fetch = fetch;
        this.context = context;
        this.evalUtil = new KvinEvaluationUtil(strategy.getKvin());
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(BindingSet bs) {
        return evalUtil
            .evaluate(strategy.getValueFactory(), bs, fetch.params, fetch.getStatement(), context.getDataset());
    }
}