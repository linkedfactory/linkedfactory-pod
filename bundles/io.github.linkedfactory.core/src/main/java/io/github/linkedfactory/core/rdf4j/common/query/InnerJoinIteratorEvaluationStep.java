package io.github.linkedfactory.core.rdf4j.common.query;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryEvaluationStep;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

public class InnerJoinIteratorEvaluationStep implements BatchQueryEvaluationStep {
	EvaluationStrategy strategy;
	Supplier<ExecutorService> executorService;
	QueryEvaluationStep leftPrepared;
	QueryEvaluationStep rightPrepared;
	boolean lateral;
	boolean async;

	public InnerJoinIteratorEvaluationStep(
			EvaluationStrategy strategy, Supplier<ExecutorService> executorService,
			QueryEvaluationStep leftPrepared, QueryEvaluationStep rightPrepared, boolean lateral, boolean async) {
		this.strategy = strategy;
		this.executorService = executorService;
		this.leftPrepared = leftPrepared;
		this.rightPrepared = rightPrepared;
		this.lateral = lateral;
		this.async = async;
	}

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(BindingSet bindingSet) {
		return new InnerJoinIterator(strategy, executorService,
				leftPrepared, rightPrepared, List.of(bindingSet), lateral, async
		);
	}

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(List<BindingSet> bindingSets) {
		return BatchQueryEvaluationStep.super.evaluate(bindingSets);
	}
}
