package io.github.linkedfactory.core.rdf4j.common.query;

import java.util.NoSuchElementException;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.common.iteration.LookAheadIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryEvaluationStep;

public class InnerJoinIterator extends LookAheadIteration<BindingSet, QueryEvaluationException> {

	/*-----------*
	 * Variables *
	 *-----------*/

	private final EvaluationStrategy strategy;

	private final CloseableIteration<BindingSet, QueryEvaluationException> leftIter;
	private final QueryEvaluationStep preparedJoinArg;
	private volatile CloseableIteration<BindingSet, QueryEvaluationException> rightIter;

	/*--------------*
	 * Constructors *
	 *--------------*/

	public InnerJoinIterator(EvaluationStrategy strategy, QueryEvaluationStep leftPrepared,
	                         QueryEvaluationStep rightPrepared, BindingSet bindings, boolean lateral) throws QueryEvaluationException {
		this.strategy = strategy;

		CloseableIteration<BindingSet, QueryEvaluationException> leftIt = leftPrepared.evaluate(bindings);
		if (leftIt.hasNext() || lateral) {
			preparedJoinArg = rightPrepared;
		} else {
			leftIt.close();
			leftIt = rightPrepared.evaluate(bindings);
			preparedJoinArg = leftPrepared;
		}
		rightIter = new EmptyIteration<>();
		leftIter = leftIt;
	}

	/*---------*
	 * Methods *
	 *---------*/

	@Override
	protected BindingSet getNextElement() throws QueryEvaluationException {
		try {
			while (rightIter.hasNext() || leftIter.hasNext()) {
				if (rightIter.hasNext()) {
					return rightIter.next();
				}

				// Right iteration exhausted
				rightIter.close();

				if (leftIter.hasNext()) {
					rightIter = preparedJoinArg.evaluate(leftIter.next());
				}
			}
		} catch (NoSuchElementException ignore) {
			// probably, one of the iterations has been closed concurrently in
			// handleClose()
		}

		return null;
	}

	@Override
	protected void handleClose() throws QueryEvaluationException {
		super.handleClose();

		leftIter.close();
		rightIter.close();
	}
}