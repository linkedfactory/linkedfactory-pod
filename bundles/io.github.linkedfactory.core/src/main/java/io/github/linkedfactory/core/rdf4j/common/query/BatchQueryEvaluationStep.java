package io.github.linkedfactory.core.rdf4j.common.query;

import org.eclipse.rdf4j.common.iteration.AbstractCloseableIteration;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryEvaluationStep;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public interface BatchQueryEvaluationStep extends QueryEvaluationStep {
	static CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
			QueryEvaluationStep step, List<BindingSet> bindingSets) {
		if (step instanceof BatchQueryEvaluationStep) {
			return ((BatchQueryEvaluationStep) step).evaluate(bindingSets);
		}
		return evaluateSerial(step, bindingSets);
	}

	static CloseableIteration<BindingSet, QueryEvaluationException> evaluateSerial(
			QueryEvaluationStep step, List<BindingSet> bindingSets) {
		Iterator<BindingSet> bindingSetIterator = bindingSets.iterator();
		return new AbstractCloseableIteration<>() {
			CloseableIteration<BindingSet, QueryEvaluationException> it;

			@Override
			public boolean hasNext() throws QueryEvaluationException {
				if (it == null || !it.hasNext()) {
					while (bindingSetIterator.hasNext()) {
						var bs  = bindingSetIterator.next();
						it = step.evaluate(bs);
						if (it.hasNext()) {
							break;
						} else {
							it.close();
						}
					}
				}
				return it != null && it.hasNext();
			}

			@Override
			public BindingSet next() throws QueryEvaluationException {
				if (hasNext()) {
					return it.next();
				} else if (it != null) {
					it.close();
					it = null;
				}
				throw new NoSuchElementException();
			}

			@Override
			public void remove() throws QueryEvaluationException {
				throw new UnsupportedOperationException("Removal is not supported");
			}

			@Override
			protected void handleClose() throws QueryEvaluationException {
				super.handleClose();
				if (it != null) {
					it.close();
					it = null;
				}
			}
		};
	}

	default CloseableIteration<BindingSet, QueryEvaluationException> evaluate(List<BindingSet> bindingSets) {
		return evaluateSerial(this, bindingSets);
	}
}
