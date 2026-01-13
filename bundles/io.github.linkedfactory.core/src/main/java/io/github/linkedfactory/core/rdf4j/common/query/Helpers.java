package io.github.linkedfactory.core.rdf4j.common.query;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.common.iteration.SingletonIteration;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.DefaultEvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.helpers.TupleExprs;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

public class Helpers {
	public static CloseableIteration<BindingSet, QueryEvaluationException> compareAndBind(BindingSet bs, Var variable, Value valueToBind) {
		Value varValue = DefaultEvaluationStrategy.getVarValue(variable, bs);
		if (varValue == null) {
			CompositeBindingSet newBs = new CompositeBindingSet(bs);
			newBs.addBinding(variable.getName(), valueToBind);
			return new SingletonIteration<>(newBs);
		} else if (varValue.equals(valueToBind)) {
			return new SingletonIteration<>(bs);
		}
		return new EmptyIteration<>();
	}

	public static Fetch findFirstFetch(TupleExpr t) {
		TupleExpr n = t;
		Deque<TupleExpr> queue = null;
		do {
			if (n instanceof Fetch) {
				return (Fetch) n;
			}

			if (n instanceof Service) {
				return null;
			}

			List<TupleExpr> children = TupleExprs.getChildren(n);
			if (!children.isEmpty()) {
				if (queue == null) {
					queue = new ArrayDeque<>();
				}
				queue.addAll(children);
			}
			n = queue != null ? queue.poll() : null;
		} while (n != null);
		return null;
	}
}
