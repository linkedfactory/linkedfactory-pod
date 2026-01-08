/*******************************************************************************
 * Copyright (c) 2023 Eclipse RDF4J contributors.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 ******************************************************************************/

package io.github.linkedfactory.core.rdf4j.kvin.query;

import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;

import io.github.linkedfactory.core.rdf4j.common.query.AsyncIterator;
import io.github.linkedfactory.core.rdf4j.common.query.InnerJoinIterator;
import org.eclipse.rdf4j.common.annotation.Experimental;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MutableBindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryEvaluationStep;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryEvaluationContext;

/**
 * @author Håvard M. Ottestad
 */
@Experimental
public class MergeJoinIterator<V> implements CloseableIteration<BindingSet, QueryEvaluationException> {

	private final PeekMarkIterator<BindingSet> leftIterator;
	private final PeekMarkIterator<BindingSet> rightIterator;
	private final Comparator<V> cmp;
	private final Function<BindingSet, V> valueFunction;
	private final QueryEvaluationContext context;
	// -1 for unset, 0 for equal, 1 for different
	int currentLeftValueAndPeekEquals = -1;
	private BindingSet next;
	private BindingSet currentLeft;
	private V currentLeftValue;
	private V leftPeekValue;
	private boolean closed = false;
	private final boolean leftOuter; // new: emit left rows even when no matching right

	MergeJoinIterator(PeekMarkIterator<BindingSet> leftIterator, PeekMarkIterator<BindingSet> rightIterator, Comparator<V> cmp, Function<BindingSet, V> valueFunction, QueryEvaluationContext context, boolean leftOuter) throws QueryEvaluationException {

		this.leftIterator = leftIterator;
		this.rightIterator = rightIterator;
		this.cmp = cmp;
		this.valueFunction = valueFunction;
		this.context = context;
		this.leftOuter = leftOuter;
	}

	public static <V> CloseableIteration<BindingSet, QueryEvaluationException> getInstance(QueryEvaluationStep leftPrepared, QueryEvaluationStep preparedRight, BindingSet bindings, Comparator<V> cmp, Function<BindingSet, V> value, QueryEvaluationContext context, Supplier<ExecutorService> executorService) {
		return getInstance(leftPrepared, preparedRight, bindings, cmp, value, context, executorService, false);
	}

	public static <V> CloseableIteration<BindingSet, QueryEvaluationException> getInstance(QueryEvaluationStep leftPrepared, QueryEvaluationStep preparedRight, BindingSet bindings, Comparator<V> cmp, Function<BindingSet, V> value, QueryEvaluationContext context, Supplier<ExecutorService> executorService, boolean leftOuter) {
		CloseableIteration<BindingSet, QueryEvaluationException> leftIter = leftPrepared.evaluate(bindings);
		if (leftIter == QueryEvaluationStep.EMPTY_ITERATION) {
			return leftIter;
		}

		CloseableIteration<BindingSet, QueryEvaluationException> rightIter;
		if (InnerJoinIterator.asyncDepth.get() == null || InnerJoinIterator.asyncDepth.get() < InnerJoinIterator.MAX_ASYNC_DEPTH) {
			rightIter = new AsyncIterator<>(() -> preparedRight.evaluate(bindings), executorService);
		} else {
			rightIter = preparedRight.evaluate(bindings);
			if (rightIter == QueryEvaluationStep.EMPTY_ITERATION) {
				return rightIter;
			}
		}

		return new MergeJoinIterator<>(new PeekMarkIterator<>(leftIter), new PeekMarkIterator<>(rightIter), cmp, value, context, leftOuter);
	}

	private MutableBindingSet createBindingSet(BindingSet base) {
		return context.createBindingSet(base);
		// return new MutableCompositeBindingSet(base);
	}

	private BindingSet join(BindingSet left, BindingSet right, boolean createNewBindingSet) {
		// allow right to be null which means "no bindings from right" (left-outer case)
		if (right == null) {
			if (!createNewBindingSet && left instanceof MutableBindingSet) {
				return left;
			} else {
				return createBindingSet(left);
			}
		}

		MutableBindingSet joined;
		if (!createNewBindingSet && left instanceof MutableBindingSet) {
			joined = (MutableBindingSet) left;
		} else {
			joined = createBindingSet(left);
		}

		for (Binding binding : right) {
			if (!joined.hasBinding(binding.getName())) {
				joined.addBinding(binding);
			}
		}
		return joined;
	}

	private void calculateNext() {
		if (next != null) {
			return;
		}

		if (currentLeft == null && leftIterator.hasNext()) {
			currentLeft = leftIterator.next();
			currentLeftValue = null;
			leftPeekValue = null;
			currentLeftValueAndPeekEquals = -1;
		}

		if (currentLeft == null) {
			return;
		}

		loop();

	}

	private void loop() {
		while (next == null) {
			if (rightIterator.hasNext()) {
				BindingSet peekRight = rightIterator.peek();

				if (currentLeftValue == null) {
					currentLeftValue = valueFunction.apply(currentLeft);
					leftPeekValue = null;
					currentLeftValueAndPeekEquals = -1;
				}

				int compare = compare(currentLeftValue, valueFunction.apply(peekRight));

				if (compare == 0) {
					equal();
					return;
				} else if (compare < 0) {
					// leftIterator is behind, or in other words, rightIterator is ahead
					if (leftOuter) {
						// emit left row even though there's no matching right
						next = join(currentLeft, null, true);
						if (leftIterator.hasNext()) {
							// advance left for subsequent processing (maintain duplicate handling)
							lessThan();
						} else {
							// no more left rows, close after delivering this result
							close();
						}
						return;
					} else {
						if (leftIterator.hasNext()) {
							lessThan();
						} else {
							close();
							return;
						}
					}
				} else {
					// rightIterator is behind, skip forward
					rightIterator.next();

				}

			} else if (rightIterator.isResettable() && leftIterator.hasNext()) {
				rightIterator.reset();
				currentLeft = leftIterator.next();
				currentLeftValue = null;
				leftPeekValue = null;
				currentLeftValueAndPeekEquals = -1;
			} else {
				// if leftOuter is true we may still need to emit remaining left rows that have no matching right
				if (leftOuter && currentLeft != null) {
					// right is exhausted and not resettable; emit currentLeft and then advance through remaining left rows
					next = join(currentLeft, null, true);
					if (leftIterator.hasNext()) {
						lessThan();
					} else {
						close();
					}
					return;
				}
				close();
				return;
			}

		}
	}

	private int compare(V left, V right) {
		int compareTo;

		if (left == right) {
			compareTo = 0;
		} else {
			compareTo = cmp.compare(left, right);
		}
		return compareTo;
	}

	private void lessThan() {
		V oldLeftValue = currentLeftValue;
		currentLeft = leftIterator.next();
		if (leftPeekValue != null) {
			currentLeftValue = leftPeekValue;
		} else {
			currentLeftValue = valueFunction.apply(currentLeft);
		}

		leftPeekValue = null;
		currentLeftValueAndPeekEquals = -1;

		if (oldLeftValue.equals(currentLeftValue)) {
			// we have duplicate keys on the leftIterator and need to reset the rightIterator (if it
			// is resettable)
			if (rightIterator.isResettable()) {
				rightIterator.reset();
			}
		} else {
			rightIterator.unmark();
		}
	}

	private void equal() {
		if (rightIterator.isResettable()) {
			next = join(currentLeft, rightIterator.next(), true);
		} else {
			doLeftPeek();

			if (currentLeftValueAndPeekEquals == 0) {
				rightIterator.mark();
				next = join(currentLeft, rightIterator.next(), true);
			} else {
				next = join(rightIterator.next(), currentLeft, false);
			}
		}
	}

	private void doLeftPeek() {
		if (leftPeekValue == null) {
			BindingSet leftPeek = leftIterator.peek();
			leftPeekValue = leftPeek != null ? valueFunction.apply(leftPeek) : null;
			currentLeftValueAndPeekEquals = -1;
		}

		if (currentLeftValueAndPeekEquals == -1) {
			boolean equals = currentLeftValue.equals(leftPeekValue);
			if (equals) {
				currentLeftValue = leftPeekValue;
				currentLeftValueAndPeekEquals = 0;
			} else {
				currentLeftValueAndPeekEquals = 1;
			}
		}
	}

	@Override
	public final boolean hasNext() {
		if (isClosed()) {
			return false;
		}

		calculateNext();
		return next != null;
	}

	@Override
	public final BindingSet next() {
		if (isClosed()) {
			throw new NoSuchElementException("The iteration has been closed.");
		}
		calculateNext();

		if (next == null) {
			close();
		}

		BindingSet result = next;

		if (result != null) {
			next = null;
			return result;
		} else {
			throw new NoSuchElementException();
		}
	}

	/**
	 * Throws an {@link UnsupportedOperationException}.
	 */
	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Checks whether this CloseableIteration has been closed.
	 *
	 * @return <var>true</var> if the CloseableIteration has been closed, <var>false</var> otherwise.
	 */
	public final boolean isClosed() {
		return closed;
	}

	@Override
	public final void close() {
		if (!closed) {
			closed = true;
			try {
				leftIterator.close();
			} finally {
				rightIterator.close();
			}
		}
	}
}