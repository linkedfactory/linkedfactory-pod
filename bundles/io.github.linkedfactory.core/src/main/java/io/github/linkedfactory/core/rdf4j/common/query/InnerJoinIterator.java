package io.github.linkedfactory.core.rdf4j.common.query;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.*;
import java.util.function.Supplier;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.common.iteration.LookAheadIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryEvaluationStep;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;

public class InnerJoinIterator extends LookAheadIteration<BindingSet, QueryEvaluationException> {

	/*-----------*
	 * Variables *
	 *-----------*/

	private static final BindingSet NULL_BINDINGS = new EmptyBindingSet();
	private static final ThreadLocal<Boolean> isAsync = new ThreadLocal<>();
	private final EvaluationStrategy strategy;
	private final Supplier<ExecutorService> executorService;
	private final CloseableIteration<BindingSet, QueryEvaluationException> leftIter;
	private final QueryEvaluationStep preparedJoinArg;
	private final List<BlockingQueue<BindingSet>> joined;
	private volatile CloseableIteration<BindingSet, QueryEvaluationException> rightIter;

	/*--------------*
	 * Constructors *
	 *--------------*/

	public InnerJoinIterator(EvaluationStrategy strategy, Supplier<ExecutorService> executorService, QueryEvaluationStep leftPrepared,
	                         QueryEvaluationStep rightPrepared, BindingSet bindings, boolean lateral, boolean async) throws QueryEvaluationException {
		this.strategy = strategy;
		this.executorService = executorService;

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

		if (async && isAsync.get() == Boolean.TRUE) {
			async = false;
		}
		joined = async ? new ArrayList<>() : null;
	}

	/*---------*
	 * Methods *
	 *---------*/

	@Override
	protected BindingSet getNextElement() throws QueryEvaluationException {
		if (joined == null) {
			return getNextElementSync();
		} else {
			return getNextElementAsync();
		}
	}

	protected BindingSet getNextElementSync() throws QueryEvaluationException {
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

	protected BindingSet getNextElementAsync() throws QueryEvaluationException {
		try {
			while (!joined.isEmpty() || leftIter.hasNext()) {
				enqueueNext();
				if (!joined.isEmpty()) {
					BlockingQueue<BindingSet> nextQueue = joined.get(0);
					BindingSet next = nextQueue.take();
					if (next == NULL_BINDINGS) {
						joined.remove(0);
						continue;
					}
					enqueueNext();
					if (next != NULL_BINDINGS) {
						return next;
					}
				}
			}
		} catch (NoSuchElementException ignore) {
			// probably, one of the iterations has been closed concurrently in
			// handleClose()
		} catch (InterruptedException e) {
			close();
		}

		return null;
	}

	private void enqueueNext() {
		while (joined.size() < 5 && leftIter.hasNext()) {
			BlockingQueue<BindingSet> queue = new ArrayBlockingQueue<>(50);
			joined.add(queue);
			BindingSet next = leftIter.next();
			executorService.get().submit(() -> {
				isAsync.set(true);
				var rightIt = preparedJoinArg.evaluate(next);
				try {
					while (rightIt.hasNext()) {
						BindingSet bindings = rightIt.next();
						while (! queue.offer(bindings, 100, TimeUnit.MILLISECONDS)) {
							if (isClosed()) {
								return;
							}
						}
					}
					rightIt.close();
					while (! queue.offer(NULL_BINDINGS, 100, TimeUnit.MILLISECONDS)) {
						if (isClosed()) {
							return;
						}
					}
				} catch (InterruptedException e) {
					// just return
				} finally {
					rightIt.close();
					isAsync.remove();
				}
			});
		}
	}

	@Override
	protected void handleClose() throws QueryEvaluationException {
		super.handleClose();

		leftIter.close();
		rightIter.close();
	}
}