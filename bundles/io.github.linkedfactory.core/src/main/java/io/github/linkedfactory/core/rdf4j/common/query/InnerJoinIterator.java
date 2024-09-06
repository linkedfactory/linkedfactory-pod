package io.github.linkedfactory.core.rdf4j.common.query;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.common.iteration.LookAheadIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryEvaluationStep;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class InnerJoinIterator extends LookAheadIteration<BindingSet, QueryEvaluationException> {
	private static final Logger log = LoggerFactory.getLogger(InnerJoinIterator.class);

	/*-----------*
	 * Variables *
	 *-----------*/

	public static final ThreadLocal<Integer> asyncDepth = new ThreadLocal<>();
	public static final int MAX_ASYNC_DEPTH = 3;
	private static final BindingSet NULL_BINDINGS = new EmptyBindingSet();
	public static final int BATCH_SIZE = 200;
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
	                         QueryEvaluationStep rightPrepared, List<BindingSet> bindingSets, boolean lateral, boolean async) throws QueryEvaluationException {
		this.strategy = strategy;
		this.executorService = executorService;

		CloseableIteration<BindingSet, QueryEvaluationException> leftIt = BatchQueryEvaluationStep.evaluate(leftPrepared, bindingSets);
		if (leftIt.hasNext() || lateral) {
			preparedJoinArg = rightPrepared;
		} else {
			leftIt.close();
			leftIt = BatchQueryEvaluationStep.evaluate(rightPrepared, bindingSets);
			preparedJoinArg = leftPrepared;
		}
		rightIter = new EmptyIteration<>();
		leftIter = leftIt;

		if (async && (asyncDepth.get() != null && asyncDepth.get() > InnerJoinIterator.MAX_ASYNC_DEPTH)) {
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

				if (preparedJoinArg instanceof BatchQueryEvaluationStep) {
					List<BindingSet> nextLefts = new ArrayList<>(BATCH_SIZE);
					while (leftIter.hasNext() && nextLefts.size() < BATCH_SIZE) {
						nextLefts.add(leftIter.next());
					}
					if (!nextLefts.isEmpty()) {
						rightIter = ((BatchQueryEvaluationStep) preparedJoinArg).evaluate(nextLefts);
					}
				} else {
					if (leftIter.hasNext()) {
						rightIter = preparedJoinArg.evaluate(leftIter.next());
					}
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
		while (joined.size() < 2 && leftIter.hasNext()) {
			BlockingQueue<BindingSet> queue = new ArrayBlockingQueue<>(50);
			joined.add(queue);
			boolean useBatch = preparedJoinArg instanceof BatchQueryEvaluationStep;
			List<BindingSet> nextLefts = useBatch ? new ArrayList<>(BATCH_SIZE) : List.of(leftIter.next());
			if (useBatch) {
				while (leftIter.hasNext() && nextLefts.size() < BATCH_SIZE) {
					nextLefts.add(leftIter.next());
				}
			}
			var currentAsync = asyncDepth.get();
			executorService.get().submit(() -> {
				asyncDepth.set(currentAsync != null ? currentAsync + 1 : 1);
				CloseableIteration<BindingSet, QueryEvaluationException> rightIt = null;
				try {
					rightIt = useBatch && nextLefts.size() > 1 ?
							((BatchQueryEvaluationStep) preparedJoinArg).evaluate(nextLefts) :
							preparedJoinArg.evaluate(nextLefts.get(0));

					while (rightIt.hasNext()) {
						BindingSet bindings = rightIt.next();
						while (!queue.offer(bindings, 100, TimeUnit.MILLISECONDS)) {
							if (isClosed()) {
								return;
							}
						}
					}
					rightIt.close();
				} catch (Exception e) {
					log.error("Exception in async iterator", e);
				} finally {
					if (rightIt != null) {
						rightIt.close();
					}
					try {
						while (!queue.offer(NULL_BINDINGS, 100, TimeUnit.MILLISECONDS)) {
							if (isClosed()) {
								return;
							}
						}
					} catch (InterruptedException e) {
						// ignore
					} finally {
						asyncDepth.remove();
					}
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