package io.github.linkedfactory.core.rdf4j.common.query;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class AsyncIterator<T> implements CloseableIteration<T, QueryEvaluationException> {
	static final Logger log = LoggerFactory.getLogger(AsyncIterator.class);

	static final Object NULL_ELEMENT = new Object();
	final BlockingQueue<T> nextElements;
	volatile boolean closed = false;
	T next;

	public AsyncIterator(Supplier<CloseableIteration<T, QueryEvaluationException>> base, Supplier<ExecutorService> executorService) {
		nextElements = new ArrayBlockingQueue<>(100);
		var currentAsync = InnerJoinIterator.asyncDepth.get();
		executorService.get().submit(() -> {
			InnerJoinIterator.asyncDepth.set(currentAsync != null ? currentAsync + 1 : 1);
			try {
				var baseIt = base.get();
				try {
					while (baseIt.hasNext()) {
						T element = baseIt.next();
						while (!nextElements.offer(element, 10, TimeUnit.MILLISECONDS)) {
							if (closed) {
								return;
							}
						}
					}
				} catch (InterruptedException e) {
					// just return
				} finally {
					baseIt.close();
				}
			} catch (Exception e) {
				log.error("Error while computing elements", e);
			} finally {
				try {
					while (!nextElements.offer((T) NULL_ELEMENT, 10, TimeUnit.MILLISECONDS)) {
						if (closed) {
							break;
						}
					}
				} catch (InterruptedException e) {
					// just return
				} finally {
					InnerJoinIterator.asyncDepth.remove();
				}
			}
		});
	}

	@Override
	public boolean hasNext() {
		if (next == null && !closed) {
			try {
				T nextElement = nextElements.take();
				if (nextElement == NULL_ELEMENT) {
					close();
				} else {
					next = nextElement;
				}
			} catch (InterruptedException e) {
				return false;
			}
		}
		return next != null;
	}

	@Override
	public T next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}
		T result = next;
		next = null;
		return result;
	}

	@Override
	public void remove() throws QueryEvaluationException {
		throw new UnsupportedOperationException("Remove is not supported");
	}

	@Override
	public void close() {
		closed = true;
	}
}
