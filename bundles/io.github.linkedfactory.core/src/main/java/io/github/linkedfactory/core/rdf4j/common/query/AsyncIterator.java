package io.github.linkedfactory.core.rdf4j.common.query;

import io.github.linkedfactory.core.rdf4j.common.query.InnerJoinIterator;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.NiceIterator;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.QueryEvaluationException;

import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class AsyncIterator<T> implements CloseableIteration<T, QueryEvaluationException> {
	final BlockingQueue<T> nextElements;
	volatile boolean closed = false;
	static final Object NULL_ELEMENT = new Object();
	T next;

	public AsyncIterator(Supplier<CloseableIteration<T, QueryEvaluationException>> base, Supplier<ExecutorService> executorService) {
			nextElements = new ArrayBlockingQueue<>(100);
			executorService.get().submit(() -> {
				InnerJoinIterator.isAsync.set(true);
				var baseIt = base.get();
				try {
					while (baseIt.hasNext()) {
						T element = baseIt.next();
						while (!nextElements.offer(element, 100, TimeUnit.MILLISECONDS)) {
							if (closed) {
								return;
							}
						}
					}
					baseIt.close();
					while (!nextElements.offer((T) NULL_ELEMENT, 100, TimeUnit.MILLISECONDS)) {
						if (closed) {
							return;
						}
					}
				} catch (InterruptedException e) {
					// just return
				} finally {
					baseIt.close();
					InnerJoinIterator.isAsync.remove();
				}
			});
	}

	@Override
	public boolean hasNext() {
		if (next == null) {
				try {
					T nextElement = nextElements.take();
					if (nextElement != NULL_ELEMENT) {
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
		if (! hasNext()) {
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
