package io.github.linkedfactory.core.rdf4j.common;

import org.eclipse.rdf4j.query.algebra.evaluation.federation.AbstractFederatedServiceResolver;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Bas implementation of federated service resolver that also provides an executor service.
 */
public abstract class BaseFederatedServiceResolver extends AbstractFederatedServiceResolver {
	private ExecutorService executorService;

	public synchronized ExecutorService getExecutorService() {
		if (executorService == null) {
			executorService = Executors.newCachedThreadPool();
		}
		return executorService;
	}

	@Override
	public void shutDown() {
		super.shutDown();
		synchronized (this) {
			if (executorService != null) {
				executorService.shutdownNow();
				executorService = null;
			}
		}
	}
}
