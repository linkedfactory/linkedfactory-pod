package io.github.linkedfactory.core.rdf4j.fts;

import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.AbstractFederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;

import java.net.URI;

public class FtsFederatedServiceResolver extends AbstractFederatedServiceResolver {
	private final FtsFederatedServiceConfig config;
	private final FtsSearchBackendFactory backendFactory;

	public FtsFederatedServiceResolver() {
		this(FtsFederatedServiceConfig.defaults());
	}

	public FtsFederatedServiceResolver(FtsFederatedServiceConfig config) {
		this.config = config == null || config.getBackend() == null || config.getBackend().isBlank()
				? FtsFederatedServiceConfig.defaults()
				: config;
			this.backendFactory = this.config.getBackend().toLowerCase().equals(FtsFederatedServiceConfig.HTTP_ELASTICSEARCH_BACKEND)
					? new ElasticKeywordSearchBackendFactory()
					: new LuceneSearchBackendFactory();
	}

	public FtsFederatedServiceResolver(FtsFederatedServiceConfig config, FtsSearchBackendFactory backendFactory) {
		this.config = config == null ? FtsFederatedServiceConfig.defaults() : config;
		this.backendFactory = backendFactory == null ? new LuceneSearchBackendFactory() : backendFactory;
	}

	@Override
	protected FederatedService createService(String serviceUrl) throws QueryEvaluationException {
		if (serviceUrl == null || !serviceUrl.startsWith(FTS.FTS)) {
			return null;
		}

		if (backendFactory == null || !backendFactory.backendType().equalsIgnoreCase(config.getBackend())) {
			throw new QueryEvaluationException("missing or wrong FTS backend '" + (backendFactory != null ? backendFactory.backendType() : null) + "'. configured backends: "
					+ config.getBackend());
		}
		try {
			return new FtsFederatedService(backendFactory.create(effectiveConfig(serviceUrl)));
		} catch (Exception e) {
			throw new QueryEvaluationException("Unable to initialize FTS backend '" + backendFactory.backendType() + "'", e);
		}
	}

	private FtsFederatedServiceConfig effectiveConfig(String serviceUrl) {
		return new FtsFederatedServiceConfig(
				config.getBackend(),
				endpoint(serviceUrl),
				config.getSearchPath(),
				config.isFailOnError(),
				config.getDefaultLimit());
	}

	private String endpoint(String serviceUrl) {
		String suffix = serviceUrl.substring(FTS.FTS.length()).trim();
		if (!suffix.isEmpty()) {
			if (suffix.startsWith("//") || URI.create(suffix).isAbsolute()) {
				throw new IllegalArgumentException("Absolute FTS service URLs are not allowed: " + serviceUrl);
			}
			return URI.create(config.getEndpoint()).resolve(URI.create(suffix)).toString();
		}
		return config.getEndpoint();
	}
}
