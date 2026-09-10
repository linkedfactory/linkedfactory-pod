package io.github.linkedfactory.core.rdf4j.fts;

import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.AbstractFederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;

import java.net.URI;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public class FtsFederatedServiceResolver extends AbstractFederatedServiceResolver {
	private final FtsFederatedServiceConfig config;
	private final Map<String, FtsSearchBackendFactory> backendFactories;

	public FtsFederatedServiceResolver() {
		this(FtsFederatedServiceConfig.defaults());
	}

	public FtsFederatedServiceResolver(FtsFederatedServiceConfig config) {
		this(config, java.util.List.of(new ElasticKeywordSearchBackendFactory()));
	}

	public FtsFederatedServiceResolver(FtsFederatedServiceConfig config,
			Collection<? extends FtsSearchBackendFactory> backendFactories) {
		this.config = config == null ? FtsFederatedServiceConfig.defaults() : config;
		this.backendFactories = new LinkedHashMap<>();
		if (backendFactories != null) {
			for (FtsSearchBackendFactory factory : backendFactories) {
				if (factory != null) {
					this.backendFactories.put(factory.backendType().toLowerCase(), factory);
				}
			}
		}
	}

	@Override
	protected FederatedService createService(String serviceUrl) throws QueryEvaluationException {
		if (serviceUrl == null || !serviceUrl.startsWith(FTS.FTS)) {
			return null;
		}

		String backendType = config.getBackend().toLowerCase();
		FtsSearchBackendFactory backendFactory = backendFactories.get(backendType);
		if (backendFactory == null) {
			throw new QueryEvaluationException("Unknown FTS backend '" + backendType + "'. Available backends: "
					+ backendFactories.keySet());
		}
		try {
			return new FtsFederatedService(backendFactory.create(effectiveConfig(serviceUrl)));
		} catch (Exception e) {
			throw new QueryEvaluationException("Unable to initialize FTS backend '" + backendType + "'", e);
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
