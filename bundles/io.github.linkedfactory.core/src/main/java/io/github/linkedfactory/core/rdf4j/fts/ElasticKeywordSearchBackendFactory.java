package io.github.linkedfactory.core.rdf4j.fts;

public class ElasticKeywordSearchBackendFactory implements FtsSearchBackendFactory {
	@Override
	public String backendType() {
		return FtsFederatedServiceConfig.DEFAULT_BACKEND;
	}

	@Override
	public FtsSearchBackend create(FtsFederatedServiceConfig config) {
		return new ElasticKeywordSearchBackend(config.getEndpoint(), config.getSearchPath(), config.isFailOnError(),
				config.getDefaultLimit());
	}
}
