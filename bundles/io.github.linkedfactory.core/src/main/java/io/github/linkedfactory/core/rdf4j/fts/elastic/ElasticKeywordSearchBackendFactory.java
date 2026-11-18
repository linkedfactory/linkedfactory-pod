package io.github.linkedfactory.core.rdf4j.fts.elastic;

import io.github.linkedfactory.core.rdf4j.fts.FtsFederatedServiceConfig;
import io.github.linkedfactory.core.rdf4j.fts.FtsSearchBackend;
import io.github.linkedfactory.core.rdf4j.fts.FtsSearchBackendFactory;

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
