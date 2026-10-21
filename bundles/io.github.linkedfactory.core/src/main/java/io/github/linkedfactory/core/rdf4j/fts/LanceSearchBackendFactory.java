package io.github.linkedfactory.core.rdf4j.fts;

public class LanceSearchBackendFactory implements FtsSearchBackendFactory {
	public static final String BACKEND_TYPE = "lance";

	@Override
	public String backendType() {
		return BACKEND_TYPE;
	}

	@Override
	public FtsSearchBackend create(FtsFederatedServiceConfig config) {
		LanceFtsIndex index = LanceFtsIndexRegistry.getOrCreate(config.getEndpoint());
		return new LanceFtsSearchBackend(index);
	}
}
