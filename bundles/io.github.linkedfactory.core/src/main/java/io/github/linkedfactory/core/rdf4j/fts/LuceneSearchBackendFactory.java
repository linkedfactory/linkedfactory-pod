package io.github.linkedfactory.core.rdf4j.fts;

public class LuceneSearchBackendFactory implements FtsSearchBackendFactory {
	public static final String BACKEND_TYPE = "lucene";

	@Override
	public String backendType() {
		return BACKEND_TYPE;
	}

	@Override
	public FtsSearchBackend create(FtsFederatedServiceConfig config) throws Exception {
		LuceneFtsIndex index = LuceneFtsIndexRegistry.getOrCreate(config.getEndpoint());
		return new LuceneFtsSearchBackend(index);
	}
}
