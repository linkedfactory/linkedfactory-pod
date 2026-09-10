package io.github.linkedfactory.core.rdf4j.fts;

public interface FtsSearchBackendFactory {
	String backendType();

	FtsSearchBackend create(FtsFederatedServiceConfig config) throws Exception;
}
