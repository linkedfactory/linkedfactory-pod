package io.github.linkedfactory.core.rdf4j.fts.elastic;

import io.github.linkedfactory.core.rdf4j.fts.FtsIndexUpdate;
import io.github.linkedfactory.core.rdf4j.fts.FtsIndexUpdateException;
import io.github.linkedfactory.core.rdf4j.fts.FtsIndexUpdateStrategy;

import java.io.IOException;
import java.util.Map;

/** Elasticsearch HTTP implementation of the backend-neutral FTS update strategy. */
public final class ElasticHttpFtsIndexUpdateStrategy implements FtsIndexUpdateStrategy {
	private final HttpFtsSearchService delegate;

	public ElasticHttpFtsIndexUpdateStrategy(String endpoint, String bulkPath, boolean failOnError,
			String scratchDirectory, int requestTimeoutMillis) {
		this.delegate = new HttpFtsSearchService(endpoint, bulkPath, failOnError,
				scratchDirectory, requestTimeoutMillis);
	}

	public ElasticHttpFtsIndexUpdateStrategy(Map<String, Object> properties) {
		this.delegate = new HttpFtsSearchService(properties);
	}

	@Override
	public void apply(FtsIndexUpdate update) throws FtsIndexUpdateException {
		delegate.apply(update);
	}

	@Override
	public void close() throws IOException {
		delegate.shutdown();
	}
}
