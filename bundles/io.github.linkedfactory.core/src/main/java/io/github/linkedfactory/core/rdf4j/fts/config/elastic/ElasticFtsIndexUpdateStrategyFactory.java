package io.github.linkedfactory.core.rdf4j.fts.config.elastic;

import io.github.linkedfactory.core.rdf4j.fts.elastic.ElasticHttpFtsIndexUpdateStrategy;
import io.github.linkedfactory.core.rdf4j.fts.FtsIndexUpdateStrategy;
import io.github.linkedfactory.core.rdf4j.fts.config.FtsIndexUpdateStrategyFactory;
import io.github.linkedfactory.core.rdf4j.fts.config.FtsSailConfig;
import org.eclipse.rdf4j.sail.config.SailConfigException;

/** Service provider for the Elasticsearch HTTP update strategy. */
public final class ElasticFtsIndexUpdateStrategyFactory implements FtsIndexUpdateStrategyFactory {
	@Override
	public String backendType() {
		return "elastic";
	}

	@Override
	public FtsIndexUpdateStrategy create(FtsSailConfig config) throws SailConfigException {
		if (config.getEndpoint() == null || config.getEndpoint().isBlank()) {
			throw new SailConfigException("The elastic FTS backend requires fts:endpoint");
		}
		return new ElasticHttpFtsIndexUpdateStrategy(
				config.getEndpoint(),
				config.getBulkPath(),
				config.isFailOnError(),
				config.getOutboxDir(),
				config.getRequestTimeoutMillis());
	}
}
