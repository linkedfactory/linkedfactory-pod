package io.github.linkedfactory.core.rdf4j.fts.config.lance;

import io.github.linkedfactory.core.rdf4j.fts.FtsIndexUpdateStrategy;
import io.github.linkedfactory.core.rdf4j.fts.LanceFtsIndexRegistry;
import io.github.linkedfactory.core.rdf4j.fts.LanceFtsSearchService;
import io.github.linkedfactory.core.rdf4j.fts.LanceSearchBackendFactory;
import io.github.linkedfactory.core.rdf4j.fts.config.FtsIndexUpdateStrategyFactory;
import io.github.linkedfactory.core.rdf4j.fts.config.FtsSailConfig;
import org.eclipse.rdf4j.sail.config.SailConfigException;

/** Service provider for the embedded Lance update strategy. */
public final class LanceFtsIndexUpdateStrategyFactory implements FtsIndexUpdateStrategyFactory {
	@Override
	public String backendType() {
		return LanceSearchBackendFactory.BACKEND_TYPE;
	}

	@Override
	public FtsIndexUpdateStrategy create(FtsSailConfig config) throws SailConfigException {
		if (config.getIndexDir() == null || config.getIndexDir().isBlank()) {
			throw new SailConfigException("The lance FTS backend requires fts:indexDir");
		}
		try {
			return new LanceFtsSearchService(LanceFtsIndexRegistry.getOrCreate(config.getIndexDir()));
		} catch (Exception e) {
			throw new SailConfigException(
					"Unable to initialize embedded Lance FTS index at " + config.getIndexDir(), e);
		}
	}
}
