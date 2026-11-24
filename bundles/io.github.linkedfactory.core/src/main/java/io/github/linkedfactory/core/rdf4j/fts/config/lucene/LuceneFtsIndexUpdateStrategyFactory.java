package io.github.linkedfactory.core.rdf4j.fts.config.lucene;

import io.github.linkedfactory.core.rdf4j.fts.FtsIndexUpdateStrategy;
import io.github.linkedfactory.core.rdf4j.fts.LuceneFtsIndexRegistry;
import io.github.linkedfactory.core.rdf4j.fts.LuceneFtsSearchService;
import io.github.linkedfactory.core.rdf4j.fts.LuceneSearchBackendFactory;
import io.github.linkedfactory.core.rdf4j.fts.config.FtsIndexUpdateStrategyFactory;
import io.github.linkedfactory.core.rdf4j.fts.config.FtsSailConfig;
import org.eclipse.rdf4j.sail.config.SailConfigException;

/** Service provider for the embedded Lucene update strategy. */
public final class LuceneFtsIndexUpdateStrategyFactory implements FtsIndexUpdateStrategyFactory {
	@Override
	public String backendType() {
		return LuceneSearchBackendFactory.BACKEND_TYPE;
	}

	@Override
	public FtsIndexUpdateStrategy create(FtsSailConfig config) throws SailConfigException {
		if (config.getIndexDir() == null || config.getIndexDir().isBlank()) {
			throw new SailConfigException("The lucene FTS backend requires fts:indexDir");
		}
		try {
			String indexDir = config.getIndexDir();
			return new LuceneFtsSearchService(LuceneFtsIndexRegistry.getOrCreate(indexDir), indexDir);
		} catch (Exception e) {
			throw new SailConfigException(
					"Unable to initialize embedded Lucene FTS index at " + config.getIndexDir(), e);
		}
	}
}
