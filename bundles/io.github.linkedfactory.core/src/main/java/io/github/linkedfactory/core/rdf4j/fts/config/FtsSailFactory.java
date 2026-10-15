package io.github.linkedfactory.core.rdf4j.fts.config;

import io.github.linkedfactory.core.rdf4j.fts.FtsSail;
import io.github.linkedfactory.core.rdf4j.fts.FtsFederatedServiceConfig;
import io.github.linkedfactory.core.rdf4j.fts.FtsSearchService;
import io.github.linkedfactory.core.rdf4j.fts.HttpFtsSearchService;
import io.github.linkedfactory.core.rdf4j.fts.LuceneFtsIndexRegistry;
import io.github.linkedfactory.core.rdf4j.fts.LuceneFtsSearchService;
import io.github.linkedfactory.core.rdf4j.fts.LuceneSearchBackendFactory;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.config.SailConfigException;
import org.eclipse.rdf4j.sail.config.SailFactory;
import org.eclipse.rdf4j.sail.config.SailImplConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FtsSailFactory implements SailFactory {
	public static final String SAIL_TYPE = "fts:FtsSail";
	private static final Logger logger = LoggerFactory.getLogger(FtsSailFactory.class);

	@Override
	public String getSailType() {
		return SAIL_TYPE;
	}

	@Override
	public SailImplConfig getConfig() {
		return new FtsSailConfig();
	}

	@Override
	public Sail getSail(SailImplConfig config) throws SailConfigException {
		if (!SAIL_TYPE.equals(config.getType())) {
			throw new SailConfigException("Invalid Sail type: " + config.getType());
		}

		FtsSail sail;
		if (config instanceof FtsSailConfig ftsConfig) {
			boolean lucene = LuceneSearchBackendFactory.BACKEND_TYPE.equalsIgnoreCase(ftsConfig.getBackend());
			String resolvedEndpoint = lucene ? ftsConfig.getIndexDir() : ftsConfig.getEndpoint();
			FtsSearchService searchService;
			if (lucene) {
				try {
					searchService = new LuceneFtsSearchService(LuceneFtsIndexRegistry.getOrCreate(resolvedEndpoint));
				} catch (Exception e) {
					throw new SailConfigException("Unable to initialize embedded Lucene FTS index at " + resolvedEndpoint, e);
				}
			} else {
				searchService = new HttpFtsSearchService(
						ftsConfig.getEndpoint(),
						ftsConfig.getBulkPath(),
						ftsConfig.isFailOnError(),
						ftsConfig.getOutboxDir());
			}
			sail = new FtsSail(
					searchService,
					new FtsFederatedServiceConfig(
							ftsConfig.getBackend(),
							resolvedEndpoint,
							ftsConfig.getSearchPath(),
							ftsConfig.isFailOnError(),
							ftsConfig.getDefaultLimit()));
		} else {
			logger.warn("Config is instance of {} and not FtsSailConfig, using defaults.", config.getClass().getName());
			sail = new FtsSail(new HttpFtsSearchService());
		}
		return sail;
	}
}
