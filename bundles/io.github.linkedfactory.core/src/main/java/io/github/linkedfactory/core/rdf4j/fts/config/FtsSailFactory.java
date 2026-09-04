package io.github.linkedfactory.core.rdf4j.fts.config;

import io.github.linkedfactory.core.rdf4j.fts.FtsSail;
import io.github.linkedfactory.core.rdf4j.fts.FtsFederatedServiceConfig;
import io.github.linkedfactory.core.rdf4j.fts.HttpFtsSearchService;
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
			sail = new FtsSail(
					new HttpFtsSearchService(
							ftsConfig.getEndpoint(),
							ftsConfig.getBulkPath(),
							ftsConfig.isFailOnError(),
							ftsConfig.getOutboxDir()),
					new FtsFederatedServiceConfig(
							ftsConfig.getBackend(),
							ftsConfig.getEndpoint(),
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
