package io.github.linkedfactory.core.rdf4j.fts.config;

import io.github.linkedfactory.core.rdf4j.fts.FtsIndexUpdateStrategy;
import org.eclipse.rdf4j.sail.config.SailConfigException;

/** Service-provider extension point for backend-specific FTS update strategies. */
public interface FtsIndexUpdateStrategyFactory {
	String backendType();

	FtsIndexUpdateStrategy create(FtsSailConfig config) throws SailConfigException;
}
