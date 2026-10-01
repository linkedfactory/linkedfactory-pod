package io.github.linkedfactory.core.rdf4j.fts;

import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.NotifyingSailWrapper;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public class FtsSail extends NotifyingSailWrapper {
	private final FtsSearchService searchService;
	private final FtsFederatedServiceConfig federatedServiceConfig;
	private final Set<String> excludedModels;

	public FtsSail() {
		this(FtsSearchService.NOOP, FtsFederatedServiceConfig.defaults());
	}

	public FtsSail(FtsSearchService searchService) {
		this(searchService, FtsFederatedServiceConfig.defaults(), Collections.emptySet());
	}

	public FtsSail(FtsSearchService searchService, FtsFederatedServiceConfig federatedServiceConfig) {
		this(searchService, federatedServiceConfig, Collections.emptySet());
	}

	public FtsSail(FtsSearchService searchService, FtsFederatedServiceConfig federatedServiceConfig,
			Set<String> excludedModels) {
		this.searchService = searchService == null ? FtsSearchService.NOOP : searchService;
		this.federatedServiceConfig = federatedServiceConfig == null
				? FtsFederatedServiceConfig.defaults()
				: federatedServiceConfig;
		this.excludedModels = excludedModels == null || excludedModels.isEmpty()
				? Collections.emptySet()
				: Collections.unmodifiableSet(new LinkedHashSet<>(excludedModels));
	}

	public FtsSail(FtsSearchService searchService, Sail baseSail) {
		this(searchService, FtsFederatedServiceConfig.defaults(), Collections.emptySet());
		setBaseSail(baseSail);
	}

	public FtsFederatedServiceConfig getFederatedServiceConfig() {
		return federatedServiceConfig;
	}

	public Set<String> getExcludedModels() {
		return excludedModels;
	}

	@Override
	public NotifyingSailConnection getConnection() throws SailException {
		SailConnection wrappedConnection = super.getConnection();
		if (!(wrappedConnection instanceof NotifyingSailConnection)) {
			throw new SailException("Wrapped SailConnection must implement NotifyingSailConnection.");
		}
		return new FtsSailConnection((NotifyingSailConnection) wrappedConnection, searchService, excludedModels);
	}
}
