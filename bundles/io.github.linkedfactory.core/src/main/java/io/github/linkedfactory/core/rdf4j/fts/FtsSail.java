package io.github.linkedfactory.core.rdf4j.fts;

import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.NotifyingSailWrapper;

public class FtsSail extends NotifyingSailWrapper {
	private final FtsSearchService searchService;
	private final FtsFederatedServiceConfig federatedServiceConfig;

	public FtsSail() {
		this(FtsSearchService.NOOP, FtsFederatedServiceConfig.defaults());
	}

	public FtsSail(FtsSearchService searchService) {
		this(searchService, FtsFederatedServiceConfig.defaults());
	}

	public FtsSail(FtsSearchService searchService, FtsFederatedServiceConfig federatedServiceConfig) {
		this.searchService = searchService == null ? FtsSearchService.NOOP : searchService;
		this.federatedServiceConfig = federatedServiceConfig == null
				? FtsFederatedServiceConfig.defaults()
				: federatedServiceConfig;
	}

	public FtsSail(FtsSearchService searchService, Sail baseSail) {
		this(searchService, FtsFederatedServiceConfig.defaults());
		setBaseSail(baseSail);
	}

	public FtsFederatedServiceConfig getFederatedServiceConfig() {
		return federatedServiceConfig;
	}

	@Override
	public NotifyingSailConnection getConnection() throws SailException {
		SailConnection wrappedConnection = super.getConnection();
		if (!(wrappedConnection instanceof NotifyingSailConnection)) {
			throw new SailException("Wrapped SailConnection must implement NotifyingSailConnection.");
		}
		return new FtsSailConnection((NotifyingSailConnection) wrappedConnection, searchService);
	}
}
