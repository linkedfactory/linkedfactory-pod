package io.github.linkedfactory.core.rdf4j.fts;

import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.NotifyingSailWrapper;

public class FtsSail extends NotifyingSailWrapper {
	private final FtsSearchService searchService;

	public FtsSail() {
		this(FtsSearchService.NOOP);
	}

	public FtsSail(FtsSearchService searchService) {
		this.searchService = searchService == null ? FtsSearchService.NOOP : searchService;
	}

	public FtsSail(FtsSearchService searchService, Sail baseSail) {
		this(searchService);
		setBaseSail(baseSail);
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
