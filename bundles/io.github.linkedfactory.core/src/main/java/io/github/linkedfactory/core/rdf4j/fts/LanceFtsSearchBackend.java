package io.github.linkedfactory.core.rdf4j.fts;

import java.util.List;

public class LanceFtsSearchBackend implements FtsSearchBackend {
	private final LanceFtsIndex index;

	public LanceFtsSearchBackend(LanceFtsIndex index) {
		this.index = index;
	}

	@Override
	public List<FtsSearchHit> search(FtsSearchRequest request) throws Exception {
		return index.search(request);
	}
}
