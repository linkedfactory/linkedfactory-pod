package io.github.linkedfactory.core.rdf4j.fts;

import java.util.List;

public class LuceneFtsSearchBackend implements FtsSearchBackend {
	private final LuceneFtsIndex index;

	public LuceneFtsSearchBackend(LuceneFtsIndex index) {
		this.index = index;
	}

	@Override
	public List<FtsSearchHit> search(FtsSearchRequest request) throws Exception {
		return index.search(request);
	}
}
