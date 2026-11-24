package io.github.linkedfactory.core.rdf4j.fts;

import java.util.List;

public class LuceneFtsSearchBackend implements FtsSearchBackend {
	private final LuceneFtsIndex index;
	private final String registryKey;

	public LuceneFtsSearchBackend(LuceneFtsIndex index) {
		this(index, null);
	}

	public LuceneFtsSearchBackend(LuceneFtsIndex index, String registryKey) {
		this.index = index;
		this.registryKey = registryKey;
	}

	@Override
	public List<FtsSearchHit> search(FtsSearchRequest request) throws Exception {
		return index.search(request);
	}

	@Override
	public void close() throws Exception {
		if (registryKey != null) {
			LuceneFtsIndexRegistry.release(registryKey);
		}
	}
}
