package io.github.linkedfactory.core.rdf4j.vector;

import java.util.List;

public class JVectorSearchBackend implements VectorSearchBackend {
	private final JVectorIndex index;

	public JVectorSearchBackend(JVectorIndex index) {
		this.index = index;
	}

	@Override
	public List<VectorSearchHit> search(VectorSearchRequest request) throws Exception {
		return index.search(request);
	}
}
