package io.github.linkedfactory.core.rdf4j.vector;

public class VectorSearchRequest {
	private final float[] queryVector;
	private final int topK;
	private final String iriFilter;

	public VectorSearchRequest(float[] queryVector, int topK, String iriFilter) {
		this.queryVector = queryVector;
		this.topK = topK;
		this.iriFilter = iriFilter;
	}

	public float[] getQueryVector() {
		return queryVector;
	}

	public int getTopK() {
		return topK;
	}

	public String getIriFilter() {
		return iriFilter;
	}
}
