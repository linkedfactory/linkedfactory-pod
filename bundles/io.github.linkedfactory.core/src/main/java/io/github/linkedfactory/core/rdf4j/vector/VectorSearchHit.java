package io.github.linkedfactory.core.rdf4j.vector;

public class VectorSearchHit {
	private final String iri;
	private final double score;

	public VectorSearchHit(String iri, double score) {
		this.iri = iri;
		this.score = score;
	}

	public String getIri() {
		return iri;
	}

	public double getScore() {
		return score;
	}
}
