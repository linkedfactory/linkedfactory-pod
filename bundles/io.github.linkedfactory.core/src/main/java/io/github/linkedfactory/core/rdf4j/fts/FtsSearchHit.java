package io.github.linkedfactory.core.rdf4j.fts;

public class FtsSearchHit {
	private final String iri;
	private final Double score;
	private final String snippet;

	public FtsSearchHit(String iri, Double score, String snippet) {
		this.iri = iri;
		this.score = score;
		this.snippet = snippet;
	}

	public String getIri() {
		return iri;
	}

	public Double getScore() {
		return score;
	}

	public String getSnippet() {
		return snippet;
	}
}
