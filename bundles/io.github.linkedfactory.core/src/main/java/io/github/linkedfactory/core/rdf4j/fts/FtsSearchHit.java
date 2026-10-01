package io.github.linkedfactory.core.rdf4j.fts;

public class FtsSearchHit {
	private final String iri;
	private final Double score;
	private final String snippet;
	private final String documentId;

	public FtsSearchHit(String iri, Double score, String snippet) {
		this(iri, score, snippet, null);
	}

	public FtsSearchHit(String iri, Double score, String snippet, String documentId) {
		this.iri = iri;
		this.score = score;
		this.snippet = snippet;
		this.documentId = documentId;
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

	public String getDocumentId() {
		return documentId;
	}
}
