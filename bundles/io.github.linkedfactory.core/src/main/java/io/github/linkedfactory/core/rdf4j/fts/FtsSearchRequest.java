package io.github.linkedfactory.core.rdf4j.fts;

public class FtsSearchRequest {
	private final String keywords;
	private final String field;
	private final int limit;
	private final Double boost;
	private final boolean includeSnippet;
	private final String iriFilter;

	public FtsSearchRequest(String keywords, String field, int limit, Double boost, boolean includeSnippet, String iriFilter) {
		this.keywords = keywords;
		this.field = field;
		this.limit = limit;
		this.boost = boost;
		this.includeSnippet = includeSnippet;
		this.iriFilter = iriFilter;
	}

	public String getKeywords() {
		return keywords;
	}

	public String getField() {
		return field;
	}

	public int getLimit() {
		return limit;
	}

	public Double getBoost() {
		return boost;
	}

	public boolean isIncludeSnippet() {
		return includeSnippet;
	}

	public String getIriFilter() {
		return iriFilter;
	}
}
