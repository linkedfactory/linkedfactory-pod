package io.github.linkedfactory.core.rdf4j.fts;

public class FtsFederatedServiceConfig {
	public static final String DEFAULT_BACKEND = "elastic";
	public static final String DEFAULT_ENDPOINT = "http://localhost:9200";
	public static final String DEFAULT_SEARCH_PATH = "/fts/_search";
	public static final int DEFAULT_LIMIT = 100;
	public static final boolean DEFAULT_FAIL_ON_ERROR = true;

	private final String backend;
	private final String endpoint;
	private final String searchPath;
	private final boolean failOnError;
	private final int defaultLimit;

	public FtsFederatedServiceConfig(String backend, String endpoint, String searchPath, boolean failOnError,
			int defaultLimit) {
		this.backend = backend == null || backend.isBlank() ? DEFAULT_BACKEND : backend.trim().toLowerCase();
		this.endpoint = endpoint == null || endpoint.isBlank() ? DEFAULT_ENDPOINT : endpoint.trim();
		this.searchPath = searchPath == null || searchPath.isBlank() ? DEFAULT_SEARCH_PATH : searchPath.trim();
		this.failOnError = failOnError;
		this.defaultLimit = Math.max(defaultLimit, 1);
	}

	public static FtsFederatedServiceConfig defaults() {
		return new FtsFederatedServiceConfig(DEFAULT_BACKEND, DEFAULT_ENDPOINT, DEFAULT_SEARCH_PATH, DEFAULT_FAIL_ON_ERROR,
				DEFAULT_LIMIT);
	}

	public String getBackend() {
		return backend;
	}

	public String getEndpoint() {
		return endpoint;
	}

	public String getSearchPath() {
		return searchPath;
	}

	public boolean isFailOnError() {
		return failOnError;
	}

	public int getDefaultLimit() {
		return defaultLimit;
	}
}
