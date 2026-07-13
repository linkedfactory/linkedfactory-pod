package io.github.linkedfactory.core.rdf4j.fts;

import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.AbstractFederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;

public class FtsFederatedServiceResolver extends AbstractFederatedServiceResolver {
	@Override
	protected FederatedService createService(String serviceUrl) throws QueryEvaluationException {
		if (serviceUrl == null || !serviceUrl.startsWith(FTS.FTS)) {
			return null;
		}

		String endpoint = endpoint(serviceUrl);
		String searchPath = System.getProperty(ElasticKeywordSearchBackend.PROP_SEARCH_PATH, "/fts/_search");
		boolean failOnError = Boolean.parseBoolean(
				System.getProperty(ElasticKeywordSearchBackend.PROP_FAIL_ON_ERROR, "true"));
		int defaultLimit = 100;
		String defaultLimitValue = System.getProperty(ElasticKeywordSearchBackend.PROP_DEFAULT_LIMIT);
		if (defaultLimitValue != null && !defaultLimitValue.isBlank()) {
			try {
				defaultLimit = Integer.parseInt(defaultLimitValue.trim());
			} catch (NumberFormatException ignored) {
			}
		}

		FtsSearchBackend backend = new ElasticKeywordSearchBackend(endpoint, searchPath, failOnError, defaultLimit);
		return new FtsFederatedService(backend);
	}

	private String endpoint(String serviceUrl) {
		String suffix = serviceUrl.substring(FTS.FTS.length()).trim();
		if (!suffix.isEmpty()) {
			if (suffix.startsWith("//")) {
				return "http:" + suffix;
			}
			return suffix;
		}
		return System.getProperty(ElasticKeywordSearchBackend.PROP_ENDPOINT, "http://localhost:9200");
	}
}
