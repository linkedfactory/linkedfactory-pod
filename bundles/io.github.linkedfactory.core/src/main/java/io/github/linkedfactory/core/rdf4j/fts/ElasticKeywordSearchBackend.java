package io.github.linkedfactory.core.rdf4j.fts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class ElasticKeywordSearchBackend implements FtsSearchBackend {
	private static final Logger logger = LoggerFactory.getLogger(ElasticKeywordSearchBackend.class);

	public static final String PROP_ENDPOINT = "fts.endpoint";
	public static final String PROP_SEARCH_PATH = "fts.searchPath";
	public static final String PROP_FAIL_ON_ERROR = "fts.failOnError";
	public static final String PROP_DEFAULT_LIMIT = "fts.defaultLimit";

	private final ObjectMapper mapper = new ObjectMapper();
	private final String endpoint;
	private final String searchPath;
	private final boolean failOnError;
	private final int defaultLimit;
	private final RequestConfig requestConfig = RequestConfig.custom()
			.setConnectTimeout(5_000)
			.setConnectionRequestTimeout(5_000)
			.setSocketTimeout(30_000)
			.build();
	private volatile CloseableHttpClient httpClient;

	public ElasticKeywordSearchBackend(String endpoint) {
		this(endpoint, "/fts/_search", true, 100);
	}

	public ElasticKeywordSearchBackend(String endpoint, String searchPath, boolean failOnError, int defaultLimit) {
		this.endpoint = normalizeEndpoint(endpoint);
		this.searchPath = normalizePath(searchPath, "/fts/_search");
		this.failOnError = failOnError;
		this.defaultLimit = Math.max(defaultLimit, 1);
	}

	@Override
	/*
	  Performs a search query against the configured FTS backend.

	  @param request The search request containing keywords, field, limit, boost, snippet inclusion, and IRI filter.
	 * @return A list of search hits matching the query.
	 * @throws Exception If an error occurs during the search operation.

	 */
	public List<FtsSearchHit> search(FtsSearchRequest request) throws Exception {
		if (endpoint.isEmpty()) {
			return Collections.emptyList();
		}

		ObjectNode payload = mapper.createObjectNode();
		payload.put("size", request.getLimit() > 0 ? request.getLimit() : defaultLimit);
		payload.put("_source", false);
		ObjectNode query = payload.putObject("query");
		ObjectNode boolQuery = query.putObject("bool");
		ArrayNode must = boolQuery.putArray("must");
		ObjectNode queryString = must.addObject().putObject("query_string");
		queryString.put("query", request.getKeywords());
		if (request.getField() != null && !request.getField().isBlank()) {
			queryString.put("default_field", request.getField());
		}
		if (request.getBoost() != null) {
			queryString.put("boost", request.getBoost());
		}
		if (request.getIriFilter() != null && !request.getIriFilter().isBlank()) {
			boolQuery.putArray("filter")
					.addObject()
					.putObject("term")
					.put("_id", request.getIriFilter());
		}

		if (request.isIncludeSnippet()) {
			payload.putObject("highlight").putObject("fields").putObject("*");
		}

		String url = endpoint + searchPath;
		HttpPost httpPost = new HttpPost(url);
		httpPost.setConfig(requestConfig);
		httpPost.setEntity(new StringEntity(mapper.writeValueAsString(payload), ContentType.APPLICATION_JSON));

		try (CloseableHttpResponse response = client().execute(httpPost)) {
			int status = response.getStatusLine().getStatusCode();
			HttpEntity entity = response.getEntity();
			String body = entity == null ? "" : EntityUtils.toString(entity, StandardCharsets.UTF_8);
			if (status >= 300) {
				IOException error = new IOException("HTTP " + status + " while querying FTS backend: " + body);
				if (failOnError) {
					throw error;
				}
				logger.error("Ignoring FTS query failure because {}=false", PROP_FAIL_ON_ERROR, error);
				return Collections.emptyList();
			}
			return parseHits(body);
		}
	}

	@Override
	public void close() throws Exception {
		CloseableHttpClient client = httpClient;
		httpClient = null;
		if (client != null) {
			client.close();
		}
	}

	private CloseableHttpClient client() {
		CloseableHttpClient existing = httpClient;
		if (existing != null) {
			return existing;
		}
		synchronized (this) {
			if (httpClient == null) {
				httpClient = HttpClients.custom()
						.setDefaultRequestConfig(requestConfig)
						.disableAutomaticRetries()
						.build();
			}
			return httpClient;
		}
	}

	private List<FtsSearchHit> parseHits(String body) throws IOException {
		JsonNode root = mapper.readTree(body);
		JsonNode hitsArray = root.path("hits").path("hits");
		if (hitsArray.isArray()) {
			List<FtsSearchHit> hits = new ArrayList<>(hitsArray.size());
			for (JsonNode hit : hitsArray) {
				String iri = textOrNull(hit.get("_id"));
				if (iri == null) {
					iri = textOrNull(hit.path("_source").get("iri"));
				}
				if (iri == null) {
					continue;
				}
				Double score = hit.has("_score") && !hit.get("_score").isNull() ? hit.get("_score").asDouble() : null;
				String snippet = firstSnippet(hit.path("highlight"));
				hits.add(new FtsSearchHit(iri, score, snippet));
			}
			return hits;
		}

		JsonNode results = root.path("results");
		if (results.isArray()) {
			List<FtsSearchHit> hits = new ArrayList<>(results.size());
			for (JsonNode result : results) {
				String iri = textOrNull(result.get("iri"));
				if (iri == null) {
					iri = textOrNull(result.get("id"));
				}
				if (iri == null) {
					continue;
				}
				Double score = result.has("score") && !result.get("score").isNull() ? result.get("score").asDouble() : null;
				String snippet = textOrNull(result.get("snippet"));
				hits.add(new FtsSearchHit(iri, score, snippet));
			}
			return hits;
		}

		return Collections.emptyList();
	}

	private String firstSnippet(JsonNode highlight) {
		if (!highlight.isObject()) {
			return null;
		}
		Iterator<JsonNode> values = highlight.elements();
		while (values.hasNext()) {
			JsonNode value = values.next();
			if (value.isArray() && value.size() > 0) {
				return textOrNull(value.get(0));
			}
		}
		return null;
	}

	private static String textOrNull(JsonNode node) {
		return node == null || node.isNull() ? null : node.asText();
	}

	private static String normalizeEndpoint(String endpoint) {
		if (endpoint == null || endpoint.isBlank()) {
			return "";
		}
		String trimmed = endpoint.trim();
		return trimmed.endsWith("/") ? trimmed.substring(0, trimmed.length() - 1) : trimmed;
	}

	private static String normalizePath(String path, String defaultPath) {
		String value = Objects.requireNonNullElse(path, defaultPath).trim();
		if (value.isEmpty()) {
			value = defaultPath;
		}
		return value.startsWith("/") ? value : "/" + value;
	}
}
