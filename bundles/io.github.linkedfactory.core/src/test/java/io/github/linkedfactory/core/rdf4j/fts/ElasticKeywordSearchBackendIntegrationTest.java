package io.github.linkedfactory.core.rdf4j.fts;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ElasticKeywordSearchBackendIntegrationTest {
	private final ObjectMapper mapper = new ObjectMapper();
	private final HttpClient http = HttpClient.newBuilder()
			.connectTimeout(Duration.ofSeconds(5))
			.build();
	private String endpoint;
	private String indexName;

	@Before
	public void setUp() {
		endpoint = System.getProperty("fts.integration.endpoint");
		if (endpoint == null || endpoint.isBlank()) {
			endpoint = System.getenv("FTS_INTEGRATION_ENDPOINT");
		}
		Assume.assumeTrue(endpoint != null && !endpoint.isBlank());
		indexName = System.getProperty("fts.integration.index", "linkedfactory-fts-it-" + UUID.randomUUID());
	}

	@After
	public void tearDown() throws Exception {
		if (endpoint == null || endpoint.isBlank()) {
			return;
		}
		send("DELETE", "/" + indexName, null);
	}

	@Test
	public void queriesARealElasticSearchBackend() throws Exception {
		send("PUT", "/" + indexName, """
				{"settings":{"number_of_shards":1,"number_of_replicas":0}}
				""");
		send("PUT", "/" + indexName + "/_doc/urn:item1?refresh=true", """
				{"label":"motor data integration","category":"urn:label"}
				""");

		ElasticKeywordSearchBackend backend = new ElasticKeywordSearchBackend(
				endpoint,
				"/" + indexName + "/_search",
				true,
				10);
		try {
			List<FtsSearchHit> hits = backend.search(new FtsSearchRequest("motor data", "label", 10, null, true, null));
			assertFalse(hits.isEmpty());
			assertEquals("urn:item1", hits.get(0).getIri());
			assertTrue(hits.get(0).getScore() != null);
		} finally {
			backend.close();
		}
	}

	private void send(String method, String path, String body) throws Exception {
		HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(endpoint + path))
				.timeout(Duration.ofSeconds(30))
				.header("Content-Type", "application/json")
				.method(method, body == null
						? HttpRequest.BodyPublishers.noBody()
						: HttpRequest.BodyPublishers.ofString(body));
		HttpResponse<String> response = http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
		if (response.statusCode() >= 300) {
			throw new AssertionError("HTTP " + response.statusCode() + " for " + method + " " + path + ": " + response.body());
		}
	}
}
