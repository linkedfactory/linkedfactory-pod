package io.github.linkedfactory.core.rdf4j.fts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Locale;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class ModelsFtsLifecycleIntegrationTest {
	private final HttpClient http = HttpClient.newBuilder()
			.connectTimeout(Duration.ofSeconds(10))
			.build();
	private final ObjectMapper mapper = new ObjectMapper();

	private String podEndpoint;
	private String elasticsearchEndpoint;
	private String indexName;
	private String token;
	private long tripleCount;
	private long chunkTriples;
	private boolean verifyEachChunk;
	private Duration requestTimeout;
	private String modelUri;
	private boolean modelMayExist;
	private Path rdfFile;

	@Before
	public void setUp() {
		podEndpoint = setting("models.integration.pod", "POD_INTEGRATION_BASE_URL");
		elasticsearchEndpoint = setting("models.integration.elasticsearch", "FTS_INTEGRATION_ENDPOINT");
		Assume.assumeTrue("Set POD_INTEGRATION_BASE_URL and FTS_INTEGRATION_ENDPOINT to enable this test",
				podEndpoint != null && !podEndpoint.isBlank()
						&& elasticsearchEndpoint != null && !elasticsearchEndpoint.isBlank());

		podEndpoint = trimTrailingSlash(podEndpoint);
		elasticsearchEndpoint = trimTrailingSlash(elasticsearchEndpoint);
		indexName = settingOrDefault("models.integration.index", "linkedfactory");
		token = setting("models.integration.token", "POD_INTEGRATION_TOKEN");
		tripleCount = Long.parseLong(settingOrDefault("models.integration.triples", "100000"));
		chunkTriples = Long.parseLong(settingOrDefault("models.integration.chunkTriples", "0"));
		verifyEachChunk = Boolean.parseBoolean(settingOrDefault("models.integration.verifyEachChunk", "false"));
		if (tripleCount <= 0 || chunkTriples < 0) {
			throw new IllegalArgumentException("models.integration.triples must be positive and chunkTriples nonnegative");
		}
		requestTimeout = Duration.ofSeconds(Long.parseLong(
				settingOrDefault("models.integration.timeout.seconds", "600")));
		modelUri = "urn:linkedfactory:loadtest:" + UUID.randomUUID();
	}

	@After
	public void tearDown() throws Exception {
		try {
			if (modelMayExist) {
				HttpResponse<String> response = sendPod("DELETE", modelPath(), null, null);
				assertStatus(response, 200, 404);
			}
		} finally {
			if (rdfFile != null) {
				Files.deleteIfExists(rdfFile);
			}
		}
	}

	@Test
	public void uploadingAndDeletingLargeModelRemovesItsFtsDocuments() throws Exception {
		long uploadMillis = chunkTriples == 0 ? uploadSingleRequest() : uploadInChunks();
		assertModelCounts(tripleCount);

		long deleteStart = System.nanoTime();
		HttpResponse<String> delete = sendPod("DELETE", modelPath(), null, null);
		assertStatus(delete, 200);
		long deleteMillis = Duration.ofNanos(System.nanoTime() - deleteStart).toMillis();
		modelMayExist = false;

		HttpResponse<String> getDeletedModel = sendPod("GET", modelPath(), "text/turtle", null);
		assertStatus(getDeletedModel, 404);
		assertEquals("Deleting the model should remove all of its FTS documents", 0, elasticsearchCount(false));
		System.out.printf(Locale.ROOT, "Model FTS lifecycle: triples=%d chunkTriples=%d upload=%dms delete=%dms%n",
				tripleCount, chunkTriples, uploadMillis, deleteMillis);
	}

	private long uploadSingleRequest() throws Exception {
		return uploadRange(0, tripleCount);
	}

	private long uploadInChunks() throws Exception {
		long uploadMillis = 0;
		for (long first = 0; first < tripleCount;) {
			long count = Math.min(chunkTriples, tripleCount - first);
			uploadMillis += uploadRange(first, count);
			first += count;
			if (verifyEachChunk) {
				assertModelCounts(first);
			}
		}
		return uploadMillis;
	}

	private long uploadRange(long first, long count) throws Exception {
		rdfFile = createRdfFile(first, count);
		try {
			modelMayExist = true;
			long start = System.nanoTime();
			// POST appends to the same model; PUT would replace earlier chunks.
			HttpResponse<String> upload = sendPod("POST", modelPath(), "text/turtle",
					HttpRequest.BodyPublishers.ofFile(rdfFile));
			assertStatus(upload, 200);
			return Duration.ofNanos(System.nanoTime() - start).toMillis();
		} finally {
			Files.deleteIfExists(rdfFile);
			rdfFile = null;
		}
	}

	private void assertModelCounts(long expectedCount) throws Exception {
		HttpResponse<String> response = sendPod("GET",
				modelPath() + "&query=" + encode("SELECT (COUNT(*) AS ?count) WHERE { ?s <urn:linkedfactory:loadtest:label> ?o }"),
				"application/sparql-results+json", null);
		assertStatus(response, 200);
		JsonNode queryResult = mapper.readTree(response.body());
		String actualTripleCount = queryResult.path("results").path("bindings").path(0)
				.path("count").path("value").asText();
		assertEquals("Uploaded generated triple count", expectedCount, Long.parseLong(actualTripleCount));
		assertEquals("Every generated literal triple should have an FTS document",
				expectedCount, elasticsearchCount(true));
	}

	private Path createRdfFile(long first, long count) throws Exception {
		Path file = Files.createTempFile("linkedfactory-model-fts-", ".ttl");
		try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
			for (long offset = 0; offset < count; offset++) {
				long i = first + offset;
				writer.write("<urn:linkedfactory:loadtest:s:");
				writer.write(Long.toString(i));
				writer.write("> <urn:linkedfactory:loadtest:label> \"load test value ");
				writer.write(Long.toString(i));
				writer.write("\" .\n");
			}
		} catch (Exception e) {
			Files.deleteIfExists(file);
			throw e;
		}
		return file;
	}

	private long elasticsearchCount(boolean generatedStatementsOnly) throws Exception {
		sendExpecting("POST", elasticsearchEndpoint + "/" + indexName + "/_refresh", null, 200);
		String query = generatedStatementsOnly
				? "{\"bool\":{\"filter\":[{\"term\":{\"context\":\"" + modelUri
						+ "\"}},{\"term\":{\"predicate\":\"urn:linkedfactory:loadtest:label\"}}]}}"
				: "{\"term\":{\"context\":\"" + modelUri + "\"}}";
		JsonNode response = mapper.readTree(sendExpecting(
				"POST",
				elasticsearchEndpoint + "/" + indexName + "/_count",
				"{\"query\":" + query + "}",
				200).body());
		return response.path("count").asLong();
	}

	private String modelPath() {
		return "/models?model=" + encode(modelUri);
	}

	private HttpResponse<String> sendPod(String method, String path, String contentType,
			HttpRequest.BodyPublisher body) throws Exception {
		HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(podEndpoint + path))
				.timeout(requestTimeout)
				.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : body);
		if (contentType != null) {
			request.header(method.equals("GET") ? "Accept" : "Content-Type", contentType);
		}
		if (token != null && !token.isBlank()) {
			request.header("Authorization", "Bearer " + token);
		}
		return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
	}

	private HttpResponse<String> sendExpecting(String method, String url, String body, int expectedStatus)
			throws Exception {
		HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(url))
				.timeout(requestTimeout)
				.header("Content-Type", "application/json")
				.method(method, body == null
						? HttpRequest.BodyPublishers.noBody()
						: HttpRequest.BodyPublishers.ofString(body));
		HttpResponse<String> response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
		assertStatus(response, expectedStatus);
		return response;
	}

	private void assertStatus(HttpResponse<String> response, int... expectedStatuses) {
		for (int expectedStatus : expectedStatuses) {
			if (response.statusCode() == expectedStatus) {
				return;
			}
		}
		throw new AssertionError("Unexpected HTTP " + response.statusCode() + ": " + response.body());
	}

	private static String encode(String value) {
		return URLEncoder.encode(value, StandardCharsets.UTF_8);
	}

	private static String setting(String property, String environmentVariable) {
		String value = System.getProperty(property);
		return value == null || value.isBlank() ? System.getenv(environmentVariable) : value;
	}

	private static String settingOrDefault(String property, String defaultValue) {
		String value = System.getProperty(property);
		return value == null || value.isBlank() ? defaultValue : value;
	}

	private static String trimTrailingSlash(String value) {
		return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
	}
}
