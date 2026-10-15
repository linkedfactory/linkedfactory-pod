/*
package io.github.linkedfactory.core.rdf4j.fts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ModelsFtsLifecycleChunkTest {
	private final ObjectMapper mapper = new ObjectMapper();
	private final Map<String, String> previousProperties = new HashMap<>();
	private final List<URI> uploads = new ArrayList<>();
	private final List<Integer> chunkSizes = new ArrayList<>();
	private final List<String> receivedLines = new ArrayList<>();
	private final List<Integer> modelCounts = new ArrayList<>();
	private final List<Integer> ftsCounts = new ArrayList<>();
	private final List<JsonNode> countQueries = new ArrayList<>();
	private final AtomicReference<Throwable> serverFailure = new AtomicReference<>();
	private HttpServer server;
	private String modelUri;
	private boolean modelExists;
	private int storedTriples;
	private int deletes;
	private int deletedModelGets;
	private int refreshes;
	private int failedUpload;

	@Before
	public void setUp() throws IOException {
		server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/", exchange -> {
			try {
				handle(exchange);
			} catch (Throwable failure) {
				serverFailure.compareAndSet(null, failure);
				reply(exchange, 500, failure.toString());
			} finally {
				exchange.close();
			}
		});
		server.start();
		String endpoint = "http://127.0.0.1:" + server.getAddress().getPort();
		setProperty("pod", endpoint);
		setProperty("elasticsearch", endpoint);
		setProperty("index", "chunk-test");
		setProperty("token", "chunk-test-token");
		setProperty("triples", "23");
		setProperty("chunkTriples", null);
		setProperty("verifyEachChunk", null);
		setProperty("timeout.seconds", "5");
	}

	@After
	public void tearDown() {
		if (server != null) {
			server.stop(0);
		}
		previousProperties.forEach((property, value) -> {
			if (value == null) {
				System.clearProperty(property);
			} else {
				System.setProperty(property, value);
			}
		});
		assertNull("Mock HTTP request validation failed: " + serverFailure.get(), serverFailure.get());
	}

	@Test
	public void chunksAppendUniqueSubjectsAndVerifyCumulativeCounts() throws Exception {
		setProperty("chunkTriples", "10");
		setProperty("verifyEachChunk", "true");
		runLifecycle();

		assertEquals(List.of(10, 10, 3), chunkSizes);
		assertEquals(List.of(10, 20, 23, 23), modelCounts);
		assertEquals(List.of(10, 20, 23, 23, 0), ftsCounts);
		assertSuccessfulLifecycle();
	}

	@Test
	public void chunksWithoutVerificationOnlyCheckFinalCounts() throws Exception {
		setProperty("chunkTriples", "10");
		runLifecycle();

		assertEquals(List.of(10, 10, 3), chunkSizes);
		assertEquals(List.of(23), modelCounts);
		assertEquals(List.of(23, 0), ftsCounts);
		assertSuccessfulLifecycle();
	}

	@Test
	public void defaultUploadUsesOneRequest() throws Exception {
		runLifecycle();

		assertEquals(List.of(23), chunkSizes);
		assertEquals(List.of(23), modelCounts);
		assertEquals(List.of(23, 0), ftsCounts);
		assertSuccessfulLifecycle();
	}

	@Test
	public void failedMiddleChunkStillDeletesPartialModel() throws Exception {
		setProperty("chunkTriples", "10");
		setProperty("verifyEachChunk", "true");
		failedUpload = 2;

		AssertionError failure = assertThrows(AssertionError.class, this::runLifecycle);
		assertTrue(failure.getMessage().contains("Unexpected HTTP 500"));
		assertEquals(List.of(10, 10), chunkSizes);
		assertEquals(List.of(10), modelCounts);
		assertEquals(List.of(10), ftsCounts);
		assertEquals(1, deletes);
		assertFalse(modelExists);
		assertEquals(0, storedTriples);
	}

	private void runLifecycle() throws Exception {
		ModelsFtsLifecycleIntegrationTest lifecycle = new ModelsFtsLifecycleIntegrationTest();
		try {
			lifecycle.setUp();
			lifecycle.uploadingAndDeletingLargeModelRemovesItsFtsDocuments();
		} finally {
			lifecycle.tearDown();
		}
	}

	private void assertSuccessfulLifecycle() {
		assertEquals(23, receivedLines.size());
		for (int i = 0; i < receivedLines.size(); i++) {
			assertEquals("<urn:linkedfactory:loadtest:s:" + i
					+ "> <urn:linkedfactory:loadtest:label> \"load test value " + i + "\" .",
					receivedLines.get(i));
		}
		assertTrue(uploads.stream().allMatch(uploads.get(0)::equals));
		assertEquals(1, deletes);
		assertEquals(1, deletedModelGets);
		assertFalse(modelExists);
		assertEquals(0, storedTriples);
		assertEquals(ftsCounts.size(), refreshes);
		for (int i = 0; i < countQueries.size() - 1; i++) {
			JsonNode filters = countQueries.get(i).path("bool").path("filter");
			assertEquals(modelUri, filters.path(0).path("term").path("context").asText());
			assertEquals("urn:linkedfactory:loadtest:label",
					filters.path(1).path("term").path("predicate").asText());
		}
		assertEquals(modelUri, countQueries.getLast().path("term").path("context").asText());
	}

	private void handle(HttpExchange exchange) throws IOException {
		String path = exchange.getRequestURI().getPath();
		String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
		if ("/models".equals(path)) {
			assertEquals("Bearer chunk-test-token", exchange.getRequestHeaders().getFirst("Authorization"));
			String query = URLDecoder.decode(exchange.getRequestURI().getRawQuery(), StandardCharsets.UTF_8);
			String requestedModel = query.substring("model=".length()).split("&", 2)[0];
			if (modelUri == null) {
				modelUri = requestedModel;
			}
			assertEquals(modelUri, requestedModel);
			switch (exchange.getRequestMethod()) {
				case "POST" -> {
					assertEquals("text/turtle", exchange.getRequestHeaders().getFirst("Content-Type"));
					uploads.add(exchange.getRequestURI());
					List<String> lines = body.lines().toList();
					chunkSizes.add(lines.size());
					if (uploads.size() == failedUpload) {
						reply(exchange, 500, "middle chunk rejected");
						return;
					}
					receivedLines.addAll(lines);
					storedTriples += lines.size();
					modelExists = true;
					reply(exchange, 200, "");
				}
				case "GET" -> {
					if (query.contains("&query=")) {
						assertTrue(query.contains("COUNT(*)"));
						assertTrue(query.contains("<urn:linkedfactory:loadtest:label>"));
						modelCounts.add(storedTriples);
						reply(exchange, 200, "{\"results\":{\"bindings\":[{\"count\":{\"value\":\""
								+ storedTriples + "\"}}]}}");
					} else {
						assertFalse(modelExists);
						deletedModelGets++;
						reply(exchange, 404, "");
					}
				}
				case "DELETE" -> {
					deletes++;
					storedTriples = 0;
					modelExists = false;
					reply(exchange, 200, "");
				}
				default -> fail("Unexpected model method: " + exchange.getRequestMethod());
			}
		} else {
			assertEquals("POST", exchange.getRequestMethod());
			if ("/chunk-test/_refresh".equals(path)) {
				refreshes++;
				reply(exchange, 200, "{}");
			} else {
				assertEquals("/chunk-test/_count", path);
				countQueries.add(mapper.readTree(body).path("query"));
				ftsCounts.add(storedTriples);
				reply(exchange, 200, "{\"count\":" + storedTriples + "}");
			}
		}
	}

	private static void reply(HttpExchange exchange, int status, String body) throws IOException {
		byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
		exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
		exchange.getResponseBody().write(bytes);
	}

	private void setProperty(String name, String value) {
		String property = "models.integration." + name;
		if (!previousProperties.containsKey(property)) {
			previousProperties.put(property, System.getProperty(property));
		}
		if (value == null) {
			System.clearProperty(property);
		} else {
			System.setProperty(property, value);
		}
	}
}
*/
