package io.github.linkedfactory.core.rdf4j.fts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HttpFtsSearchServiceTest {
	private static final String BULK_SUCCESS_RESPONSE = "{\"errors\":false,\"items\":[]}";

	private final ObjectMapper mapper = new ObjectMapper();
	private final SimpleValueFactory vf = SimpleValueFactory.getInstance();
	private HttpServer server;
	private AtomicReference<String> body;
	private AtomicReference<String> requestPath;
	private AtomicInteger requests;
	private volatile int responseCode = 200;
	private volatile String responseBody = BULK_SUCCESS_RESPONSE;

	@Before
	public void setUp() throws IOException {
		body = new AtomicReference<>();
		requestPath = new AtomicReference<>();
		requests = new AtomicInteger(0);
		server = HttpServer.create(new InetSocketAddress(0), 0);
		server.createContext("/_bulk", this::handleRequest);
		server.createContext("/_delete_by_query", this::handleRequest);
		server.start();
	}

	@After
	public void tearDown() {
		if (server != null) {
			server.stop(0);
		}
	}

	@Test
	public void commitSendsBatchedPayload() throws Exception {
		Path outboxDir = Files.createTempDirectory("fts-outbox-test");
		HttpFtsSearchService service = new HttpFtsSearchService(endpoint(), "/_bulk", true, outboxDir.toString());

		Statement added = vf.createStatement(
				vf.createIRI("urn:sensor1"),
				vf.createIRI("urn:label"),
				vf.createLiteral("Battery Sensor"));
		Statement removed = vf.createStatement(
				vf.createIRI("urn:sensor1"),
				vf.createIRI("urn:label"),
				vf.createLiteral("Battery Sensor"));

		service.begin();
		service.addRemoveStatements(Set.of(added), Set.of(removed));
		service.commit();
		service.shutdown();

		assertEquals(1, requests.get());
		String[] lines = body.get().strip().split("\\n");
		assertEquals(3, lines.length);

		JsonNode indexAction = mapper.readTree(lines[0]);
		JsonNode indexPayload = mapper.readTree(lines[1]);
		JsonNode deleteAction = mapper.readTree(lines[2]);

		String docId = indexAction.path("index").path("_id").asText();
		assertFalse(docId.isBlank());
		assertEquals(docId, deleteAction.path("delete").path("_id").asText());
		assertEquals("urn:sensor1", indexPayload.path("subject").asText());
		assertEquals("urn:label", indexPayload.path("predicate").asText());
		assertEquals("Battery Sensor", indexPayload.path("value").asText());
	}

	@Test
	public void commitCoalescesStatementBatchesForSameDocument() throws Exception {
		Path outboxDir = Files.createTempDirectory("fts-outbox-test");
		HttpFtsSearchService service = new HttpFtsSearchService(endpoint(), "/_bulk", true, outboxDir.toString());

		Statement first = vf.createStatement(
				vf.createIRI("urn:sensor1"),
				vf.createIRI("urn:label"),
				vf.createLiteral("Battery Sensor"));

		service.begin();
		service.addRemoveStatements(Set.of(first), Set.of());
		service.addRemoveStatements(Set.of(first), Set.of());
		service.commit();
		service.shutdown();

		assertEquals(1, requests.get());
		String[] lines = body.get().strip().split("\\n");
		assertEquals(2, lines.length);
		assertEquals(
				mapper.readTree(lines[0]).path("index").path("_id").asText(),
				mapper.readTree(lines[0]).path("index").path("_id").asText());
	}

	@Test
	public void rollbackSkipsRequest() throws Exception {
		Path outboxDir = Files.createTempDirectory("fts-outbox-test");
		HttpFtsSearchService service = new HttpFtsSearchService(endpoint(), "/_bulk", true, outboxDir.toString());

		service.begin();
		service.addRemoveStatements(Set.of(vf.createStatement(
				vf.createIRI("urn:s"),
				vf.createIRI("urn:p"),
				vf.createLiteral("x"))), Set.of());
		service.rollback();
		service.commit();
		service.shutdown();

		assertEquals(0, requests.get());
		try (var files = Files.list(outboxDir)) {
			assertFalse(files.findAny().isPresent());
		}
	}

	@Test
	public void shutdownDiscardsUncommittedPayload() throws Exception {
		Path outboxDir = Files.createTempDirectory("fts-outbox-test");
		HttpFtsSearchService service = new HttpFtsSearchService(endpoint(), "/_bulk", true, outboxDir.toString());

		service.begin();
		service.addRemoveStatements(Set.of(vf.createStatement(
				vf.createIRI("urn:s"),
				vf.createIRI("urn:p"),
				vf.createLiteral("x"))), Set.of());
		service.shutdown();

		assertEquals(0, requests.get());
		try (var files = Files.list(outboxDir)) {
			assertFalse(files.findAny().isPresent());
		}
	}

	@Test
	public void failOnErrorThrowsWhenEnabled() throws Exception {
		HttpFtsSearchService service = new HttpFtsSearchService();
		service.configure(Map.of(
				HttpFtsSearchService.PROP_ENDPOINT, endpoint(),
				HttpFtsSearchService.PROP_FAIL_ON_ERROR, "true"
		));
		responseCode = 500;

		service.begin();
		service.addRemoveStatements(Set.of(vf.createStatement(
				vf.createIRI("urn:s"),
				vf.createIRI("urn:p"),
				vf.createLiteral("x"))), Set.of());
		try {
			service.commit();
			fail("Expected exception on HTTP error with failOnError=true");
		} catch (IOException expected) {
			assertTrue(expected.getMessage().contains("HTTP 500"));
		} finally {
			service.shutdown();
		}
	}

	@Test
	public void failOnErrorCanBeDisabled() throws Exception {
		Path outboxDir = Files.createTempDirectory("fts-outbox-test");
		HttpFtsSearchService service = new HttpFtsSearchService(endpoint(), "/_bulk", false, outboxDir.toString());
		responseCode = 500;

		service.begin();
		service.addRemoveStatements(Set.of(vf.createStatement(
				vf.createIRI("urn:s"),
				vf.createIRI("urn:p"),
				vf.createLiteral("x"))), Set.of());
		service.commit();
		service.shutdown();

		assertEquals(3, requests.get());
		try (var files = Files.list(outboxDir)) {
			assertTrue(files.findAny().isPresent());
		}
	}

	@Test
	public void retriesPendingOutboxAfterFailedCommit() throws Exception {
		Path outboxDir = Files.createTempDirectory("fts-outbox-test");
		HttpFtsSearchService service = new HttpFtsSearchService(endpoint(), "/_bulk", true, outboxDir.toString());

		responseCode = 500;
		service.begin();
		service.addRemoveStatements(Set.of(vf.createStatement(
				vf.createIRI("urn:s"),
				vf.createIRI("urn:p"),
				vf.createLiteral("x"))), Set.of());
		try {
			service.commit();
			fail("Expected initial failure");
		} catch (IOException expected) {
			assertTrue(expected.getMessage().contains("HTTP 500"));
		}

		responseCode = 200;
		responseBody = BULK_SUCCESS_RESPONSE;
		service.commit();
		service.shutdown();

		assertEquals(4, requests.get());
		try (var files = Files.list(outboxDir)) {
			assertFalse(files.findAny().isPresent());
		}
	}

	@Test
	public void clearContextsUsesDeleteByQuery() throws Exception {
		Path outboxDir = Files.createTempDirectory("fts-outbox-test");
		HttpFtsSearchService service = new HttpFtsSearchService(endpoint(), "/_bulk", true, outboxDir.toString());

		service.begin();
		service.clearContexts(vf.createIRI("urn:ctx:A"), vf.createIRI("urn:ctx:B"));
		service.commit();
		service.shutdown();

		assertEquals("/_delete_by_query", requestPath.get());
		JsonNode payload = mapper.readTree(body.get());
		assertTrue(payload.path("query").path("terms").has("context"));
	}

	private String endpoint() {
		return "http://127.0.0.1:" + server.getAddress().getPort();
	}

	private void handleRequest(HttpExchange exchange) throws IOException {
		requests.incrementAndGet();
		requestPath.set(exchange.getRequestURI().getPath());
		try (InputStream in = exchange.getRequestBody()) {
			body.set(new String(in.readAllBytes(), StandardCharsets.UTF_8));
		}

		byte[] response = responseBody.getBytes(StandardCharsets.UTF_8);
		exchange.sendResponseHeaders(responseCode, response.length);
		exchange.getResponseBody().write(response);
		exchange.close();
	}
}
