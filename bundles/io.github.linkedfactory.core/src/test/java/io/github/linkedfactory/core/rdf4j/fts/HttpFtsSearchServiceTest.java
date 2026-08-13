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
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HttpFtsSearchServiceTest {
	private final ObjectMapper mapper = new ObjectMapper();
	private final SimpleValueFactory vf = SimpleValueFactory.getInstance();
	private HttpServer server;
	private AtomicReference<String> body;
	private AtomicInteger requests;
	private volatile int responseCode = 200;

	@Before
	public void setUp() throws IOException {
		body = new AtomicReference<>();
		requests = new AtomicInteger(0);
		server = HttpServer.create(new InetSocketAddress(0), 0);
		server.createContext("/_bulk", this::handleRequest);
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
		HttpFtsSearchService service = new HttpFtsSearchService(
				endpoint(),
				"/_bulk",
				true,
				outboxDir.toString());


		Statement added = vf.createStatement(
				vf.createIRI("urn:sensor1"),
				vf.createIRI("urn:label"),
				vf.createLiteral("Battery Sensor"));
		Statement removed = vf.createStatement(
				vf.createIRI("urn:sensor1"),
				vf.createIRI("urn:locatedIn"),
				vf.createIRI("urn:lineA"));
		Set<Statement> addSet = new LinkedHashSet<>(Collections.singleton(added));
		Set<Statement> removeSet = new LinkedHashSet<>(Collections.singleton(removed));

		service.begin();
		service.addRemoveStatements(addSet, removeSet);
		service.commit();
		service.shutdown();

		assertEquals(1, requests.get());
		String[] lines = body.get().strip().split("\\n");
		assertEquals(4, lines.length);

		JsonNode upsertAction = mapper.readTree(lines[0]);
		JsonNode upsertPayload = mapper.readTree(lines[1]);
		JsonNode removeAction = mapper.readTree(lines[2]);
		JsonNode removePayload = mapper.readTree(lines[3]);

		assertEquals("urn:sensor1", upsertAction.path("update").path("_id").asText());
		assertEquals("urn:sensor1", removeAction.path("update").path("_id").asText());
		assertTrue(upsertPayload.path("doc_as_upsert").asBoolean());
		assertEquals("Battery Sensor",
				upsertPayload.path("doc")
						.get("urn:label").get(0).get("value").asText());
		assertEquals("urn:lineA",
				removePayload.path("script").path("params").path("fields")
						.get("urn:locatedIn").get(0).get("value").asText());
	}

	@Test
	public void commitCoalescesStatementBatchesForSameDocument() throws Exception {
		Path outboxDir = Files.createTempDirectory("fts-outbox-test");
		HttpFtsSearchService service = new HttpFtsSearchService(
				endpoint(),
				"/_bulk",
				true,
				outboxDir.toString());

		Statement first = vf.createStatement(
				vf.createIRI("urn:sensor1"),
				vf.createIRI("urn:label"),
				vf.createLiteral("Battery Sensor"));
		Statement second = vf.createStatement(
				vf.createIRI("urn:sensor1"),
				vf.createIRI("urn:label"),
				vf.createLiteral("Temperature Sensor"));

		service.begin();
		service.addRemoveStatements(Set.of(first), Set.of());
		service.addRemoveStatements(Set.of(second), Set.of());
		service.commit();
		service.shutdown();

		assertEquals(1, requests.get());
		String[] lines = body.get().strip().split("\\n");
		assertEquals(2, lines.length);

		JsonNode action = mapper.readTree(lines[0]);
		JsonNode payload = mapper.readTree(lines[1]);
		assertEquals("urn:sensor1", action.path("update").path("_id").asText());
		assertEquals(2, payload.path("doc").path("urn:label").size());
		assertEquals("Battery Sensor", payload.path("doc").path("urn:label").get(0).path("value").asText());
		assertEquals("Temperature Sensor", payload.path("doc").path("urn:label").get(1).path("value").asText());
	}

	@Test
	public void rollbackSkipsRequest() throws Exception {
		Path outboxDir = Files.createTempDirectory("fts-outbox-test");
		HttpFtsSearchService service = new HttpFtsSearchService(
				endpoint(),
				"/_bulk",
				true,
				outboxDir.toString());

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
		HttpFtsSearchService service = new HttpFtsSearchService(
				endpoint(),
				"/_bulk",
				true,
				outboxDir.toString());

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
		HttpFtsSearchService service = new HttpFtsSearchService(
				endpoint(),
				"/_bulk",
				false,
				outboxDir.toString());
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
		HttpFtsSearchService service = new HttpFtsSearchService(
				endpoint(),
				"/_bulk",
				true,
				outboxDir.toString());

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
		}

		assertEquals(3, requests.get());
		try (var files = Files.list(outboxDir)) {
			assertTrue(files.findAny().isPresent());
		}

		responseCode = 200;
		service.commit();
		service.shutdown();

		assertEquals(4, requests.get());
		try (var files = Files.list(outboxDir)) {
			assertFalse(files.findAny().isPresent());
		}
	}

	@Test
	public void restartsAndDrainsPersistedOutbox() throws Exception {
		Path outboxDir = Files.createTempDirectory("fts-outbox-test");
		HttpFtsSearchService first = new HttpFtsSearchService(
				endpoint(),
				"/_bulk",
				true,
				outboxDir.toString());

		responseCode = 500;
		first.begin();
		first.addRemoveStatements(Set.of(vf.createStatement(
				vf.createIRI("urn:s"),
				vf.createIRI("urn:p"),
				vf.createLiteral("x"))), Set.of());
		try {
			first.commit();
			fail("Expected exception on HTTP error with failOnError=true");
		} catch (IOException expected) {
			assertTrue(expected.getMessage().contains("HTTP 500"));
		} finally {
			first.shutdown();
		}

		assertEquals(3, requests.get());
		try (var files = Files.list(outboxDir)) {
			assertTrue(files.findAny().isPresent());
		}

		responseCode = 200;
		HttpFtsSearchService restarted = new HttpFtsSearchService(
				endpoint(),
				"/_bulk",
				true,
				outboxDir.toString());
		restarted.commit();
		restarted.shutdown();

		assertEquals(4, requests.get());
		try (var files = Files.list(outboxDir)) {
			assertFalse(files.findAny().isPresent());
		}
	}

	@Test
	public void drainsLegacyOutboxPayload() throws Exception {
		Path outboxDir = Files.createTempDirectory("fts-outbox-test");
		Files.writeString(outboxDir.resolve("legacy.json"),
				"{\"operations\":["
						+ "{\"op\":\"upsert\",\"documents\":{\"urn:sensor1\":{\"urn:label\":[{\"kind\":\"literal\",\"value\":\"Battery Sensor\"}]}}},"
						+ "{\"op\":\"remove\",\"documents\":{\"urn:sensor1\":{\"urn:locatedIn\":[{\"kind\":\"iri\",\"value\":\"urn:lineA\"}]}}}"
						+ "]}",
				StandardCharsets.UTF_8);

		HttpFtsSearchService service = new HttpFtsSearchService(
				endpoint(),
				"/_bulk",
				true,
				outboxDir.toString());
		service.commit();
		service.shutdown();

		assertEquals(1, requests.get());
		String[] lines = body.get().strip().split("\\n");
		assertEquals(4, lines.length);
		assertEquals("Battery Sensor",
				mapper.readTree(lines[1]).path("doc").path("urn:label").get(0).path("value").asText());
		assertEquals("urn:lineA",
				mapper.readTree(lines[3]).path("script").path("params").path("fields")
						.path("urn:locatedIn").get(0).path("value").asText());
	}

	@Test
	public void retriesTransientHttpFailuresBeforeSucceeding() throws Exception {
		AtomicInteger attempt = new AtomicInteger();
		HttpServer retryServer = HttpServer.create(new InetSocketAddress(0), 0);
		retryServer.createContext("/_bulk", exchange -> {
			int current = attempt.incrementAndGet();
			try (InputStream in = exchange.getRequestBody()) {
				body.set(new String(in.readAllBytes(), StandardCharsets.UTF_8));
			}
			int status = current < 3 ? 503 : 200;
			byte[] response = current < 3 ? "retry".getBytes(StandardCharsets.UTF_8) : "ok".getBytes(StandardCharsets.UTF_8);
			exchange.sendResponseHeaders(status, response.length);
			exchange.getResponseBody().write(response);
			exchange.close();
		});
		retryServer.start();

		try {
			HttpFtsSearchService service = new HttpFtsSearchService(
					"http://127.0.0.1:" + retryServer.getAddress().getPort(),
					"/_bulk",
					true,
					Files.createTempDirectory("fts-outbox-test").toString());

			service.begin();
			service.addRemoveStatements(Set.of(vf.createStatement(
					vf.createIRI("urn:s"),
					vf.createIRI("urn:p"),
					vf.createLiteral("x"))), Set.of());
			service.commit();
			service.shutdown();

			assertEquals(3, attempt.get());
		} finally {
			retryServer.stop(0);
		}
	}

	private String endpoint() {
		return "http://127.0.0.1:" + server.getAddress().getPort();
	}

	private void handleRequest(HttpExchange exchange) throws IOException {
		requests.incrementAndGet();
		try (InputStream in = exchange.getRequestBody()) {
			body.set(new String(in.readAllBytes(), StandardCharsets.UTF_8));
		}
		byte[] response = "ok".getBytes(StandardCharsets.UTF_8);
		exchange.sendResponseHeaders(responseCode, response.length);
		exchange.getResponseBody().write(response);
		exchange.close();
	}
}
