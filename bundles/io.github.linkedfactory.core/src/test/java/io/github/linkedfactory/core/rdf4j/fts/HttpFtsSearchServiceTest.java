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
		server.createContext("/fts/bulk", this::handleRequest);
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
				"/fts/bulk",
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
		//System.out.println(body.get());
		JsonNode payload = mapper.readTree(body.get());
		assertTrue(payload.has("operations"));
		assertEquals(2, payload.get("operations").size());
		assertEquals("upsert", payload.get("operations").get(0).get("op").asText());
		assertEquals("remove", payload.get("operations").get(1).get("op").asText());
		assertEquals("Battery Sensor",
				payload.get("operations").get(0)
						.get("documents").get("urn:sensor1")
						.get("urn:label").get(0).get("value").asText());
		assertEquals("urn:lineA",
				payload.get("operations").get(1)
						.get("documents").get("urn:sensor1")
						.get("urn:locatedIn").get(0).get("value").asText());
	}

	@Test
	public void rollbackSkipsRequest() throws Exception {
		Path outboxDir = Files.createTempDirectory("fts-outbox-test");
		HttpFtsSearchService service = new HttpFtsSearchService(
				endpoint(),
				"/fts/bulk",
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
		HttpFtsSearchService service = new HttpFtsSearchService();
		service.configure(Map.of(
				HttpFtsSearchService.PROP_ENDPOINT, endpoint(),
				HttpFtsSearchService.PROP_FAIL_ON_ERROR, "false"
		));
		responseCode = 500;

		service.begin();
		service.addRemoveStatements(Set.of(vf.createStatement(
				vf.createIRI("urn:s"),
				vf.createIRI("urn:p"),
				vf.createLiteral("x"))), Set.of());
		service.commit();
		service.shutdown();

		assertEquals(1, requests.get());
	}

	@Test
	public void retriesPendingOutboxAfterFailedCommit() throws Exception {
		Path outboxDir = Files.createTempDirectory("fts-outbox-test");
		HttpFtsSearchService service = new HttpFtsSearchService(
				endpoint(),
				"/fts/bulk",
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

		assertEquals(1, requests.get());
		try (var files = Files.list(outboxDir)) {
			assertTrue(files.findAny().isPresent());
		}

		responseCode = 200;
		service.commit();
		service.shutdown();

		assertEquals(2, requests.get());
		try (var files = Files.list(outboxDir)) {
			assertFalse(files.findAny().isPresent());
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
