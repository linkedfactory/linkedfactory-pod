package io.github.linkedfactory.core.rdf4j.fts.elastic;

import com.sun.net.httpserver.HttpServer;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ModelsFtsDeliveryWaitTest {
	@Test
	public void waitsForAsynchronousIndexingAndDeletion() throws Exception {
		AtomicInteger calls = new AtomicInteger();
		ModelsFtsLifecycleIntegrationTest.awaitCount(3, Duration.ofSeconds(2), Duration.ofMillis(1),
				remaining -> Math.min(calls.incrementAndGet(), 3));
		assertEquals(3, calls.get());

		calls.set(0);
		ModelsFtsLifecycleIntegrationTest.awaitCount(0, Duration.ofSeconds(2), Duration.ofMillis(1),
				remaining -> Math.max(0, 3 - calls.incrementAndGet()));
		assertEquals(3, calls.get());
	}

	@Test
	public void timeoutReportsExpectedAndLastObservedCounts() throws Exception {
		try {
			ModelsFtsLifecycleIntegrationTest.awaitCount(10, Duration.ofMillis(250), Duration.ofMillis(1),
					remaining -> 4);
			fail("Expected delivery timeout");
		} catch (AssertionError expected) {
			assertTrue(expected.getMessage().contains("expected document count 10"));
			assertTrue(expected.getMessage().contains("last observed count: 4"));
		}
	}

	@Test
	public void queryFailuresAreNotMistakenForEventualConsistency() throws Exception {
		IOException failure = new IOException("Elasticsearch unavailable");
		try {
			ModelsFtsLifecycleIntegrationTest.awaitCount(0, Duration.ofSeconds(2), Duration.ofMillis(1),
					remaining -> {
						throw failure;
					});
			fail("Expected Elasticsearch failure");
		} catch (IOException expected) {
			assertSame(failure, expected);
		}
	}

	@Test
	public void lifecyclePollsHttpCountsAfterUploadAndDeletion() throws Exception {
		HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		AtomicInteger counts = new AtomicInteger();
		AtomicInteger refreshes = new AtomicInteger();
		server.createContext("/models", exchange -> {
			exchange.getRequestBody().readAllBytes();
			String body = exchange.getRequestMethod().equals("GET") && exchange.getRequestURI().getQuery().contains("query=")
					? "{\"results\":{\"bindings\":[{\"count\":{\"value\":\"3\"}}]}}"
					: "";
			int status = exchange.getRequestMethod().equals("GET") && body.isEmpty() ? 404 : 200;
			byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
			exchange.sendResponseHeaders(status, bytes.length);
			try (var out = exchange.getResponseBody()) {
				out.write(bytes);
			}
		});
		server.createContext("/linkedfactory/_refresh", exchange -> {
			exchange.getRequestBody().readAllBytes();
			refreshes.incrementAndGet();
			exchange.sendResponseHeaders(200, -1);
			exchange.close();
		});
		server.createContext("/linkedfactory/_count", exchange -> {
			exchange.getRequestBody().readAllBytes();
			int[] results = { 0, 1, 3, 3, 1, 0 };
			int index = counts.getAndIncrement();
			byte[] bytes = ("{\"count\":" + results[Math.min(index, results.length - 1)] + "}")
					.getBytes(StandardCharsets.UTF_8);
			exchange.sendResponseHeaders(200, bytes.length);
			try (var out = exchange.getResponseBody()) {
				out.write(bytes);
			}
		});
		server.start();
		Map<String, String> settings = new LinkedHashMap<>();
		String endpoint = "http://127.0.0.1:" + server.getAddress().getPort();
		settings.put("models.integration.pod", endpoint);
		settings.put("models.integration.elasticsearch", endpoint);
		settings.put("models.integration.index", "linkedfactory");
		settings.put("models.integration.triples", "3");
		settings.put("models.integration.chunkTriples", "0");
		settings.put("models.integration.verifyEachChunk", "false");
		settings.put("models.integration.timeout.seconds", "5");
		settings.put("models.integration.delivery.timeout.seconds", "5");
		settings.put("models.integration.delivery.poll.millis", "1");
		Map<String, String> previous = new LinkedHashMap<>();
		settings.forEach((key, value) -> previous.put(key, System.getProperty(key)));
		settings.forEach(System::setProperty);
		ModelsFtsLifecycleIntegrationTest lifecycle = new ModelsFtsLifecycleIntegrationTest();
		try {
			lifecycle.setUp();
			lifecycle.uploadingAndDeletingLargeModelRemovesItsFtsDocuments();
			assertEquals(6, counts.get());
			assertEquals(6, refreshes.get());
		} finally {
			try {
				lifecycle.tearDown();
			} finally {
				previous.forEach((key, value) -> {
					if (value == null) {
						System.clearProperty(key);
					} else {
						System.setProperty(key, value);
					}
				});
				server.stop(0);
			}
		}
	}
}
