package io.github.linkedfactory.core.rdf4j.fts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ElasticKeywordSearchBackendTest {
	private final ObjectMapper mapper = new ObjectMapper();
	private HttpServer server;
	private AtomicReference<String> body;

	@Before
	public void setUp() throws IOException {
		body = new AtomicReference<>();
		server = HttpServer.create(new InetSocketAddress(0), 0);
		server.createContext("/fts/_search", this::handleRequest);
		server.start();
	}

	@After
	public void tearDown() {
		if (server != null) {
			server.stop(0);
		}
	}

	@Test
	public void translatesFtsRequestToElasticQueryAndParsesHits() throws Exception {
		ElasticKeywordSearchBackend backend = new ElasticKeywordSearchBackend(endpoint(), "/fts/_search", true, 100);

		FtsSearchRequest request = new FtsSearchRequest("motor data", "urn:label", 3, 1.5, true, "urn:item1");
		List<FtsSearchHit> hits = backend.search(request);
		backend.close();

		Assert.assertEquals(1, hits.size());
		Assert.assertEquals("urn:item1", hits.get(0).getIri());
		Assert.assertEquals(0.87d, hits.get(0).getScore(), 1e-6);
		Assert.assertEquals("matched snippet", hits.get(0).getSnippet());

		JsonNode payload = mapper.readTree(body.get());
		Assert.assertEquals(3, payload.get("size").asInt());
		Assert.assertEquals("motor data", payload.path("query").path("bool").path("must").get(0)
				.path("query_string").path("query").asText());
		Assert.assertEquals("urn:label", payload.path("query").path("bool").path("must").get(0)
				.path("query_string").path("default_field").asText());
		Assert.assertEquals("urn:item1", payload.path("query").path("bool").path("filter").get(0)
				.path("term").path("_id").asText());
	}

	@Test
	public void retriesTransientHttpFailuresBeforeReturningHits() throws Exception {
		AtomicInteger attempts = new AtomicInteger();
		HttpServer retryServer = HttpServer.create(new InetSocketAddress(0), 0);
		retryServer.createContext("/fts/_search", exchange -> {
			int current = attempts.incrementAndGet();
			try (InputStream in = exchange.getRequestBody()) {
				body.set(new String(in.readAllBytes(), StandardCharsets.UTF_8));
			}
			byte[] response = current < 3 ? "{}".getBytes(StandardCharsets.UTF_8) : """
					{
					  "hits": {
					    "hits": [
					      {
					        "_id": "urn:item1",
					        "_score": 0.87,
					        "highlight": {
					          "urn:label": ["matched snippet"]
					        }
					      }
					    ]
					  }
					}
					""".getBytes(StandardCharsets.UTF_8);
			int status = current < 3 ? 503 : 200;
			exchange.sendResponseHeaders(status, response.length);
			exchange.getResponseBody().write(response);
			exchange.close();
		});
		retryServer.start();

		try {
			ElasticKeywordSearchBackend backend = new ElasticKeywordSearchBackend(
					"http://127.0.0.1:" + retryServer.getAddress().getPort(),
					"/fts/_search",
					true,
					100);
			FtsSearchRequest request = new FtsSearchRequest("motor data", "urn:label", 3, 1.5, true, "urn:item1");
			List<FtsSearchHit> hits = backend.search(request);
			backend.close();

			Assert.assertEquals(3, attempts.get());
			Assert.assertEquals(1, hits.size());
			Assert.assertEquals("urn:item1", hits.get(0).getIri());
		} finally {
			retryServer.stop(0);
		}
	}

	private String endpoint() {
		return "http://127.0.0.1:" + server.getAddress().getPort();
	}

	private void handleRequest(HttpExchange exchange) throws IOException {
		try (InputStream in = exchange.getRequestBody()) {
			body.set(new String(in.readAllBytes(), StandardCharsets.UTF_8));
		}

		byte[] response = """
				{
				  "hits": {
				    "hits": [
				      {
				        "_id": "urn:item1",
				        "_score": 0.87,
				        "highlight": {
				          "urn:label": ["matched snippet"]
				        }
				      }
				    ]
				  }
				}
				""".getBytes(StandardCharsets.UTF_8);
		exchange.sendResponseHeaders(200, response.length);
		exchange.getResponseBody().write(response);
		exchange.close();
	}
}
