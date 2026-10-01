package io.github.linkedfactory.core.rdf4j.fts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HttpFtsSearchServiceIntegrationTest {
	private final ObjectMapper mapper = new ObjectMapper();
	private final HttpClient http = HttpClient.newBuilder()
			.connectTimeout(Duration.ofSeconds(5))
			.build();
	private final SimpleValueFactory vf = SimpleValueFactory.getInstance();

	private String endpoint;
	private String indexName;
	private HttpFtsSearchService searchService;
	private SailRepository repository;
	private Path outboxDir;

	@Before
	public void setUp() {
		endpoint = System.getProperty("fts.integration.endpoint");
		if (endpoint == null || endpoint.isBlank()) {
			endpoint = System.getenv("FTS_INTEGRATION_ENDPOINT");
		}
		Assume.assumeTrue(endpoint != null && !endpoint.isBlank());
		indexName = System.getProperty("fts.integration.index", "linkedfactory-fts-sync-it-" + UUID.randomUUID());
	}

	@After
	public void tearDown() throws Exception {
		if (repository != null) {
			repository.shutDown();
			repository = null;
		}
		if (searchService != null) {
			searchService.shutdown();
			searchService = null;
		}
		if (endpoint != null && !endpoint.isBlank() && indexName != null && !indexName.isBlank()) {
			deleteIndexIfExists();
		}
		if (outboxDir != null) {
			try (var files = Files.list(outboxDir)) {
				files.forEach(path -> {
					try {
						Files.deleteIfExists(path);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				});
			}
			Files.deleteIfExists(outboxDir);
			outboxDir = null;
		}
	}

	@Test
	public void syncsLiteralStatementsAsIndependentDocuments() throws Exception {
		initRepository(defaultIndexDefinition());

		try (SailRepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			connection.add(vf.createIRI("urn:sensor1"), vf.createIRI("urn:label"), vf.createLiteral("alpha"));
			connection.add(vf.createIRI("urn:sensor1"), vf.createIRI("urn:label"), vf.createLiteral("beta"));
			connection.add(vf.createIRI("urn:sensor1"), vf.createIRI("urn:related"), vf.createIRI("urn:lineA"));
			connection.commit();
		}

		refreshIndex();
		assertEquals(2, documentCount());
		assertEquals(Set.of("alpha", "beta"), valuesForSubject("urn:sensor1"));

		try (SailRepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			connection.remove(vf.createIRI("urn:sensor1"), vf.createIRI("urn:label"), vf.createLiteral("alpha"));
			connection.add(vf.createIRI("urn:sensor1"), vf.createIRI("urn:label"), vf.createLiteral("gamma"));
			connection.commit();
		}

		refreshIndex();
		assertEquals(2, documentCount());
		assertEquals(Set.of("beta", "gamma"), valuesForSubject("urn:sensor1"));

		try (SailRepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			connection.remove(vf.createIRI("urn:sensor1"), vf.createIRI("urn:label"), vf.createLiteral("beta"));
			connection.remove(vf.createIRI("urn:sensor1"), vf.createIRI("urn:label"), vf.createLiteral("gamma"));
			connection.commit();
		}

		refreshIndex();
		assertTrue(valuesForSubject("urn:sensor1").isEmpty());
		assertEquals(0, documentCount());
	}

	@Test
	public void clearContextsRemovesMatchingDocuments() throws Exception {
		initRepository(defaultIndexDefinition());

		IRI ctxA = vf.createIRI("urn:ctx:A");
		IRI ctxB = vf.createIRI("urn:ctx:B");

		try (SailRepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			connection.add(vf.createIRI("urn:sensor1"), vf.createIRI("urn:label"), vf.createLiteral("alpha"), ctxA);
			connection.add(vf.createIRI("urn:sensor1"), vf.createIRI("urn:label"), vf.createLiteral("beta"), ctxB);
			connection.add(vf.createIRI("urn:sensor2"), vf.createIRI("urn:label"), vf.createLiteral("ctx-only"), ctxA);
			connection.commit();
		}

		refreshIndex();
		assertEquals(3, documentCount());

		try (SailRepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			connection.clear(ctxA);
			connection.commit();
		}

		refreshIndex();
		assertEquals(Set.of("beta"), valuesForSubject("urn:sensor1"));
		assertTrue(valuesForSubject("urn:sensor2").isEmpty());
		assertEquals(1, documentCount());
	}

	@Test
	public void clearRemovesAllIndexedDocuments() throws Exception {
		initRepository(defaultIndexDefinition());

		try (SailRepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			connection.add(vf.createIRI("urn:sensor1"), vf.createIRI("urn:label"), vf.createLiteral("alpha"));
			connection.add(vf.createIRI("urn:sensor2"), vf.createIRI("urn:label"), vf.createLiteral("beta"));
			connection.commit();
		}

		refreshIndex();
		assertEquals(2, documentCount());

		try (SailRepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			connection.clear();
			connection.commit();
		}

		refreshIndex();
		assertEquals(0, documentCount());
	}

	@Test
	public void bulkItemErrorsSurfaceAndPreserveOutbox() throws Exception {
		initRepository(numberValueMapping());

		try (SailRepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			connection.add(vf.createIRI("urn:ok"), vf.createIRI("urn:number"), vf.createLiteral("42"));
			connection.add(vf.createIRI("urn:bad"), vf.createIRI("urn:number"), vf.createLiteral("not-a-number"));
			try {
				connection.commit();
				fail("Expected bulk item failure");
			} catch (Exception expected) {
				assertTrue(containsMessage(expected, "bulk index"));
			}
		}

		try (var files = Files.list(outboxDir)) {
			assertTrue(files.findAny().isPresent());
		}

		deleteIndexIfExists();
		createIndex(defaultIndexDefinition());
		searchService.commit();

		refreshIndex();
		assertEquals(Set.of("42"), valuesForSubject("urn:ok"));
		assertEquals(Set.of("not-a-number"), valuesForSubject("urn:bad"));
		try (var files = Files.list(outboxDir)) {
			assertTrue(files.findAny().isEmpty());
		}
	}

	private void initRepository(String indexDefinition) throws Exception {
		createIndex(indexDefinition);
		outboxDir = Files.createTempDirectory("fts-es-it-outbox");
		searchService = new HttpFtsSearchService(endpoint, "/" + indexName + "/_bulk", true, outboxDir.toString());
		repository = new SailRepository(new FtsSail(searchService, new MemoryStore()));
		repository.init();
	}

	private String defaultIndexDefinition() {
		return """
				{
				  "settings": {
				    "number_of_shards": 1,
				    "number_of_replicas": 0
				  },
				  "mappings": {
				    "dynamic": "strict",
				    "properties": {
				      "subject": { "type": "keyword" },
				      "predicate": { "type": "keyword" },
				      "value": { "type": "text" },
				      "context": { "type": "keyword" },
				      "datatype": { "type": "keyword" },
				      "language": { "type": "keyword" },
				      "sortValue": { "type": "keyword" }
				    }
				  }
				}
				""";
	}

	private String numberValueMapping() {
		return """
				{
				  "settings": {
				    "number_of_shards": 1,
				    "number_of_replicas": 0
				  },
				  "mappings": {
				    "dynamic": "strict",
				    "properties": {
				      "subject": { "type": "keyword" },
				      "predicate": { "type": "keyword" },
				      "value": { "type": "long" },
				      "context": { "type": "keyword" },
				      "datatype": { "type": "keyword" },
				      "language": { "type": "keyword" },
				      "sortValue": { "type": "keyword" }
				    }
				  }
				}
				""";
	}

	private void createIndex(String definition) throws Exception {
		sendExpecting("PUT", "/" + indexName, definition, 200);
	}

	private void refreshIndex() throws Exception {
		sendExpecting("POST", "/" + indexName + "/_refresh", null, 200);
	}

	private long documentCount() throws Exception {
		JsonNode response = sendExpecting("GET", "/" + indexName + "/_count", null, 200);
		return response.path("count").asLong();
	}

	private Set<String> valuesForSubject(String subject) throws Exception {
		JsonNode response = sendExpecting("POST", "/" + indexName + "/_search", """
				{
				  "size": 100,
				  "_source": ["value"],
				  "query": {
				    "term": {
				      "subject": "%s"
				    }
				  }
				}
				""".formatted(subject), 200);
		Set<String> values = new LinkedHashSet<>();
		for (JsonNode hit : response.path("hits").path("hits")) {
			values.add(hit.path("_source").path("value").asText());
		}
		return values;
	}

	private JsonNode sendExpecting(String method, String path, String body, int... expectedStatuses) throws Exception {
		HttpResponse<String> response = sendRaw(method, path, body);
		for (int expectedStatus : expectedStatuses) {
			if (response.statusCode() == expectedStatus) {
				return response.body().isBlank() ? mapper.createObjectNode() : mapper.readTree(response.body());
			}
		}
		throw new AssertionError("HTTP " + response.statusCode() + " for " + method + " " + path + ": " + response.body());
	}

	private HttpResponse<String> sendRaw(String method, String path, String body) throws Exception {
		HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(endpoint + path))
				.timeout(Duration.ofSeconds(30))
				.header("Content-Type", "application/json")
				.method(method, body == null
						? HttpRequest.BodyPublishers.noBody()
						: HttpRequest.BodyPublishers.ofString(body));
		return http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
	}

	private void deleteIndexIfExists() throws Exception {
		HttpResponse<String> response = sendRaw("DELETE", "/" + indexName, null);
		if (response.statusCode() != 200 && response.statusCode() != 404) {
			throw new AssertionError("HTTP " + response.statusCode() + " while deleting index: " + response.body());
		}
	}

	private boolean containsMessage(Throwable throwable, String token) {
		Throwable current = throwable;
		while (current != null) {
			if (current.getMessage() != null && current.getMessage().contains(token)) {
				return true;
			}
			current = current.getCause();
		}
		return false;
	}

	private String encodePathSegment(String value) {
		return URLEncoder.encode(value, StandardCharsets.UTF_8);
	}
}
