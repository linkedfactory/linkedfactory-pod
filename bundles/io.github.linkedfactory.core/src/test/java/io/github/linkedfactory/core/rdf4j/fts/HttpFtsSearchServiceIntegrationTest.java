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
	public void syncsMultipleValuesUpdatesAndDeletes() throws Exception {
		initRepository(defaultIndexDefinition());

		try (SailRepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			connection.add(vf.createIRI("urn:sensor1"), vf.createIRI("urn:label"), vf.createLiteral("alpha"));
			connection.add(vf.createIRI("urn:sensor1"), vf.createIRI("urn:label"), vf.createLiteral("beta"));
			connection.add(vf.createIRI("urn:sensor1"), vf.createIRI("urn:related"), vf.createIRI("urn:lineA"));
			connection.commit();
		}

		refreshIndex();
		JsonNode source = documentSource("urn:sensor1");
		assertNotNull(source);
		assertEquals("urn:sensor1", source.path("subject").asText());
		assertEquals(Set.of("alpha", "beta"), fieldValues(source, "urn:label"));
		assertEquals(Set.of("urn:lineA"), fieldValues(source, "urn:related"));

		try (SailRepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			connection.remove(vf.createIRI("urn:sensor1"), vf.createIRI("urn:label"), vf.createLiteral("alpha"));
			connection.remove(vf.createIRI("urn:sensor1"), vf.createIRI("urn:related"), vf.createIRI("urn:lineA"));
			connection.add(vf.createIRI("urn:sensor1"), vf.createIRI("urn:label"), vf.createLiteral("gamma"));
			connection.add(vf.createIRI("urn:sensor1"), vf.createIRI("urn:related"), vf.createIRI("urn:lineB"));
			connection.commit();
		}

		refreshIndex();
		source = documentSource("urn:sensor1");
		assertNotNull(source);
		assertEquals(Set.of("beta", "gamma"), fieldValues(source, "urn:label"));
		assertEquals(Set.of("urn:lineB"), fieldValues(source, "urn:related"));

		try (SailRepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			connection.remove(vf.createIRI("urn:sensor1"), vf.createIRI("urn:label"), vf.createLiteral("beta"));
			connection.remove(vf.createIRI("urn:sensor1"), vf.createIRI("urn:label"), vf.createLiteral("gamma"));
			connection.remove(vf.createIRI("urn:sensor1"), vf.createIRI("urn:related"), vf.createIRI("urn:lineB"));
			connection.commit();
		}

		refreshIndex();
		assertNull(documentSource("urn:sensor1"));
	}

	@Test
	public void clearContextsRemovesMatchingValuesAndDeletesEmptiedDocuments() throws Exception {
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
		assertEquals(2, documentCount());

		try (SailRepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			connection.clear(ctxA);
			connection.commit();
		}

		refreshIndex();
		JsonNode source = documentSource("urn:sensor1");
		assertNotNull(source);
		assertEquals(Set.of("beta"), fieldValues(source, "urn:label"));
		assertEquals(Set.of("urn:ctx:B"), fieldContexts(source, "urn:label"));
		assertNull(documentSource("urn:sensor2"));
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
				assertTrue(containsMessage(expected, "bulk update"));
			}
		}

		try (var files = Files.list(outboxDir)) {
			assertTrue(files.findAny().isPresent());
		}

		deleteIndexIfExists();
		createIndex(defaultIndexDefinition());
		searchService.commit();

		refreshIndex();
		assertNotNull(documentSource("urn:ok"));
		assertNotNull(documentSource("urn:bad"));
		assertEquals(Set.of("42"), fieldValues(documentSource("urn:ok"), "urn:number"));
		assertEquals(Set.of("not-a-number"), fieldValues(documentSource("urn:bad"), "urn:number"));
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
				    "properties": {
				      "subject": {
				        "type": "keyword"
				      }
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
				    "properties": {
				      "subject": {
				        "type": "keyword"
				      },
				      "urn:number": {
				        "properties": {
				          "kind": { "type": "keyword" },
				          "value": { "type": "long" },
				          "datatype": { "type": "keyword" },
				          "language": { "type": "keyword" },
				          "context": { "type": "keyword" }
				        }
				      }
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

	private JsonNode documentSource(String subject) throws Exception {
		HttpResponse<String> response = sendRaw("GET", "/" + indexName + "/_doc/" + encodePathSegment(subject), null);
		if (response.statusCode() == 404) {
			return null;
		}
		if (response.statusCode() >= 300) {
			throw new AssertionError("HTTP " + response.statusCode() + " while reading document: " + response.body());
		}
		JsonNode root = mapper.readTree(response.body());
		return root.path("found").asBoolean(false) ? root.path("_source") : null;
	}

	private Set<String> fieldValues(JsonNode source, String field) {
		Set<String> values = new LinkedHashSet<>();
		for (JsonNode value : source.path(field)) {
			values.add(value.path("value").asText());
		}
		return values;
	}

	private Set<String> fieldContexts(JsonNode source, String field) {
		Set<String> values = new LinkedHashSet<>();
		for (JsonNode value : source.path(field)) {
			values.add(value.path("context").asText());
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

	private String encodePathSegment(String value) {
		return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
	}

	private boolean containsMessage(Throwable error, String fragment) {
		Throwable current = error;
		while (current != null) {
			if (current.getMessage() != null && current.getMessage().contains(fragment)) {
				return true;
			}
			current = current.getCause();
		}
		return false;
	}
}
