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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HttpFtsSearchServiceTest {
	private static final String BULK_SUCCESS_RESPONSE = "{\"errors\":false,\"items\":[]}";
	private static final String DELETE_SUCCESS_RESPONSE =
			"{\"timed_out\":false,\"total\":1,\"deleted\":1,\"version_conflicts\":0,\"failures\":[]}";
	private static final String SHARDS = "\"_shards\":{\"total\":1,\"successful\":1,\"failed\":0}";

	private final ObjectMapper mapper = new ObjectMapper();
	private final SimpleValueFactory vf = SimpleValueFactory.getInstance();
	private HttpServer server;
	private AtomicReference<String> body;
	private List<String> bodies;
	private AtomicInteger requests;
	private volatile int responseCode = 200;
	private volatile String responseBody = BULK_SUCCESS_RESPONSE;
	private volatile String deleteResponse = DELETE_SUCCESS_RESPONSE;
	private volatile String refreshResponse = "{" + SHARDS + "}";
	private volatile String countResponse = "{\"count\":0," + SHARDS + "}";
	private AtomicReference<String> deleteBody;
	private AtomicReference<String> deleteQuery;
	private List<String> paths;
	private ExecutorService serverExecutor;
	private Path testDirectory;
	private volatile CountDownLatch requestEntered;
	private volatile CountDownLatch releaseRequest;
	private AtomicInteger activeRequests;
	private AtomicInteger maxActiveRequests;
	private volatile List<String> countReplies = List.of();
	private AtomicInteger countRequests = new AtomicInteger();
	private volatile CountDownLatch requestFinished;
	private volatile CountDownLatch twoRequestsEntered;
	private volatile String deleteSubmission = "{\"task\":\"node:1\"}";
	private volatile int deleteStatus = 200;
	private volatile String taskReply;
	private volatile int taskStatus = 200;
	private volatile int taskListingStatus = 200;
	private volatile List<Integer> taskStatuses = List.of();
	private volatile String taskRetryAfter;
	private List<Long> taskRequestTimes = Collections.synchronizedList(new ArrayList<>());
	private volatile String taskListing = "{\"tasks\":[]}";
	private AtomicInteger taskRequests = new AtomicInteger();
	private AtomicReference<String> opaqueId = new AtomicReference<>();
	private volatile boolean captureBodies = true;
	private AtomicLong receivedLines = new AtomicLong();
	private AtomicInteger maxBodyBytes = new AtomicInteger();

	@Before
	public void setUp() throws IOException {
		Path target = Path.of("target");
		Files.createDirectories(target);
		testDirectory = Files.createTempDirectory(target, "fts-http-test-");
		body = new AtomicReference<>();
		bodies = Collections.synchronizedList(new ArrayList<>());
		paths = Collections.synchronizedList(new ArrayList<>());
		deleteBody = new AtomicReference<>();
		deleteQuery = new AtomicReference<>();
		activeRequests = new AtomicInteger();
		maxActiveRequests = new AtomicInteger();
		requests = new AtomicInteger(0);
		server = HttpServer.create(new InetSocketAddress(0), 0);
		server.createContext("/_bulk", this::handleRequest);
		server.createContext("/_delete_by_query", this::handleRequest);
		server.createContext("/_refresh", this::handleRequest);
		server.createContext("/_count", this::handleRequest);
		server.createContext("/_tasks", this::handleTaskRequest);
		server.createContext("/test-index/", this::handleRequest);
		serverExecutor = Executors.newFixedThreadPool(4);
		server.setExecutor(serverExecutor);
		server.start();
	}

	@After
	public void tearDown() throws IOException {
		if (releaseRequest != null) {
			releaseRequest.countDown();
		}
		if (server != null) {
			server.stop(0);
		}
		serverExecutor.shutdownNow();
		try (var files = Files.walk(testDirectory)) {
			for (Path path : files.sorted(java.util.Comparator.reverseOrder()).toList()) {
				Files.deleteIfExists(path);
			}
		}
	}

	@Test
	public void commitSendsBatchedPayload() throws Exception {
		Path outboxDir = newOutbox();
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
		Path outboxDir = newOutbox();
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
		Path outboxDir = newOutbox();
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
		assertEquals(0, pendingCount(outboxDir));
	}

	@Test
	public void shutdownDiscardsUncommittedPayload() throws Exception {
		Path outboxDir = newOutbox();
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
				HttpFtsSearchService.PROP_FAIL_ON_ERROR, "true",
				HttpFtsSearchService.PROP_OUTBOX_DIR, newOutbox().toString()
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
		Path outboxDir = newOutbox();
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
		assertEquals(1, pendingCount(outboxDir));
	}

	@Test
	public void retriesPendingOutboxAfterFailedCommit() throws Exception {
		Path outboxDir = newOutbox();
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
		assertEquals(0, pendingCount(outboxDir));
	}

	@Test
	public void clearContextsUsesDeleteByQuery() throws Exception {
		Path outboxDir = newOutbox();
		HttpFtsSearchService service = new HttpFtsSearchService(endpoint(), "/_bulk", true, outboxDir.toString());

		service.begin();
		service.clearContexts(vf.createIRI("urn:ctx:A"), vf.createIRI("urn:ctx:B"));
		service.commit();
		service.shutdown();

		assertEquals(List.of("/_refresh", "/_delete_by_query", "/_count"), paths);
		assertEquals("conflicts=proceed&refresh=true&wait_for_completion=false", deleteQuery.get());
		JsonNode payload = mapper.readTree(deleteBody.get());
		assertTrue(payload.path("query").path("terms").has("context"));
		assertEquals(0, pendingCount(outboxDir));
	}

	@Test
	public void commitSplitsLargeBulkRequestIntoMultipleNdjsonPosts() throws Exception {
		Path outboxDir = newOutbox();
		HttpFtsSearchService service = new HttpFtsSearchService(endpoint(), "/_bulk", true, outboxDir.toString());

		service.begin();
		for (int i = 0; i < 20_000; i++) {
			service.addRemoveStatements(Set.of(vf.createStatement(
					vf.createIRI("urn:s:" + i),
					vf.createIRI("urn:label"),
					vf.createLiteral("value-" + i + "-abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"))), Set.of());
		}
		service.commit();
		service.shutdown();

		assertTrue(bodies.size() > 1);
		for (String requestBody : bodies) {
			if (requestBody.isBlank()) {
				continue;
			}
			assertTrue(requestBody.getBytes(StandardCharsets.UTF_8).length <= 5 * 1024 * 1024 + 1024);
			String[] lines = requestBody.strip().split("\\n");
			assertEquals(0, lines.length % 2);
		}
	}

	@Test
	public void clearRefreshesScopedIndexAndRetriesRemainingDocuments() throws Exception {
		Path outboxDir = newOutbox();
		HttpFtsSearchService service = new HttpFtsSearchService(endpoint(), "/test-index/_bulk", true,
				outboxDir.toString());
		deleteResponse = "{\"timed_out\":false,\"total\":2,\"deleted\":1,\"version_conflicts\":1,\"failures\":[]}";
		countReplies = List.of("{\"count\":1," + SHARDS + "}", "{\"count\":0," + SHARDS + "}");
		service.begin();
		service.clear();
		service.commit();
		service.shutdown();

		assertEquals(List.of("/test-index/_refresh", "/test-index/_delete_by_query", "/test-index/_count",
				"/test-index/_refresh", "/test-index/_delete_by_query", "/test-index/_count"), paths);
		assertTrue(mapper.readTree(deleteBody.get()).path("query").has("match_all"));
		assertEquals(0, pendingCount(outboxDir));
	}

	@Test
	public void incompleteDeleteResponsesKeepPayloadAndSkipLaterBulk() throws Exception {
		for (String invalid : List.of("", "{}", "{\"timed_out\":true,\"failures\":[]}",
				"{\"timed_out\":false,\"failures\":[{\"reason\":\"failed\"}]}",
				"{\"timed_out\":false,\"total\":1,\"deleted\":1,\"version_conflicts\":-1,\"failures\":[]}")) {
			Path outboxDir = newOutbox();
			HttpFtsSearchService service = new HttpFtsSearchService(endpoint(), "/_bulk", true, outboxDir.toString());
			deleteResponse = invalid;
			service.begin();
			service.clear();
			addStatement(service, "after-clear");
			try {
				service.commit();
				fail("Expected incomplete delete response to fail: " + invalid);
			} catch (IOException expected) {
				assertEquals(1, pendingCount(outboxDir));
			} finally {
				service.shutdown();
			}
		}
		assertFalse(paths.contains("/_bulk"));
		assertFalse(paths.contains("/_count"));
	}

	@Test
	public void deletionMustConvergeAndCountMustBeComplete() throws Exception {
		for (String invalidCount : List.of("{}", "{\"count\":0,\"_shards\":{\"total\":2,\"successful\":1,\"failed\":1}}",
				"{\"count\":1," + SHARDS + "}")) {
			Path outboxDir = newOutbox();
			HttpFtsSearchService service = new HttpFtsSearchService(endpoint(), "/_bulk", false, outboxDir.toString());
			countResponse = invalidCount;
			int before = requests.get();
			service.begin();
			service.clearContexts(vf.createIRI("urn:ctx:A"));
			service.commit();
			service.shutdown();
			assertEquals(1, pendingCount(outboxDir));
			assertEquals(invalidCount.contains("\"count\":1") ? 9 : 3, requests.get() - before);
		}
	}

	@Test
	public void refreshFailureDoesNotStartDelete() throws Exception {
		Path outboxDir = newOutbox();
		HttpFtsSearchService service = new HttpFtsSearchService(endpoint(), "/_bulk", false, outboxDir.toString());
		refreshResponse = "{\"_shards\":{\"total\":1,\"successful\":0,\"failed\":1}}";
		service.begin();
		service.clear();
		service.commit();
		service.shutdown();
		assertEquals(List.of("/_refresh"), paths);
		assertEquals(1, pendingCount(outboxDir));
	}

	@Test
	public void deleteNotFoundIsAnIdempotentReplaySuccess() throws Exception {
		Path outboxDir = newOutbox();
		HttpFtsSearchService service = new HttpFtsSearchService(endpoint(), "/_bulk", true, outboxDir.toString());
		responseBody = "{\"errors\":true,\"items\":[{\"delete\":{\"status\":404,\"result\":\"not_found\"}}]}";
		service.begin();
		service.addRemoveStatements(Set.of(), Set.of(vf.createStatement(vf.createIRI("urn:s"),
				vf.createIRI("urn:p"), vf.createLiteral("x"))));
		service.commit();
		service.shutdown();
		assertEquals(1, requests.get());
		assertEquals(0, pendingCount(outboxDir));
	}

	@Test
	public void failedHeadStopsReplayWithoutLosingLaterUpdates() throws Exception {
		Path outboxDir = newOutbox();
		HttpFtsSearchService service = new HttpFtsSearchService(endpoint(), "/_bulk", false, outboxDir.toString());
		responseCode = 500;
		service.begin();
		addStatement(service, "first");
		service.commit();
		service.begin();
		addStatement(service, "second");
		service.commit();
		assertEquals(6, requests.get());
		assertEquals(2, pendingCount(outboxDir));

		responseCode = 200;
		service.commit();
		service.shutdown();
		assertEquals(8, requests.get());
		assertTrue(bodies.get(6).contains("\"value\":\"first\""));
		assertTrue(bodies.get(7).contains("\"value\":\"second\""));
		assertEquals(0, pendingCount(outboxDir));
	}

	@Test
	public void bulkItemFailureHonorsFailOnErrorAndStopsQueue() throws Exception {
		Path outboxDir = newOutbox();
		HttpFtsSearchService service = new HttpFtsSearchService(endpoint(), "/_bulk", false, outboxDir.toString());
		responseBody = "{\"errors\":true,\"items\":[{\"index\":{\"status\":400,\"error\":{\"type\":\"bad_mapping\"}}}]}";
		service.begin();
		addStatement(service, "first");
		service.commit();
		assertEquals(1, requests.get());
		assertEquals(1, pendingCount(outboxDir));
		service.shutdown();
	}

	@Test
	public void serviceInstancesSharingDirectorySerializePublishAndDrain() throws Exception {
		Path outboxDir = newOutbox();
		HttpFtsSearchService first = new HttpFtsSearchService(endpoint(), "/_bulk", true, outboxDir.toString());
		HttpFtsSearchService second = new HttpFtsSearchService(endpoint(), "/_bulk", true,
				outboxDir.resolve(".").toString());
		requestEntered = new CountDownLatch(1);
		twoRequestsEntered = new CountDownLatch(2);
		releaseRequest = new CountDownLatch(1);
		ExecutorService commits = Executors.newFixedThreadPool(2);
		try {
			Future<?> firstCommit = commits.submit(() -> commitStatement(first, "first"));
			assertTrue(requestEntered.await(5, TimeUnit.SECONDS));
			CountDownLatch secondStarted = new CountDownLatch(1);
			Future<?> secondCommit = commits.submit(() -> {
				try {
					second.begin();
					addStatement(second, "second");
					secondStarted.countDown();
					second.commit();
				} catch (Exception error) {
					throw new RuntimeException(error);
				}
			});
			assertTrue(secondStarted.await(5, TimeUnit.SECONDS));
			assertFalse(twoRequestsEntered.await(250, TimeUnit.MILLISECONDS));
			assertFalse(secondCommit.isDone());
			releaseRequest.countDown();
			firstCommit.get(5, TimeUnit.SECONDS);
			secondCommit.get(5, TimeUnit.SECONDS);
			assertEquals(2, requests.get());
			assertEquals(1, maxActiveRequests.get());
			assertEquals(0, pendingCount(outboxDir));
		} finally {
			releaseRequest.countDown();
			commits.shutdownNow();
			first.shutdown();
			second.shutdown();
		}
	}

	@Test
	public void socketTimeoutBlocksReplayAcrossInstancesUntilExplicitReconciliation() throws Exception {
		Path outboxDir = newOutbox();
		HttpFtsSearchService first = new HttpFtsSearchService(Map.of(
				HttpFtsSearchService.PROP_ENDPOINT, endpoint(),
				HttpFtsSearchService.PROP_OUTBOX_DIR, outboxDir.toString(),
				HttpFtsSearchService.PROP_REQUEST_TIMEOUT, 100,
				HttpFtsSearchService.PROP_FAIL_ON_ERROR, false));
		requestEntered = new CountDownLatch(1);
		releaseRequest = new CountDownLatch(1);
		requestFinished = new CountDownLatch(1);
		first.begin();
		addStatement(first, "first");
		first.commit();
		first.shutdown();
		assertEquals(1, requests.get());
		assertEquals(1, pendingCount(outboxDir));

		HttpFtsSearchService second = new HttpFtsSearchService(endpoint(), "/_bulk", false, outboxDir.toString());
		second.begin();
		addStatement(second, "second");
		second.commit();
		assertEquals(1, requests.get());
		assertEquals(2, pendingCount(outboxDir));
		releaseRequest.countDown();
		assertTrue(requestFinished.await(5, TimeUnit.SECONDS));
		requestEntered = null;
		try (var files = Files.list(outboxDir)) {
			for (Path marker : files.filter(path -> path.toString().endsWith(".uncertain")).toList()) {
				Files.delete(marker);
			}
		}
		second.commit();
		second.shutdown();
		assertEquals(3, requests.get());
		assertEquals(0, pendingCount(outboxDir));
	}

	@Test
	public void gatewayTimeoutIsNotBlindlyRetried() throws Exception {
		Path outboxDir = newOutbox();
		HttpFtsSearchService service = new HttpFtsSearchService(endpoint(), "/_bulk", false, outboxDir.toString());
		responseCode = 504;
		service.begin();
		addStatement(service, "first");
		service.commit();
		service.commit();
		service.shutdown();
		assertEquals(1, requests.get());
		assertEquals(1, pendingCount(outboxDir));
	}

	@Test
	public void persistedDeleteTaskRecoversAcrossInstancesWithoutOverlappingReplay() throws Exception {
		Path outboxDir = newOutbox();
		taskReply = "{\"completed\":false}";
		HttpFtsSearchService first = new HttpFtsSearchService(endpoint(), "/_bulk", false,
				outboxDir.toString(), 100);
		first.begin();
		first.clear();
		addStatement(first, "first");
		first.commit();
		first.shutdown();
		assertEquals(2, requests.get());
		Path marker = marker(outboxDir);
		assertEquals("node:1", mapper.readTree(Files.readString(marker)).path("taskId").asText());

		HttpFtsSearchService second = new HttpFtsSearchService(endpoint(), "/_bulk", false,
				outboxDir.toString(), 100);
		second.begin();
		addStatement(second, "second");
		second.commit();
		assertEquals(2, requests.get());
		assertEquals(2, pendingCount(outboxDir));
		taskReply = null;
		second.commit();
		second.shutdown();
		assertEquals(0, pendingCount(outboxDir));
		assertFalse(Files.exists(marker));
		assertEquals(List.of("/_refresh", "/_delete_by_query", "/_refresh", "/_delete_by_query",
				"/_count", "/_bulk", "/_bulk"), paths);
		assertTrue(bodies.get(bodies.size() - 2).contains("first"));
		assertTrue(bodies.get(bodies.size() - 1).contains("second"));
	}

	@Test
	public void lostDeleteAcknowledgementDiscoversTaskUsingPersistedOpaqueId() throws Exception {
		Path outboxDir = newOutbox();
		deleteStatus = 504;
		taskReply = "{\"completed\":false}";
		HttpFtsSearchService service = new HttpFtsSearchService(endpoint(), "/_bulk", false,
				outboxDir.toString(), 100);
		service.begin();
		service.clear();
		service.commit();
		Path marker = marker(outboxDir);
		JsonNode fence = mapper.readTree(Files.readString(marker));
		assertEquals(opaqueId.get(), fence.path("requestId").asText());
		assertFalse(fence.has("taskId"));
		taskListing = mapper.writeValueAsString(Map.of("tasks", List.of(Map.of(
				"node", "node", "id", 1, "headers", Map.of("X-Opaque-Id", opaqueId.get())))));
		service.commit();
		assertEquals("node:1", mapper.readTree(Files.readString(marker)).path("taskId").asText());
		assertEquals(2, requests.get());
		deleteStatus = 200;
		taskReply = null;
		service.commit();
		service.shutdown();
		assertEquals(0, pendingCount(outboxDir));
		assertFalse(Files.exists(marker));
	}

	@Test
	public void observedBulkTaskCompletionAllowsOrderedRecoveryButMissingNodeDoesNot() throws Exception {
		Path outboxDir = newOutbox();
		HttpFtsSearchService service = new HttpFtsSearchService(endpoint(), "/_bulk", false,
				outboxDir.toString());
		service.begin();
		addStatement(service, "first");
		responseCode = 504;
		service.commit();
		Path marker = marker(outboxDir);
		JsonNode fence = mapper.readTree(Files.readString(marker));
		taskListing = mapper.writeValueAsString(Map.of("tasks", List.of(Map.of(
				"node", "node", "id", 1, "headers", Map.of("X-Opaque-Id", fence.path("requestId").asText())))));
		taskStatus = 404;
		taskReply = "{\"error\":{\"reason\":\"node [node] isn't part of the cluster\"}}";
		service.begin();
		addStatement(service, "second");
		service.commit();
		assertEquals(1, requests.get());
		assertTrue(Files.exists(marker));
		assertEquals("node:1", mapper.readTree(Files.readString(marker)).path("taskId").asText());
		taskReply = "{\"error\":{\"reason\":\"task [node:1] isn't running and hasn't stored its results\"}}";
		responseCode = 200;
		service.commit();
		service.shutdown();
		assertEquals(3, requests.get());
		assertEquals(0, pendingCount(outboxDir));
		assertTrue(bodies.get(1).contains("first"));
		assertTrue(bodies.get(2).contains("second"));
	}

	@Test
	public void legacyMarkerRequiresExplicitReconciliationWithoutNetworkReplay() throws Exception {
		Path outboxDir = newOutbox();
		Path payload = outboxDir.resolve("1-legacy.jsonl");
		Files.writeString(payload, "{\"op\":\"clear\"}\n");
		Path marker = payload.resolveSibling(payload.getFileName() + ".uncertain");
		Files.writeString(marker, "FTS request in progress; confirm Elasticsearch has finished before replay.");
		HttpFtsSearchService service = new HttpFtsSearchService(endpoint(), "/_bulk", true, outboxDir.toString());
		try {
			service.commit();
			fail("Expected legacy marker to block replay");
		} catch (IOException expected) {
			assertTrue(expected.getMessage().contains("No completion proof"));
		} finally {
			service.shutdown();
		}
		assertEquals(0, requests.get());
		assertEquals(0, taskRequests.get());
		assertTrue(Files.exists(payload));
		assertTrue(Files.exists(marker));
	}

	@Test
	public void taskPollingFailureRetainsTaskIdAndDoesNotSendBulk() throws Exception {
		Path outboxDir = newOutbox();
		taskStatus = 503;
		HttpFtsSearchService service = new HttpFtsSearchService(endpoint(), "/_bulk", false, outboxDir.toString(), 300);
		service.begin();
		service.clear();
		addStatement(service, "after-clear");
		service.commit();
		assertEquals("node:1", mapper.readTree(Files.readString(marker(outboxDir))).path("taskId").asText());
		assertFalse(paths.contains("/_bulk"));
		taskStatus = 200;
		service.commit();
		service.shutdown();
		assertEquals(0, pendingCount(outboxDir));
	}

	@Test
	public void taskPollingRecoversFromBackpressureWithoutResubmittingDeletion() throws Exception {
		Path outboxDir = newOutbox();
		taskStatuses = List.of(429, 503, 502, 408, 504, 200);
		HttpFtsSearchService service = new HttpFtsSearchService(endpoint(), "/_bulk", true,
				outboxDir.toString(), 8_000);
		try {
			service.begin();
			service.clear();
			addStatement(service, "after-clear");
			service.commit();
			assertEquals(6, taskRequests.get());
			assertEquals(1, Collections.frequency(paths, "/_delete_by_query"));
			assertEquals(0, pendingCount(outboxDir));
			try (var files = Files.list(outboxDir)) {
				assertFalse(files.anyMatch(path -> path.toString().endsWith(".uncertain")));
			}
			assertTrue(TimeUnit.NANOSECONDS.toMillis(taskRequestTimes.get(1) - taskRequestTimes.get(0)) >= 90);
		} finally {
			service.shutdown();
		}
	}

	@Test
	public void repeatedTaskBackpressureExhaustsBudgetAndRetainsOriginalTask() throws Exception {
		Path outboxDir = newOutbox();
		taskStatus = 429;
		taskReply = "{\"error\":{\"type\":\"circuit_breaking_exception\",\"reason\":\"task result too large\"}}";
		HttpFtsSearchService service = new HttpFtsSearchService(endpoint(), "/_bulk", true,
				outboxDir.toString(), 350);
		try {
			service.begin();
			service.clear();
			addStatement(service, "after-clear");
			for (int commit = 0; commit < 2; commit++) {
				long start = System.nanoTime();
				try {
					service.commit();
					fail("Expected bounded polling failure");
				} catch (IOException expected) {
					assertTrue(expected.getMessage().contains("polling budget exhausted"));
					assertTrue(expected.getMessage().contains("HTTP 429"));
					assertTrue(expected.getMessage().contains("circuit_breaking_exception"));
				}
				assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 2_000);
				assertEquals("node:1", mapper.readTree(Files.readString(marker(outboxDir))).path("taskId").asText());
			}
			assertTrue(taskRequests.get() >= 4);
			assertTrue(taskRequests.get() <= 6);
			assertEquals(1, Collections.frequency(paths, "/_delete_by_query"));
			assertFalse(paths.contains("/_bulk"));
			assertEquals(1, pendingCount(outboxDir));
		} finally {
			service.shutdown();
		}
	}

	@Test
	public void taskPollingHonorsRetryAfterWithoutExceedingBudget() throws Exception {
		Path outboxDir = newOutbox();
		taskStatuses = List.of(429, 200);
		taskRetryAfter = "1";
		HttpFtsSearchService service = new HttpFtsSearchService(endpoint(), "/_bulk", true,
				outboxDir.toString(), 2_500);
		try {
			service.begin();
			service.clear();
			service.commit();
			assertTrue(TimeUnit.NANOSECONDS.toMillis(taskRequestTimes.get(1) - taskRequestTimes.get(0)) >= 1_000);
			assertEquals(1, Collections.frequency(paths, "/_delete_by_query"));
			assertEquals(0, pendingCount(outboxDir));
		} finally {
			service.shutdown();
		}
	}

	@Test
	public void taskListingBackpressureRetainsFenceAndRecoversWithoutSubmission() throws Exception {
		Path outboxDir = newOutbox();
		deleteStatus = 504;
		taskListingStatus = 429;
		taskRetryAfter = "60";
		HttpFtsSearchService service = new HttpFtsSearchService(endpoint(), "/_bulk", true,
				outboxDir.toString(), 300);
		try {
			service.begin();
			service.clear();
			try {
				service.commit();
				fail("Expected listing backpressure to exhaust budget");
			} catch (IOException expected) {
				assertTrue(expected.getMessage().contains("HTTP 429"));
			}
			Path marker = marker(outboxDir);
			assertFalse(mapper.readTree(Files.readString(marker)).has("taskId"));
			assertEquals(1, taskRequests.get());
			taskRetryAfter = null;
			taskListingStatus = 200;
			taskStatuses = List.of(200, 429, 200);
			taskListing = mapper.writeValueAsString(Map.of("tasks", List.of(Map.of(
					"node", "node", "id", 1, "headers", Map.of("X-Opaque-Id", opaqueId.get())))));
			taskReply = "{\"completed\":false}";
			try {
				service.commit();
				fail("Expected discovered task to remain fenced while running");
			} catch (IOException expected) {
				assertTrue(expected.getMessage().contains("still running"));
			}
			assertEquals("node:1", mapper.readTree(Files.readString(marker)).path("taskId").asText());
			assertEquals(1, Collections.frequency(paths, "/_delete_by_query"));
		} finally {
			service.shutdown();
		}
	}

	@Test
	public void malformedDeleteAcknowledgementAndPartialTaskListingRemainFenced() throws Exception {
		Path outboxDir = newOutbox();
		deleteSubmission = "{}";
		HttpFtsSearchService service = new HttpFtsSearchService(endpoint(), "/_bulk", false, outboxDir.toString());
		service.begin();
		service.clear();
		service.commit();
		Path marker = marker(outboxDir);
		String id = mapper.readTree(Files.readString(marker)).path("requestId").asText();
		taskListing = mapper.writeValueAsString(Map.of(
				"tasks", List.of(Map.of("node", "node", "id", 1, "headers", Map.of("X-Opaque-Id", id))),
				"node_failures", List.of(Map.of("reason", "unavailable node"))));
		service.commit();
		service.shutdown();
		assertTrue(Files.exists(marker));
		assertEquals(2, requests.get());
		assertFalse(mapper.readTree(Files.readString(marker)).has("taskId"));
	}

	@Test
	public void replayStreamsLargeSingleRecordAndPreservesGlobalRemoveOrdering() throws Exception {
		Path outboxDir = newOutbox();
		Path payload = outboxDir.resolve("1-large.jsonl");
		int documents = Integer.getInteger("fts.test.replayDocuments", 25_000);
		captureBodies = false;
		try (var json = mapper.getFactory().createGenerator(payload.toFile(),
				com.fasterxml.jackson.core.JsonEncoding.UTF8)) {
			json.writeStartObject();
			json.writeStringField("op", "remove");
			json.writeObjectFieldStart("documents");
			json.writeObjectFieldStart("id-0");
			json.writeStringField("id", "id-0");
			json.writeEndObject();
			json.writeEndObject();
			json.writeEndObject();
			json.writeStartObject();
			// Legacy JSON property ordering is supported without materializing the record.
			json.writeObjectFieldStart("documents");
			for (int i = 0; i < documents; i++) {
				json.writeObjectFieldStart("id-" + i);
				json.writeStringField("id", "id-" + i);
				json.writeStringField("value", "x".repeat(256));
				json.writeEndObject();
			}
			json.writeEndObject();
			json.writeStringField("op", "upsert");
			json.writeEndObject();
		}
		HttpFtsSearchService service = new HttpFtsSearchService(endpoint(), "/_bulk", true, outboxDir.toString());
		service.commit();
		service.shutdown();
		assertTrue(requests.get() > 1);
		assertTrue(maxBodyBytes.get() <= 5 * 1024 * 1024);
		assertEquals(documents * 2L + 1, receivedLines.get());
		String last = body.get();
		assertTrue(last.endsWith("{\"delete\":{\"_id\":\"id-0\"}}\n"));
		assertEquals(0, pendingCount(outboxDir));
	}

	@Test
	public void malformedPayloadFailsBeforeCreatingFenceOrStartingDeletion() throws Exception {
		Path outboxDir = newOutbox();
		Files.writeString(outboxDir.resolve("1-invalid.jsonl"),
				"{\"op\":\"clear\"}\n{\"op\":\"upsert\",\"documents\":{\"invalid\":123}}\n");
		HttpFtsSearchService service = new HttpFtsSearchService(endpoint(), "/_bulk", false, outboxDir.toString());
		service.commit();
		service.shutdown();
		assertEquals(0, requests.get());
		assertEquals(1, pendingCount(outboxDir));
		try (var files = Files.list(outboxDir)) {
			assertFalse(files.anyMatch(path -> path.toString().endsWith(".uncertain")));
		}
	}

	@Test
	public void clearDoesNotReplayDocumentsFromBeforeLastClear() throws Exception {
		Path outboxDir = newOutbox();
		HttpFtsSearchService service = new HttpFtsSearchService(endpoint(), "/_bulk", true, outboxDir.toString());
		service.begin();
		addStatement(service, "before");
		service.clear();
		addStatement(service, "after");
		service.commit();
		service.shutdown();
		assertFalse(body.get().contains("before"));
		assertTrue(body.get().contains("after"));
		assertEquals(0, pendingCount(outboxDir));
	}

	private Path marker(Path outboxDir) throws IOException {
		try (var files = Files.list(outboxDir)) {
			return files.filter(path -> path.toString().endsWith(".uncertain")).findFirst().orElseThrow();
		}
	}

	private void addStatement(HttpFtsSearchService service, String value) {
		service.addRemoveStatements(Set.of(vf.createStatement(vf.createIRI("urn:s"),
				vf.createIRI("urn:p"), vf.createLiteral(value))), Set.of());
	}

	private void commitStatement(HttpFtsSearchService service, String value) {
		try {
			service.begin();
			addStatement(service, value);
			service.commit();
		} catch (Exception error) {
			throw new RuntimeException(error);
		}
	}

	private String endpoint() {
		return "http://127.0.0.1:" + server.getAddress().getPort();
	}

	private Path newOutbox() throws IOException {
		return Files.createTempDirectory(testDirectory, "outbox-");
	}

	private long pendingCount(Path directory) throws IOException {
		try (var files = Files.list(directory)) {
			return files.filter(path -> path.toString().endsWith(".jsonl")).count();
		}
	}

	private void handleRequest(HttpExchange exchange) throws IOException {
		requests.incrementAndGet();
		if (twoRequestsEntered != null) {
			twoRequestsEntered.countDown();
		}
		int active = activeRequests.incrementAndGet();
		maxActiveRequests.accumulateAndGet(active, Math::max);
		String path = exchange.getRequestURI().getPath();
		opaqueId.set(exchange.getRequestHeaders().getFirst("X-Opaque-Id"));
		paths.add(path);
		try (InputStream in = exchange.getRequestBody()) {
			byte[] bytes = in.readAllBytes();
			body.set(new String(bytes, StandardCharsets.UTF_8));
			if (captureBodies) {
				bodies.add(body.get());
			} else {
				maxBodyBytes.accumulateAndGet(bytes.length, Math::max);
				receivedLines.addAndGet(body.get().lines().count());
			}
		}

		String reply = responseBody;
		int status = responseCode;
		if (path.endsWith("/_delete_by_query")) {
			deleteBody.set(body.get());
			deleteQuery.set(exchange.getRequestURI().getQuery());
			reply = deleteSubmission;
			status = deleteStatus;
		} else if (path.endsWith("/_refresh")) {
			reply = refreshResponse;
		} else if (path.endsWith("/_count")) {
			int index = countRequests.getAndIncrement();
			reply = index < countReplies.size() ? countReplies.get(index) : countResponse;
		}
		try {
			if (requestEntered != null) {
				requestEntered.countDown();
				if (!releaseRequest.await(5, TimeUnit.SECONDS)) {
					throw new IOException("Timed out waiting to release test request");
				}
			}
			byte[] response = reply.getBytes(StandardCharsets.UTF_8);
			exchange.sendResponseHeaders(status, response.length);
			exchange.getResponseBody().write(response);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} finally {
			activeRequests.decrementAndGet();
			exchange.close();
			if (requestFinished != null) {
				requestFinished.countDown();
			}
		}
	}

	private void handleTaskRequest(HttpExchange exchange) throws IOException {
		int index = taskRequests.getAndIncrement();
		taskRequestTimes.add(System.nanoTime());
		boolean listing = exchange.getRequestURI().getPath().equals("/_tasks");
		int status = index < taskStatuses.size() ? taskStatuses.get(index) : listing ? taskListingStatus : taskStatus;
		String reply = listing ? taskListing : taskReply == null
				? "{\"completed\":true,\"response\":" + deleteResponse + "}" : taskReply;
		if (status == 429 && taskRetryAfter != null) {
			exchange.getResponseHeaders().set("Retry-After", taskRetryAfter);
		}
		byte[] bytes = reply.getBytes(StandardCharsets.UTF_8);
		exchange.sendResponseHeaders(status, bytes.length);
		exchange.getResponseBody().write(bytes);
		exchange.close();
	}
}
