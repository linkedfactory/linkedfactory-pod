package io.github.linkedfactory.core.rdf4j.fts;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.net.SocketTimeoutException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class HttpFtsSearchService implements FtsSearchService {
	private static final Logger logger = LoggerFactory.getLogger(HttpFtsSearchService.class);

	private static final int MAX_ATTEMPTS = 3;
	private static final long RETRY_BACKOFF_MILLIS = 100L;
	private static final long MAX_RETRY_BACKOFF_MILLIS = 5_000L;
	private static final int MAX_BULK_BYTES = 5 * 1024 * 1024;
	public static final int DEFAULT_REQUEST_TIMEOUT_MILLIS = 300_000;
	private static final ReentrantLock[] OUTBOX_LOCKS = new ReentrantLock[64];
	static {
		for (int i = 0; i < OUTBOX_LOCKS.length; i++) {
			OUTBOX_LOCKS[i] = new ReentrantLock();
		}
	}
	private static final ContentType NDJSON = ContentType.create("application/x-ndjson", StandardCharsets.UTF_8);
	private static final String OP_STATEMENTS = "statements";
	private static final String OP_UPSERT = "upsert";
	private static final String OP_REMOVE = "remove";
	private static final String OP_CLEAR = "clear";
	private static final String OP_CLEAR_CONTEXTS = "clearContexts";
	private static final String FIELD_ID = "id";
	private static final String FIELD_SUBJECT = "subject";
	private static final String FIELD_PREDICATE = "predicate";
	private static final String FIELD_VALUE = "value";
	private static final String FIELD_CONTEXT = "context";
	private static final String FIELD_DATATYPE = "datatype";
	private static final String FIELD_LANGUAGE = "language";
	private static final String FIELD_SORT_VALUE = "sortValue";

	static final String PROP_ENDPOINT = "fts.endpoint";
	static final String PROP_BULK_PATH = "fts.bulkPath";
	static final String PROP_FAIL_ON_ERROR = "fts.failOnError";
	static final String PROP_OUTBOX_DIR = "fts.outboxDir";
	static final String PROP_REQUEST_TIMEOUT = "fts.requestTimeoutMillis";
	private static final String DEFAULT_OUTBOX_DIR = Path.of(System.getProperty("java.io.tmpdir"),
			"linkedfactory-fts-outbox").toString();

	private final ObjectMapper mapper = new ObjectMapper();
	private volatile String endpoint = "";
	private volatile String bulkPath = "/_bulk";
	private volatile boolean failOnError = true;
	private volatile Path outboxDir = Path.of(DEFAULT_OUTBOX_DIR);
	private volatile RequestConfig requestConfig = requestConfig(DEFAULT_REQUEST_TIMEOUT_MILLIS);

	private volatile CloseableHttpClient httpClient;
	private final ThreadLocal<TransactionState> txState = new ThreadLocal<>();

	public HttpFtsSearchService() {
	}

	public HttpFtsSearchService(Map<String, Object> properties) {
		configure(properties);
	}

	public HttpFtsSearchService(String endpoint, String bulkPath, boolean failOnError) {
		this(endpoint, bulkPath, failOnError, DEFAULT_OUTBOX_DIR);
	}

	public HttpFtsSearchService(String endpoint, String bulkPath, boolean failOnError, String outboxDir) {
		this(endpoint, bulkPath, failOnError, outboxDir, DEFAULT_REQUEST_TIMEOUT_MILLIS);
	}

	public HttpFtsSearchService(String endpoint, String bulkPath, boolean failOnError, String outboxDir,
			int requestTimeoutMillis) {
		this.endpoint = normalizeEndpoint(endpoint);
		this.bulkPath = normalizePath(bulkPath);
		this.failOnError = failOnError;
		this.outboxDir = normalizeOutboxDir(outboxDir);
		this.requestConfig = requestConfig(requestTimeoutMillis);
	}

	public final void configure(Map<String, Object> properties) {
		this.endpoint = normalizeEndpoint(stringProp(properties, PROP_ENDPOINT, ""));
		this.bulkPath = normalizePath(stringProp(properties, PROP_BULK_PATH, "/_bulk"));
		this.failOnError = booleanProp(properties, PROP_FAIL_ON_ERROR, true);
		this.outboxDir = normalizeOutboxDir(stringProp(properties, PROP_OUTBOX_DIR, DEFAULT_OUTBOX_DIR));
		this.requestConfig = requestConfig(Integer.parseInt(stringProp(properties, PROP_REQUEST_TIMEOUT,
				Integer.toString(DEFAULT_REQUEST_TIMEOUT_MILLIS))));
	}

	private static RequestConfig requestConfig(int timeoutMillis) {
		if (timeoutMillis <= 0) {
			throw new IllegalArgumentException("fts.requestTimeoutMillis must be positive");
		}
		return RequestConfig.custom().setConnectTimeout(5_000).setConnectionRequestTimeout(5_000)
				.setSocketTimeout(timeoutMillis).build();
	}

	public void shutdown() throws IOException {
		cleanup(txState.get());
		txState.remove();
		CloseableHttpClient client = httpClient;
		httpClient = null;
		if (client != null) {
			client.close();
		}
	}

	@Override
	public void begin() {
		TransactionState tx = txState.get();
		if (tx != null) {
			tx.cleanup();
		}
		txState.set(TransactionState.create(outboxDir, mapper));
	}

	@Override
	public void addRemoveStatements(Set<Statement> added, Set<Statement> removed) {
		ObjectNode batch = statementBatch(added, removed);
		if (batch.has("addedDocuments") || batch.has("removedDocuments")) {
			state().append(batch);
		}
	}

	@Override
	public void clearContexts(Resource... contexts) {
		ObjectNode op = mapper.createObjectNode();
		op.put("op", OP_CLEAR_CONTEXTS);
		ArrayNode ctx = mapper.createArrayNode();
		if (contexts != null) {
			for (Resource context : contexts) {
				if (context != null) {
					ctx.add(context.stringValue());
				}
			}
		}
		op.set("contexts", ctx);
		state().append(op);
	}

	@Override
	public void clear() {
		ObjectNode op = mapper.createObjectNode();
		op.put("op", OP_CLEAR);
		state().append(op);
	}

	@Override
	public void commit() throws Exception {
		TransactionState tx = txState.get();
		if (endpoint.isEmpty()) {
			logger.debug("Skipping FTS update: no {} configured.", PROP_ENDPOINT);
			cleanup(tx);
			txState.remove();
			return;
		}

		try {
			drainOutbox(tx);
		} finally {
			cleanup(tx);
			txState.remove();
		}
	}

	@Override
	public void rollback() {
		TransactionState tx = txState.get();
		cleanup(tx);
		txState.remove();
	}

	private String deleteByQueryPath() {
		return indexPath("_delete_by_query");
	}

	private String indexPath(String operation) {
		if (bulkPath.endsWith("/_bulk")) {
			return bulkPath.substring(0, bulkPath.length() - "_bulk".length()) + operation;
		}
		return "/" + operation;
	}

	private ObjectNode statementBatch(Collection<Statement> added, Collection<Statement> removed) {
		ObjectNode op = mapper.createObjectNode();
		op.put("op", OP_STATEMENTS);
		ObjectNode addedDocuments = documentsByStatement(added);
		if (!addedDocuments.isEmpty()) {
			op.set("addedDocuments", addedDocuments);
		}
		ObjectNode removedDocuments = documentsByStatement(removed);
		if (!removedDocuments.isEmpty()) {
			op.set("removedDocuments", removedDocuments);
		}
		return op;
	}

	private ObjectNode documentsByStatement(Collection<Statement> statements) {
		ObjectNode documents = mapper.createObjectNode();
		for (Statement statement : statements) {
			if (!(statement.getObject() instanceof Literal)) {
				continue;
			}
			ObjectNode document = statementDocument(statement);
			documents.set(document.path(FIELD_ID).asText(), document);
		}
		return documents;
	}

	private ObjectNode statementDocument(Statement statement) {
		ObjectNode document = mapper.createObjectNode();
		Literal literal = (Literal) statement.getObject();
		String subject = statement.getSubject().stringValue();
		String predicate = statement.getPredicate().stringValue();
		document.put(FIELD_ID, statementId(statement));
		document.put(FIELD_SUBJECT, subject);
		document.put(FIELD_PREDICATE, predicate);
		document.put(FIELD_VALUE, literal.getLabel());
		document.put(FIELD_SORT_VALUE, normalizeSortValue(literal.getLabel()));
		if (statement.getContext() != null) {
			document.put(FIELD_CONTEXT, statement.getContext().stringValue());
		}
		if (literal.getDatatype() != null) {
			document.put(FIELD_DATATYPE, literal.getDatatype().stringValue());
		}
		literal.getLanguage().ifPresent(language -> document.put(FIELD_LANGUAGE, language));
		return document;
	}

	private String statementId(Statement statement) {
		Literal literal = (Literal) statement.getObject();
		return hash(statement.getSubject().stringValue(),
				statement.getPredicate().stringValue(),
				literal.getLabel(),
				literal.getDatatype() == null ? "" : literal.getDatatype().stringValue(),
				literal.getLanguage().orElse(""),
				statement.getContext() == null ? "" : statement.getContext().stringValue());
	}

	private String hash(String... parts) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			for (String part : parts) {
				digest.update(part.getBytes(StandardCharsets.UTF_8));
				digest.update((byte) 0);
			}
			byte[] bytes = digest.digest();
			StringBuilder value = new StringBuilder(bytes.length * 2);
			for (byte current : bytes) {
				value.append(Character.forDigit((current >> 4) & 0xF, 16));
				value.append(Character.forDigit(current & 0xF, 16));
			}
			return value.toString();
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException("SHA-256 not available", e);
		}
	}

	private String normalizeSortValue(String value) {
		return value == null ? "" : value.toLowerCase(java.util.Locale.ROOT);
	}

	private static String stringProp(Map<String, Object> properties, String key, String defaultValue) {
		Object value = properties == null ? null : properties.get(key);
		return value == null ? defaultValue : value.toString().trim();
	}

	private static boolean booleanProp(Map<String, Object> properties, String key, boolean defaultValue) {
		Object value = properties == null ? null : properties.get(key);
		if (value == null) {
			return defaultValue;
		}
		if (value instanceof Boolean) {
			return (Boolean) value;
		}
		return Boolean.parseBoolean(value.toString());
	}

	private static String normalizeEndpoint(String endpoint) {
		if (endpoint == null || endpoint.isBlank()) {
			return "";
		}
		String trimmed = endpoint.trim();
		return trimmed.endsWith("/") ? trimmed.substring(0, trimmed.length() - 1) : trimmed;
	}

	private static String normalizePath(String path) {
		String value = Objects.requireNonNullElse(path, "/_bulk").trim();
		if (value.isEmpty()) {
			value = "/_bulk";
		}
		return value.startsWith("/") ? value : "/" + value;
	}

	private void cleanup(TransactionState tx) {
		if (tx == null) {
			return;
		}
		try {
			tx.cleanup();
		} catch (RuntimeException e) {
			logger.warn("Unable to clean up FTS bulk payload", e);
		}
	}

	private TransactionState state() {
		TransactionState tx = txState.get();
		if (tx == null) {
			throw new IllegalStateException("FTS transaction has not been started. Call begin() before updating the index.");
		}
		return tx;
	}

	private CloseableHttpClient client() {
		CloseableHttpClient existing = httpClient;
		if (existing != null) {
			return existing;
		}
		synchronized (this) {
			if (httpClient == null) {
				httpClient = HttpClients.custom()
						.setDefaultRequestConfig(requestConfig)
						.disableAutomaticRetries()
						.build();
			}
			return httpClient;
		}
	}

	private void drainOutbox(TransactionState tx) throws Exception {
		ensureOutboxDir();
		Path directory = outboxDir.toRealPath();
		ReentrantLock lock = OUTBOX_LOCKS[Math.floorMod(directory.hashCode(), OUTBOX_LOCKS.length)];
		lock.lockInterruptibly();
		try (FileChannel channel = FileChannel.open(directory.resolve(".drain.lock"),
				StandardOpenOption.CREATE, StandardOpenOption.WRITE);
				var fileLock = channel.lock()) {
			if (tx != null && !tx.isEmpty()) {
				tx.persist(directory);
			}
			drainLocked(directory);
		} finally {
			lock.unlock();
		}
	}

	private void drainLocked(Path directory) throws Exception {
		List<Path> pending;
		try (var files = Files.list(directory)) {
			pending = files.filter(path -> path.getFileName().toString().endsWith(".jsonl"))
					.sorted()
					.toList();
		}
		for (Path payload : pending) {
			Path uncertain = payload.resolveSibling(payload.getFileName() + ".uncertain");
			try {
				if (Files.exists(uncertain)) {
					reconcile(uncertain);
				}
				sendPayload(payload, uncertain);
				Files.deleteIfExists(payload);
			} catch (Exception error) {
				logger.error("Retaining FTS outbox {} and stopping replay ({}={})",
						payload, PROP_FAIL_ON_ERROR, failOnError, error);
				if (failOnError) {
					throw error;
				}
				return;
			}
		}
	}

	private void sendPayload(Path payloadFile, Path uncertain) throws Exception {
		PreparedPayload prepared = PreparedPayload.from(payloadFile, mapper);

		if (prepared.hasClear()) {
			deleteAndConfirm(mapper.writeValueAsString(Map.of("query", Map.of("match_all", Map.of()))), uncertain);
		}

		if (prepared.hasClearContexts()) {
			deleteAndConfirm(mapper.writeValueAsString(Map.of(
					"query", Map.of("terms", Map.of(FIELD_CONTEXT, prepared.clearedContexts())))), uncertain);
		}

		prepared.sendBulk(mapper, MAX_BULK_BYTES, chunk -> {
			for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
				ObjectNode fence = newFence("bulk");
				writeFence(uncertain, fence);
				String responseBody;
				try {
					responseBody = sendRequestStream(endpoint + bulkPath, chunk, chunk.length(), NDJSON,
							"while sending FTS updates", fence.path("requestId").asText());
				} catch (UncertainRequestException error) {
					reconcile(uncertain);
					if (attempt == MAX_ATTEMPTS) {
						throw new IOException("FTS bulk finished with an unknown result; retained for ordered replay",
								error);
					}
					continue;
				} catch (Exception error) {
					Files.deleteIfExists(uncertain);
					throw error;
				}
				Files.deleteIfExists(uncertain);
				validateBulkResponse(responseBody);
				return;
			}
		});
	}

	private void deleteAndConfirm(String query, Path uncertain) throws Exception {
		for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
			// Bulk acknowledgements do not make recent writes visible to delete-by-query's snapshot.
			validateShards(readResponse(sendRequestBody(endpoint + indexPath("_refresh"), "",
					ContentType.APPLICATION_JSON, "while refreshing FTS index")), false);
			ObjectNode fence = newFence("delete");
			writeFence(uncertain, fence);
			try {
				byte[] bytes = query.getBytes(StandardCharsets.UTF_8);
				String response = sendRequestStream(
						endpoint + deleteByQueryPath() + "?conflicts=proceed&refresh=true&wait_for_completion=false",
						() -> new ByteArrayInputStream(bytes), bytes.length, ContentType.APPLICATION_JSON,
						"while submitting FTS deletion", fence.path("requestId").asText());
				JsonNode submitted;
				try {
					submitted = readResponse(response);
				} catch (IOException error) {
					throw new UncertainRequestException("Invalid Elasticsearch deletion task acknowledgement", error);
				}
				if (!submitted.path("task").isTextual() || submitted.path("task").asText().isBlank()) {
					throw new UncertainRequestException("Elasticsearch did not return a deletion task ID", null);
				}
				fence.put("taskId", submitted.path("task").asText());
				writeFence(uncertain, fence);
			} catch (UncertainRequestException error) {
				// Discovery can prove acceptance, but an empty task listing cannot prove non-acceptance.
				discoverTask(uncertain, fence);
			} catch (Exception error) {
				if (!fence.has("taskId")) {
					Files.deleteIfExists(uncertain);
				}
				throw error;
			}
			JsonNode deleted = awaitTask(fence);
			Files.deleteIfExists(uncertain);
			if (!deleted.path("timed_out").isBoolean() || deleted.path("timed_out").asBoolean()
					|| !deleted.path("failures").isArray() || !deleted.path("failures").isEmpty()) {
				throw new IOException("Incomplete FTS delete-by-query response: " + deleted);
			}
			requireCount(deleted, "total");
			requireCount(deleted, "deleted");
			requireCount(deleted, "version_conflicts");
			JsonNode count = readResponse(sendRequestBody(endpoint + indexPath("_count"), query,
					ContentType.APPLICATION_JSON, "while confirming FTS deletion"));
			validateShards(count, true);
			long remaining = requireCount(count, "count");
			if (remaining == 0) {
				return;
			}
			if (attempt == MAX_ATTEMPTS) {
				throw new IOException("FTS delete-by-query left " + remaining + " matching documents after "
						+ MAX_ATTEMPTS + " attempts");
			}
			sleepBeforeRetry(attempt, new IOException("FTS deletion left " + remaining + " matching documents"));
		}
	}

	private ObjectNode newFence(String kind) {
		ObjectNode fence = mapper.createObjectNode();
		fence.put("version", 1);
		fence.put("kind", kind);
		fence.put("requestId", "linkedfactory-fts-" + UUID.randomUUID());
		return fence;
	}

	private void writeFence(Path path, ObjectNode fence) throws IOException {
		Path staging = path.resolveSibling(path.getFileName() + ".writing");
		byte[] bytes = mapper.writeValueAsBytes(fence);
		try (FileChannel channel = FileChannel.open(staging, StandardOpenOption.CREATE,
				StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
			java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(bytes);
			while (buffer.hasRemaining()) {
				channel.write(buffer);
			}
			channel.force(true);
		}
		Files.move(staging, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
		try (FileChannel directory = FileChannel.open(path.getParent(), StandardOpenOption.READ)) {
			directory.force(true);
		}
	}

	private IOException blocked(Path path) {
		return new IOException("FTS outbox blocked by an uncertain request: " + path
				+ ". No completion proof is available. Confirm the original Elasticsearch request/tasks"
				+ " have finished before removing only this marker and replaying.");
	}

	private void reconcile(Path path) throws Exception {
		JsonNode parsed;
		try {
			parsed = mapper.readTree(Files.readString(path));
		} catch (IOException error) {
			throw blocked(path);
		}
		if (!(parsed instanceof ObjectNode fence) || fence.path("version").asInt() != 1
				|| !Set.of("delete", "bulk").contains(fence.path("kind").asText())
				|| !fence.path("requestId").isTextual() || fence.path("requestId").asText().isBlank()) {
			throw blocked(path);
		}
		if (!fence.hasNonNull("taskId")) {
			discoverTask(path, fence);
		}
		awaitTask(fence);
		// The old request is finished. Replaying this payload is now safe under the drain lock.
		Files.delete(path);
	}

	private void discoverTask(Path path, ObjectNode fence) throws Exception {
		JsonNode listing = getResponse("/_tasks?detailed=true&group_by=none"
				+ "&actions=*delete/byquery,*bulk");
		if (!listing.path("tasks").isArray() || hasTaskFailures(listing, "node_failures")
				|| hasTaskFailures(listing, "task_failures")) {
			throw blocked(path);
		}
		List<String> matching = new ArrayList<>();
		for (JsonNode task : listing.path("tasks")) {
			if (fence.path("requestId").asText().equals(task.path("headers").path("X-Opaque-Id").asText())
					&& !task.hasNonNull("parent_task_id")
					&& task.path("node").isTextual() && task.path("id").isIntegralNumber()) {
				matching.add(task.path("node").asText() + ":" + task.path("id").asText());
			}
		}
		if (matching.size() != 1) {
			throw blocked(path);
		}
		fence.put("taskId", matching.getFirst());
		writeFence(path, fence);
	}

	private boolean hasTaskFailures(JsonNode response, String field) {
		return response.has(field) && (!response.path(field).isArray() || !response.path(field).isEmpty());
	}

	private JsonNode awaitTask(ObjectNode fence) throws Exception {
		String taskId = fence.path("taskId").asText();
		if (!taskId.matches("[A-Za-z0-9_-]+:[0-9]+")) {
			throw new IOException("Invalid persisted Elasticsearch task ID");
		}
		long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(requestConfig.getSocketTimeout());
		while (true) {
			if (System.nanoTime() >= deadline) {
				throw new IOException("FTS task " + taskId + " is still running; retained for automatic recovery");
			}
			JsonNode task;
			try {
				int remaining = (int) Math.max(1,
						TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime()));
				int wait = Math.max(1, Math.min(1_000, remaining / 2));
				task = getResponse("/_tasks/" + taskId + "?wait_for_completion=true&timeout=" + wait + "ms",
						deadline);
			} catch (HttpRequestException error) {
				// Only an observed ephemeral bulk task can be proven finished by its disappearance.
				if (error.status == 404 && "bulk".equals(fence.path("kind").asText())
						&& error.responseBody != null
						&& error.responseBody.path("error").path("reason").asText()
								.startsWith("task [" + taskId + "] isn't running")) {
					return mapper.createObjectNode();
				}
				throw error;
			}
			if (!task.path("completed").isBoolean()) {
				throw new IOException("Invalid Elasticsearch task response");
			}
			if (task.path("completed").asBoolean()) {
				// Even a task error proves termination; replay and delete confirmation handle partial work.
				return task.path("response");
			}
			if (System.nanoTime() >= deadline) {
				throw new IOException("FTS task " + taskId + " is still running; retained for automatic recovery");
			}
			try {
				TimeUnit.NANOSECONDS.sleep(Math.min(TimeUnit.MILLISECONDS.toNanos(100),
						Math.max(0, deadline - System.nanoTime())));
			} catch (InterruptedException error) {
				Thread.currentThread().interrupt();
				throw new IOException("Interrupted while awaiting FTS task " + taskId, error);
			}
		}
	}

	private JsonNode getResponse(String path) throws IOException {
		return getResponse(path, System.nanoTime()
				+ TimeUnit.MILLISECONDS.toNanos(requestConfig.getSocketTimeout()));
	}

	private JsonNode getResponse(String path, long deadline) throws IOException {
		for (int attempt = 1; ; attempt++) {
			long remaining = deadline - System.nanoTime();
			if (remaining <= 0) {
				throw new IOException("FTS task polling budget exhausted; retained for automatic recovery");
			}
			int timeout = (int) Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining));
			try {
				return getResponseOnce(path, timeout);
			} catch (IOException error) {
				boolean retryable = isRetryable(error)
						|| error instanceof HttpRequestException http
								&& (http.status == 408 || http.status == 502 || http.status == 504)
						|| error instanceof SocketTimeoutException
						|| error instanceof ConnectTimeoutException || error instanceof HttpHostConnectException;
				if (!retryable) {
					throw error;
				}
				if (!sleepBeforeRetry(attempt, error, deadline)) {
					throw new IOException("FTS task polling budget exhausted; retained for automatic recovery: "
							+ error.getMessage(), error);
				}
			}
		}
	}

	private JsonNode getResponseOnce(String path, int timeout) throws IOException {
		HttpGet request = new HttpGet(endpoint + path);
		request.setConfig(RequestConfig.copy(requestConfig).setSocketTimeout(timeout)
				.setConnectTimeout(Math.min(5_000, timeout)).setConnectionRequestTimeout(Math.min(5_000, timeout)).build());
		try (CloseableHttpResponse response = client().execute(request)) {
			int status = response.getStatusLine().getStatusCode();
			String body = response.getEntity() == null ? ""
					: EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
			if (status < 200 || status >= 300) {
				HttpRequestException error = new HttpRequestException(status,
						"HTTP " + status + " while checking FTS task: " + body);
				error.retryAfterMillis = retryAfterMillis(response);
				try {
					error.responseBody = mapper.readTree(body);
				} catch (IOException ignored) {
					// An unstructured error is not completion evidence.
				}
				throw error;
			}
			return readResponse(body);
		}
	}

	private JsonNode readResponse(String responseBody) throws IOException {
		JsonNode response = mapper.readTree(responseBody);
		if (response == null || !response.isObject()) {
			throw new IOException("Invalid Elasticsearch response");
		}
		return response;
	}

	private long requireCount(JsonNode response, String field) throws IOException {
		JsonNode value = response.path(field);
		if (!value.isIntegralNumber() || !value.canConvertToLong() || value.longValue() < 0) {
			throw new IOException("Invalid Elasticsearch response field " + field + ": " + response);
		}
		return value.longValue();
	}

	private void validateShards(JsonNode response, boolean requireAll) throws IOException {
		JsonNode shards = response.path("_shards");
		long total = requireCount(shards, "total");
		long successful = requireCount(shards, "successful");
		if (requireCount(shards, "failed") != 0 || successful > total
				|| (total > 0 && successful == 0) || (requireAll && successful < total)) {
			throw new IOException("Incomplete Elasticsearch shard response: " + response);
		}
	}

	private String sendRequestBody(String url, String body, ContentType contentType, String action) throws Exception {
		byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
		return sendRequestStream(url, () -> new java.io.ByteArrayInputStream(bytes), bytes.length, contentType, action);
	}

	private String sendRequestStream(String url, InputStreamSource body, long length, ContentType contentType, String action)
			throws Exception {
		return sendRequestStream(url, body, length, contentType, action, null);
	}

	private String sendRequestStream(String url, InputStreamSource body, long length, ContentType contentType,
			String action, String requestId) throws Exception {
		for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
			try (InputStream requestBody = body.openStream()) {
				HttpPost request = new HttpPost(url);
				request.setConfig(requestConfig);
				if (requestId != null) {
					request.setHeader("X-Opaque-Id", requestId);
				}
				request.setEntity(new InputStreamEntity(requestBody, length, contentType));
				try (CloseableHttpResponse response = executeRequest(request, action)) {
					int status = response.getStatusLine().getStatusCode();
					HttpEntity entity = response.getEntity();
					String responseBody;
					try {
						responseBody = entity == null ? "" : EntityUtils.toString(entity, StandardCharsets.UTF_8);
					} catch (IOException error) {
						throw new UncertainRequestException("Lost Elasticsearch response " + action, error);
					}
					if (status >= 200 && status < 300) {
						return responseBody;
					}
					if (status == 408 || status == 502 || status == 504) {
						throw new UncertainRequestException("HTTP " + status + " " + action
								+ "; Elasticsearch may still be processing the request: " + responseBody, null);
					}
					throw new HttpRequestException(status, "HTTP " + status + " " + action + ": " + responseBody);
				}
			} catch (IOException e) {
				if (attempt == MAX_ATTEMPTS || !isRetryable(e)) {
					throw e;
				}
				sleepBeforeRetry(attempt, e);
			}
		}
		throw new IOException("Unable to send FTS request");
	}

	private CloseableHttpResponse executeRequest(HttpPost request, String action) throws IOException {
		try {
			return client().execute(request);
		} catch (ConnectTimeoutException | HttpHostConnectException error) {
			throw error;
		} catch (IOException error) {
			// A transport failure is not proof the server stopped. Never replay it automatically.
			throw new UncertainRequestException("Uncertain Elasticsearch request " + action
					+ "; the server may still be processing it", error);
		}
	}

	private void validateBulkResponse(String responseBody) throws IOException {
		if (responseBody == null || responseBody.isBlank()) {
			return;
		}
		JsonNode root = mapper.readTree(responseBody);
		JsonNode items = root.path("items");
		if (!root.path("errors").asBoolean(false)) {
			return;
		}
		if (!items.isArray()) {
			throw new IOException("HTTP 500 while sending FTS updates: Elasticsearch bulk response reported errors.");
		}
		List<String> failures = new ArrayList<>();
		for (JsonNode item : items) {
			if (!item.isObject()) {
				continue;
			}
			var operations = item.properties();
			for (Map.Entry<String, JsonNode> operation : operations) {
				JsonNode detail = operation.getValue();
				int status = detail.path("status").asInt();
				if (status < 300) {
					continue;
				}
				JsonNode error = detail.path("error");
				if ("delete".equals(operation.getKey()) && status == 404
						&& "not_found".equals(detail.path("result").asText())
						&& (error.isMissingNode() || error.isNull())) {
					continue;
				}
				String id = detail.path("_id").asText("");
				String reason = error.isMissingNode() || error.isNull() ? detail.toString() : error.toString();
				failures.add("HTTP " + status + " bulk " + operation.getKey()
						+ (id.isEmpty() ? "" : " for " + id)
						+ ": " + reason);
			}
		}
		if (!failures.isEmpty()) {
			throw new IOException(String.join("; ", failures));
		}
	}

	private boolean isRetryable(IOException error) {
		return error instanceof HttpRequestException http
				&& (http.status == 429 || http.status == 500 || http.status == 503);
	}

	private void sleepBeforeRetry(int attempt, IOException error) throws IOException {
		sleepBeforeRetry(attempt, error, Long.MAX_VALUE);
	}

	private boolean sleepBeforeRetry(int attempt, IOException error, long deadline) throws IOException {
		long ceiling = Math.min(MAX_RETRY_BACKOFF_MILLIS,
				RETRY_BACKOFF_MILLIS << Math.min(attempt, 6));
		long floor = Math.max(RETRY_BACKOFF_MILLIS, ceiling / 2);
		long delay = floor + ThreadLocalRandom.current().nextLong(ceiling - floor + 1);
		if (error instanceof HttpRequestException http) {
			delay = Math.max(delay, http.retryAfterMillis);
		}
		long remaining = deadline == Long.MAX_VALUE ? Long.MAX_VALUE : deadline - System.nanoTime();
		if (remaining <= 0) {
			return false;
		}
		try {
			TimeUnit.NANOSECONDS.sleep(Math.min(TimeUnit.MILLISECONDS.toNanos(delay), remaining));
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IOException("Interrupted while retrying FTS request", e);
		}
		if (deadline != Long.MAX_VALUE && System.nanoTime() >= deadline) {
			return false;
		}
		logger.warn("Retrying FTS request attempt {} after transient failure: {}", attempt + 1, error.getMessage());
		return true;
	}

	private long retryAfterMillis(CloseableHttpResponse response) {
		var header = response.getFirstHeader("Retry-After");
		if (header == null) {
			return 0;
		}
		String value = header.getValue().trim();
		try {
			return Math.max(0, Math.multiplyExact(Long.parseLong(value), 1_000L));
		} catch (NumberFormatException | ArithmeticException ignored) {
			try {
				return Math.max(0, ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME)
						.toInstant().toEpochMilli() - System.currentTimeMillis());
			} catch (DateTimeParseException error) {
				return 0;
			}
		}
	}

	private void ensureOutboxDir() throws IOException {
		Files.createDirectories(outboxDir);
	}

	private Path normalizeOutboxDir(String value) {
		String dir = value == null || value.isBlank() ? DEFAULT_OUTBOX_DIR : value.trim();
		return Path.of(dir);
	}

	private interface InputStreamSource {
		InputStream openStream() throws IOException;
	}

	private static final class UncertainRequestException extends IOException {
		private UncertainRequestException(String message, IOException cause) {
			super(message, cause);
		}
	}

	private static final class HttpRequestException extends IOException {
		private final int status;
		private JsonNode responseBody;
		private long retryAfterMillis;

		private HttpRequestException(int status, String message) {
			super(message);
			this.status = status;
		}
	}

	private static final class TransactionState {
		private final Path file;
		private final BufferedWriter writer;
		private final ObjectMapper mapper;
		private boolean empty = true;
		private boolean finished = false;
		private boolean persisted = false;

		private TransactionState(Path file, BufferedWriter writer, ObjectMapper mapper) {
			this.file = file;
			this.writer = writer;
			this.mapper = mapper;
		}

		static TransactionState create(Path outboxDir, ObjectMapper mapper) {
			try {
				Files.createDirectories(outboxDir);
				Path file = Files.createTempFile(outboxDir, ".fts-http-bulk-", ".pending");
				BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8);
				return new TransactionState(file, writer, mapper);
			} catch (IOException e) {
				throw new RuntimeException("Unable to create FTS bulk payload", e);
			}
		}

		boolean isEmpty() {
			return empty;
		}

		Path persist(Path outboxDir) {
			finishPayload();
			if (persisted) {
				return file;
			}
			try {
				Files.createDirectories(outboxDir);
				long timestamp = System.currentTimeMillis();
				try (var files = Files.list(outboxDir)) {
					for (Path pending : (Iterable<Path>) files::iterator) {
						String name = pending.getFileName().toString();
						if (name.endsWith(".jsonl") && name.indexOf('-') > 0) {
							try {
								timestamp = Math.max(timestamp, Long.parseLong(name.substring(0, name.indexOf('-'))) + 1);
							} catch (NumberFormatException ignored) {
								// Older/custom payload names still participate in normal sorted replay.
							}
						}
					}
				}
				Path target = outboxDir.resolve(timestamp + "-" + UUID.randomUUID() + ".jsonl");
				Files.move(file, target, StandardCopyOption.ATOMIC_MOVE);
				persisted = true;
				return target;
			} catch (IOException e) {
				throw new RuntimeException("Unable to persist FTS bulk payload", e);
			}
		}

		void append(JsonNode op) {
			try {
				writer.write(mapper.writeValueAsString(op));
				writer.write('\n');
				writer.flush();
				empty = false;
			} catch (IOException e) {
				throw new RuntimeException("Unable to append to FTS bulk payload", e);
			}
		}

		Path finishPayload() {
			if (!finished) {
				try {
					writer.flush();
					writer.close();
					finished = true;
				} catch (IOException e) {
					throw new RuntimeException("Unable to finalize FTS bulk payload", e);
				}
			}
			return file;
		}

		void cleanup() {
			if (persisted) {
				return;
			}
			try {
				if (!finished) {
					writer.close();
				}
			} catch (IOException e) {
				throw new RuntimeException("Unable to close FTS bulk payload", e);
			} finally {
				try {
					Files.deleteIfExists(file);
				} catch (IOException e) {
					throw new RuntimeException("Unable to delete FTS bulk payload", e);
				}
			}
		}
	}

	private static final class PreparedPayload {
		private final Path file;
		private final List<String> clearedContexts = new ArrayList<>();
		private boolean clear;
		private boolean clearContexts;
		private long lastClear = -1;

		private PreparedPayload(Path file) {
			this.file = file;
		}

		static PreparedPayload from(Path payloadFile, ObjectMapper mapper) throws IOException {
			PreparedPayload prepared = new PreparedPayload(payloadFile);
			try (JsonParser parser = mapper.getFactory().createParser(payloadFile.toFile())) {
				long record = 0;
				while (parser.nextToken() != null) {
					ObjectNode op = readHeader(parser, mapper);
					String type = op.path("op").asText();
					if (OP_CLEAR.equals(type)) {
						prepared.clear = true;
						prepared.clearContexts = false;
						prepared.clearedContexts.clear();
						prepared.lastClear = record;
					} else if (OP_CLEAR_CONTEXTS.equals(type)) {
						prepared.clearContexts = true;
						JsonNode contexts = op.path("contexts");
						if (!contexts.isArray()) {
							throw new IOException("Invalid FTS outbox payload: clearContexts contexts must be an array");
						}
						prepared.clearedContexts.clear();
						for (JsonNode context : contexts) {
							prepared.clearedContexts.add(context.asText());
						}
					} else if (!Set.of(OP_STATEMENTS, OP_UPSERT, OP_REMOVE).contains(type)) {
						throw new IOException("Invalid FTS outbox payload: unsupported operation " + type);
					}
					record++;
				}
			}
			return prepared;
		}

		private static ObjectNode readHeader(JsonParser parser, ObjectMapper mapper) throws IOException {
			if (parser.currentToken() != JsonToken.START_OBJECT) {
				throw new IOException("Invalid FTS outbox operation");
			}
			ObjectNode header = mapper.createObjectNode();
			while (parser.nextToken() != JsonToken.END_OBJECT) {
				if (parser.currentToken() != JsonToken.FIELD_NAME) {
					throw new IOException("Invalid FTS outbox operation field");
				}
				String field = parser.currentName();
				parser.nextToken();
				if ("op".equals(field) || "contexts".equals(field)) {
					header.set(field, mapper.readTree(parser));
				} else if (Set.of("documents", "addedDocuments", "removedDocuments").contains(field)) {
					validateDocuments(parser);
				} else {
					parser.skipChildren();
				}
			}
			return header;
		}

		private static void validateDocuments(JsonParser parser) throws IOException {
			if (parser.currentToken() == JsonToken.VALUE_NULL) {
				return;
			}
			if (parser.currentToken() != JsonToken.START_OBJECT) {
				throw new IOException("Invalid FTS outbox payload: documents must be an object");
			}
			while (parser.nextToken() != JsonToken.END_OBJECT) {
				if (parser.currentToken() != JsonToken.FIELD_NAME || parser.nextToken() != JsonToken.START_OBJECT) {
					throw new IOException("Invalid FTS outbox payload: document payload must be an object");
				}
				parser.skipChildren();
			}
		}

		boolean hasClear() {
			return clear;
		}

		boolean hasClearContexts() {
			return clearContexts && !clearedContexts.isEmpty();
		}

		List<String> clearedContexts() {
			return clearedContexts;
		}

		void sendBulk(ObjectMapper mapper, int maxBytes, BulkSender sender) throws Exception {
			BulkChunkBuilder builder = new BulkChunkBuilder(maxBytes, sender);
			// Preserve the existing additions-before-removals semantics without retaining the whole upload.
			for (boolean create : new boolean[] { true, false }) {
				try (JsonParser headers = mapper.getFactory().createParser(file.toFile());
						JsonParser parser = mapper.getFactory().createParser(file.toFile())) {
					long record = 0;
					while (headers.nextToken() != null) {
						String type = readHeader(headers, mapper).path("op").asText();
						if (parser.nextToken() != JsonToken.START_OBJECT) {
							throw new IOException("Invalid FTS outbox operation");
						}
						while (parser.nextToken() != JsonToken.END_OBJECT) {
							if (parser.currentToken() != JsonToken.FIELD_NAME) {
								throw new IOException("Invalid FTS outbox operation field");
							}
							String field = parser.currentName();
							parser.nextToken();
							boolean selected = OP_STATEMENTS.equals(type)
									&& (create ? "addedDocuments" : "removedDocuments").equals(field)
									|| (create ? OP_UPSERT : OP_REMOVE).equals(type) && "documents".equals(field);
							if (selected && record > lastClear) {
								appendDocuments(parser, builder, create, mapper);
							} else {
								parser.skipChildren();
							}
						}
						record++;
					}
				}
			}
			builder.finish();
		}

		private static void appendDocuments(JsonParser parser, BulkChunkBuilder builder, boolean create,
				ObjectMapper mapper) throws Exception {
			if (parser.currentToken() == JsonToken.VALUE_NULL) {
				return;
			}
			if (parser.currentToken() != JsonToken.START_OBJECT) {
				throw new IOException("Invalid FTS outbox payload: documents must be an object");
			}
			String operation = create ? "index" : "delete";
			while (parser.nextToken() != JsonToken.END_OBJECT) {
				String id = parser.currentName();
				parser.nextToken();
				if (parser.currentToken() != JsonToken.START_OBJECT) {
					throw new IOException("Invalid FTS outbox payload: document payload must be an object");
				}
				List<byte[]> lines = new ArrayList<>(create ? 2 : 1);
				lines.add(actionLine(mapper, operation, id));
				if (create) {
					ObjectNode document = mapper.readTree(parser);
					document.remove(FIELD_ID);
					lines.add(documentLine(mapper, document));
				} else {
					parser.skipChildren();
				}
				builder.appendDocument(operation + ":" + id, lines);
			}
		}

		private static byte[] actionLine(ObjectMapper mapper, String operation, String id) throws IOException {
			ObjectNode action = mapper.createObjectNode();
			action.putObject(operation).put("_id", id);
			return serializeLine(mapper, action);
		}

		private static byte[] documentLine(ObjectMapper mapper, ObjectNode payload) throws IOException {
			return serializeLine(mapper, payload);
		}

		private static byte[] serializeLine(ObjectMapper mapper, JsonNode node) throws IOException {
			byte[] json = mapper.writeValueAsBytes(node);
			byte[] line = new byte[json.length + 1];
			System.arraycopy(json, 0, line, 0, json.length);
			line[json.length] = '\n';
			return line;
		}
	}

	private interface BulkSender {
		void send(BulkRequest request) throws Exception;
	}

	private static final class BulkChunkBuilder {
		private final int maxBytes;
		private final BulkSender sender;
		private final Map<String, List<byte[]>> documents = new LinkedHashMap<>();
		private long size;

		private BulkChunkBuilder(int maxBytes, BulkSender sender) {
			this.maxBytes = maxBytes;
			this.sender = sender;
		}

		void appendDocument(String key, List<byte[]> documentLines) throws Exception {
			long documentSize = 0L;
			for (byte[] line : documentLines) {
				documentSize += line.length;
			}
			List<byte[]> previous = documents.remove(key);
			if (previous != null) {
				for (byte[] line : previous) {
					size -= line.length;
				}
			}
			if (size > 0 && size + documentSize > maxBytes) {
				finish();
			}
			documents.put(key, documentLines);
			size += documentSize;
		}

		void finish() throws Exception {
			if (!documents.isEmpty()) {
				List<byte[]> lines = new ArrayList<>();
				documents.values().forEach(lines::addAll);
				sender.send(new BulkRequest(lines, size));
				documents.clear();
				size = 0L;
			}
		}
	}

	private record BulkRequest(List<byte[]> lines, long length) implements InputStreamSource {
		@Override
		public InputStream openStream() {
			List<InputStream> streams = new ArrayList<>(lines.size());
			for (byte[] line : lines) {
				streams.add(new ByteArrayInputStream(line));
			}
			return new SequenceInputStream(Collections.enumeration(streams));
		}
	}
}
