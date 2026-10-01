package io.github.linkedfactory.core.rdf4j.fts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class HttpFtsSearchService implements FtsSearchService {
	private static final Logger logger = LoggerFactory.getLogger(HttpFtsSearchService.class);

	private static final int MAX_ATTEMPTS = 3;
	private static final long RETRY_BACKOFF_MILLIS = 100L;
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
	private static final String DEFAULT_OUTBOX_DIR = Path.of(System.getProperty("java.io.tmpdir"),
			"linkedfactory-fts-outbox").toString();

	private final ObjectMapper mapper = new ObjectMapper();
	private volatile String endpoint = "";
	private volatile String bulkPath = "/_bulk";
	private volatile boolean failOnError = true;
	private volatile Path outboxDir = Path.of(DEFAULT_OUTBOX_DIR);
	private final RequestConfig requestConfig = RequestConfig.custom()
			.setConnectTimeout(5_000)
			.setConnectionRequestTimeout(5_000)
			.setSocketTimeout(30_000)
			.build();

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
		this.endpoint = normalizeEndpoint(endpoint);
		this.bulkPath = normalizePath(bulkPath);
		this.failOnError = failOnError;
		this.outboxDir = normalizeOutboxDir(outboxDir);
	}

	public final void configure(Map<String, Object> properties) {
		this.endpoint = normalizeEndpoint(stringProp(properties, PROP_ENDPOINT, ""));
		this.bulkPath = normalizePath(stringProp(properties, PROP_BULK_PATH, "/_bulk"));
		this.failOnError = booleanProp(properties, PROP_FAIL_ON_ERROR, true);
		this.outboxDir = normalizeOutboxDir(stringProp(properties, PROP_OUTBOX_DIR, DEFAULT_OUTBOX_DIR));
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
		txState.set(TransactionState.create(mapper));
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
		if (tx == null || tx.isEmpty()) {
			drainOutbox();
			cleanup(tx);
			txState.remove();
			return;
		}

		if (endpoint.isEmpty()) {
			logger.debug("Skipping FTS update: no {} configured.", PROP_ENDPOINT);
			cleanup(tx);
			txState.remove();
			return;
		}

		try {
			tx.persist(outboxDir);
			drainOutbox();
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
		if (bulkPath.endsWith("/_bulk")) {
			return bulkPath.substring(0, bulkPath.length() - "_bulk".length()) + "_delete_by_query";
		}
		return "/_delete_by_query";
	}

	private ObjectNode statementBatch(Collection<Statement> added, Collection<Statement> removed) {
		ObjectNode op = mapper.createObjectNode();
		op.put("op", OP_STATEMENTS);
		ObjectNode addedDocuments = documentsByStatement(added);
		if (addedDocuments.size() > 0) {
			op.set("addedDocuments", addedDocuments);
		}
		ObjectNode removedDocuments = documentsByStatement(removed);
		if (removedDocuments.size() > 0) {
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

	private void drainOutbox() throws Exception {
		if (endpoint.isEmpty()) {
			return;
		}
		ensureOutboxDir();
		List<Path> pending;
		try (var files = Files.list(outboxDir)) {
			pending = files.filter(path -> path.getFileName().toString().endsWith(".json"))
					.sorted()
					.toList();
		}
		for (Path payload : pending) {
			boolean sent = sendPayload(payload);
			if (sent) {
				Files.deleteIfExists(payload);
			}
		}
	}

	private boolean sendPayload(Path payloadFile) throws Exception {
		String payload = Files.readString(payloadFile, StandardCharsets.UTF_8);
		JsonNode root = mapper.readTree(payload);
		JsonNode operationsNode = root.path("operations");
		if (!operationsNode.isArray()) {
			throw new IOException("Invalid FTS outbox payload: missing operations array");
		}

		ArrayNode operations = (ArrayNode) operationsNode;
		PreparedPayload prepared = PreparedPayload.from(operations);

		if (prepared.hasClear()) {
			if (!sendRequest(
					endpoint + deleteByQueryPath(),
					mapper.writeValueAsString(Map.of("query", Map.of("match_all", Map.of()))),
					ContentType.APPLICATION_JSON,
					"while clearing FTS index"
			)) {
				return false;
			}
		}

		if (prepared.hasClearContexts()) {
			if (!sendRequest(
					endpoint + deleteByQueryPath(),
					mapper.writeValueAsString(Map.of(
							"query", Map.of(
									"terms", Map.of(FIELD_CONTEXT, prepared.clearedContexts())
							)
					)),
					ContentType.APPLICATION_JSON,
					"while clearing FTS contexts"
			)) {
				return false;
			}
		}

		String ndjson = prepared.toBulkNdjson(mapper);
		if (ndjson.isBlank()) {
			return true;
		}
		String responseBody = sendRequestBody(endpoint + bulkPath, ndjson, NDJSON, "while sending FTS updates");
		if (responseBody == null) {
			return false;
		}
		validateBulkResponse(responseBody);
		return true;
	}

	private boolean sendRequest(String url, String body, ContentType contentType, String action) throws Exception {
		return sendRequestBody(url, body, contentType, action) != null;
	}

	private String sendRequestBody(String url, String body, ContentType contentType, String action) throws Exception {
		IOException lastError = null;
		for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
			try {
				HttpPost request = new HttpPost(url);
				request.setConfig(requestConfig);
				request.setEntity(new StringEntity(body, contentType));
				try (CloseableHttpResponse response = client().execute(request)) {
					int status = response.getStatusLine().getStatusCode();
					HttpEntity entity = response.getEntity();
					String responseBody = entity == null ? "" : EntityUtils.toString(entity, StandardCharsets.UTF_8);
					if (status < 300) {
						return responseBody;
					}
					IOException error = new IOException("HTTP " + status + " " + action + ": " + responseBody);
					if (!isRetryable(error) || attempt == MAX_ATTEMPTS) {
						if (failOnError) {
							throw error;
						}
						logger.error("Ignoring FTS update failure because {}=false", PROP_FAIL_ON_ERROR, error);
						return null;
					}
					lastError = error;
					sleepBeforeRetry(attempt, error);
				}
			} catch (IOException e) {
				if (attempt == MAX_ATTEMPTS || !isRetryable(e)) {
					if (failOnError) {
						throw e;
					}
					logger.error("Ignoring FTS update failure because {}=false", PROP_FAIL_ON_ERROR, e);
					return null;
				}
				lastError = e;
				sleepBeforeRetry(attempt, e);
			}
		}
		if (lastError != null && failOnError) {
			throw lastError;
		}
		return null;
	}

	private void validateBulkResponse(String responseBody) throws IOException {
		if (responseBody == null || responseBody.isBlank()) {
			return;
		}
		JsonNode root = mapper.readTree(responseBody);
		if (!root.path("errors").asBoolean(false)) {
			return;
		}
		JsonNode items = root.path("items");
		if (!items.isArray()) {
			throw new IOException("HTTP 500 while sending FTS updates: Elasticsearch bulk response reported errors.");
		}
		List<String> failures = new ArrayList<>();
		for (JsonNode item : items) {
			if (!item.isObject()) {
				continue;
			}
			var operations = item.fields();
			while (operations.hasNext()) {
				var operation = operations.next();
				JsonNode detail = operation.getValue();
				int status = detail.path("status").asInt();
				if (status < 300) {
					continue;
				}
				JsonNode error = detail.path("error");
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
		String message = error.getMessage();
		if (message == null) {
			return false;
		}
		return message.contains("HTTP 429") || message.contains("HTTP 500") || message.contains("HTTP 502")
				|| message.contains("HTTP 503") || message.contains("HTTP 504");
	}

	private void sleepBeforeRetry(int attempt, IOException error) throws IOException {
		try {
			Thread.sleep(RETRY_BACKOFF_MILLIS * attempt);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IOException("Interrupted while retrying FTS update", e);
		}
		logger.warn("Retrying FTS update attempt {} after transient failure: {}", attempt + 1, error.getMessage());
	}

	private void ensureOutboxDir() throws IOException {
		Files.createDirectories(outboxDir);
	}

	private Path normalizeOutboxDir(String value) {
		String dir = value == null || value.isBlank() ? DEFAULT_OUTBOX_DIR : value.trim();
		return Path.of(dir);
	}

	private static final class TransactionState {
		private final Path file;
		private final java.io.BufferedWriter writer;
		private final ObjectMapper mapper;
		private boolean empty = true;
		private boolean finished = false;
		private boolean persisted = false;

		private TransactionState(Path file, java.io.BufferedWriter writer, ObjectMapper mapper) {
			this.file = file;
			this.writer = writer;
			this.mapper = mapper;
		}

		static TransactionState create(ObjectMapper mapper) {
			try {
				Path file = Files.createTempFile("fts-http-bulk-", ".json");
				java.io.BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8);
				writer.write("{\"operations\":[");
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
				Path target = outboxDir.resolve(System.currentTimeMillis() + "-" + System.nanoTime() + ".json");
				Files.move(file, target, StandardCopyOption.REPLACE_EXISTING);
				persisted = true;
				return target;
			} catch (IOException e) {
				throw new RuntimeException("Unable to persist FTS bulk payload", e);
			}
		}

		void append(JsonNode op) {
			try {
				if (!empty) {
					writer.write(',');
				}
				writer.write(mapper.writeValueAsString(op));
				writer.flush();
				empty = false;
			} catch (IOException e) {
				throw new RuntimeException("Unable to append to FTS bulk payload", e);
			}
		}

		Path finishPayload() {
			if (!finished) {
				try {
					writer.write("]}");
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
		private final Map<String, ObjectNode> upserts = new LinkedHashMap<>();
		private final Map<String, ObjectNode> removals = new LinkedHashMap<>();
		private final List<String> clearedContexts = new ArrayList<>();
		private boolean clear;
		private boolean clearContexts;

		static PreparedPayload from(ArrayNode operations) throws IOException {
			PreparedPayload prepared = new PreparedPayload();
			for (JsonNode op : operations) {
				String type = op.path("op").asText();
				if (OP_CLEAR.equals(type)) {
					prepared.clear = true;
					prepared.clearContexts = false;
					prepared.clearedContexts.clear();
					prepared.upserts.clear();
					prepared.removals.clear();
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
				} else if (OP_STATEMENTS.equals(type)) {
					mergeDocuments(op.get("addedDocuments"), prepared.upserts);
					mergeDocuments(op.get("removedDocuments"), prepared.removals);
				} else if (OP_UPSERT.equals(type)) {
					mergeDocuments(op.get("documents"), prepared.upserts);
				} else if (OP_REMOVE.equals(type)) {
					mergeDocuments(op.get("documents"), prepared.removals);
				} else {
					throw new IOException("Invalid FTS outbox payload: unsupported operation " + type);
				}
			}
			return prepared;
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

		String toBulkNdjson(ObjectMapper mapper) throws IOException {
			StringBuilder ndjson = new StringBuilder();
			appendDocuments(ndjson, upserts, true, mapper);
			appendDocuments(ndjson, removals, false, mapper);
			return ndjson.toString();
		}

		private static void appendDocuments(StringBuilder ndjson, Map<String, ObjectNode> documents, boolean create,
				ObjectMapper mapper) throws IOException {
			String operation = create ? "index" : "delete";
			for (Map.Entry<String, ObjectNode> entry : documents.entrySet()) {
				ObjectNode action = mapper.createObjectNode();
				action.putObject(operation).put("_id", entry.getKey());
				ndjson.append(mapper.writeValueAsString(action)).append('\n');
				if (create) {
					ObjectNode payload = entry.getValue().deepCopy();
					payload.remove(FIELD_ID);
					ndjson.append(mapper.writeValueAsString(payload)).append('\n');
				}
			}
		}

		private static void mergeDocuments(JsonNode documentsNode, Map<String, ObjectNode> target) throws IOException {
			if (documentsNode == null || documentsNode.isNull()) {
				return;
			}
			if (!documentsNode.isObject()) {
				throw new IOException("Invalid FTS outbox payload: documents must be an object");
			}
			var documents = documentsNode.fields();
			while (documents.hasNext()) {
				var document = documents.next();
				if (!document.getValue().isObject()) {
					throw new IOException("Invalid FTS outbox payload: document payload must be an object");
				}
				target.put(document.getKey(), (ObjectNode) document.getValue().deepCopy());
			}
		}
	}
}
