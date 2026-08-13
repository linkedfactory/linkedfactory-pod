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
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
	private static final String SUBJECT_FIELD = "subject";
	private static final String ADD_SCRIPT = "if (!ctx._source.containsKey('subject')) { ctx._source.subject = params.subject; } "
			+ "for (entry in params.fields.entrySet()) { "
			+ "def key = entry.getKey(); "
			+ "if (!ctx._source.containsKey(key)) { ctx._source[key] = []; } "
			+ "for (value in entry.getValue()) { "
			+ "if (!ctx._source[key].contains(value)) { ctx._source[key].add(value); } "
			+ "} "
			+ "}";
	private static final String CLEAR_CONTEXTS_SCRIPT = "for (field in new ArrayList(ctx._source.keySet())) { "
			+ "if (field == 'subject') { continue; } "
			+ "def values = ctx._source[field]; "
			+ "if (values instanceof List) { "
			+ "for (int i = values.size() - 1; i >= 0; i--) { "
			+ "def value = values[i]; "
			+ "if (value instanceof Map && value.containsKey('context') && params.contexts.contains(value.context)) { "
			+ "values.remove(i); "
			+ "} "
			+ "} "
			+ "if (values.isEmpty()) { ctx._source.remove(field); } "
			+ "} "
			+ "} "
			+ "if (ctx._source.isEmpty() || (ctx._source.size() == 1 && ctx._source.containsKey('subject'))) { "
			+ "ctx.op = 'delete'; }";
	private static final String REMOVE_SCRIPT = "for (entry in params.fields.entrySet()) { "
			+ "def key = entry.getKey(); "
			+ "if (ctx._source.containsKey(key)) { "
			+ "ctx._source[key].removeAll(entry.getValue()); "
			+ "if (ctx._source[key].isEmpty()) { ctx._source.remove(key); } "
			+ "} "
			+ "} "
			+ "if (ctx._source.isEmpty() || (ctx._source.size() == 1 && ctx._source.containsKey('subject'))) { "
			+ "ctx.op = 'delete'; }";

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
		if (!added.isEmpty() || !removed.isEmpty()) {
			state().append(statementBatch(added, removed));
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
	private String deleteByQueryPath() {
		if (bulkPath.endsWith("/_bulk")) {
			return bulkPath.substring(0, bulkPath.length() - "_bulk".length()) + "_delete_by_query";
		}
		return "/_delete_by_query";
	}

	private String updateByQueryPath() {
		if (bulkPath.endsWith("/_bulk")) {
			return bulkPath.substring(0, bulkPath.length() - "_bulk".length()) + "_update_by_query";
		}
		return "/_update_by_query";
	}

	private String sendRequest(String url, String body, ContentType contentType, String operation) throws IOException {
		HttpPost request = new HttpPost(url);
		request.setConfig(requestConfig);
		request.setEntity(new StringEntity(body, contentType));
		try (CloseableHttpResponse response = client().execute(request)) {
			int status = response.getStatusLine().getStatusCode();
			HttpEntity entity = response.getEntity();
			String responseBody = entity == null ? "" : EntityUtils.toString(entity, StandardCharsets.UTF_8);
			if (status >= 300) {
				throw new IOException("HTTP " + status + " " + operation + ": " + responseBody);
			}
			return responseBody;
		}
	}


	@Override
	public void rollback() {
		cleanup(txState.get());
		txState.remove();
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

	private TransactionState state() {
		TransactionState state = txState.get();
		if (state == null) {
			state = TransactionState.create(mapper);
			txState.set(state);
		}
		return state;
	}

	private JsonNode statementBatch(Collection<Statement> added, Collection<Statement> removed) {
		ObjectNode op = mapper.createObjectNode();
		op.put("op", OP_STATEMENTS);
		ObjectNode addedDocuments = documentsBySubject(added);
		if (addedDocuments.size() > 0) {
			op.set("addedDocuments", addedDocuments);
		}
		ObjectNode removedDocuments = documentsBySubject(removed);
		if (removedDocuments.size() > 0) {
			op.set("removedDocuments", removedDocuments);
		}
		return op;
	}

	private ObjectNode documentsBySubject(Collection<Statement> statements) {
		ObjectNode documents = mapper.createObjectNode();
		Map<String, Map<String, List<JsonNode>>> grouped = new LinkedHashMap<>();
		for (Statement statement : statements) {
			Resource subject = statement.getSubject();
			IRI predicate = statement.getPredicate();
			Value object = statement.getObject();
			String subjectId = subject.stringValue();
			String predicateId = predicate.stringValue();
			Map<String, List<JsonNode>> fields = grouped.computeIfAbsent(subjectId, key -> new LinkedHashMap<>());
			List<JsonNode> values = fields.computeIfAbsent(predicateId, key -> new ArrayList<>());
			values.add(statementValue(object, statement.getContext()));
		}

		for (Map.Entry<String, Map<String, List<JsonNode>>> doc : grouped.entrySet()) {
			ObjectNode fieldNode = mapper.createObjectNode();
			for (Map.Entry<String, List<JsonNode>> field : doc.getValue().entrySet()) {
				ArrayNode values = mapper.createArrayNode();
				for (JsonNode value : field.getValue()) {
					values.add(value);
				}
				fieldNode.set(field.getKey(), values);
			}
			documents.set(doc.getKey(), fieldNode);
		}
		return documents;
	}

	private JsonNode statementValue(Value object, Resource context) {
		ObjectNode valueNode = mapper.createObjectNode();
		if (object instanceof Literal) {
			Literal literal = (Literal) object;
			valueNode.put("kind", "literal");
			valueNode.put("value", literal.getLabel());
			if (literal.getLanguage().isPresent()) {
				valueNode.put("language", literal.getLanguage().get());
			}
			if (literal.getDatatype() != null) {
				valueNode.put("datatype", literal.getDatatype().stringValue());
			}
		} else if (object instanceof IRI) {
			valueNode.put("kind", "iri");
			valueNode.put("value", object.stringValue());
		} else {
			valueNode.put("kind", "value");
			valueNode.put("value", object.stringValue());
		}
		if (context != null) {
			valueNode.put("context", context.stringValue());
		}
		return valueNode;
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
		if (tx != null) {
			try {
				tx.cleanup();
			} catch (RuntimeException e) {
				logger.warn("Unable to clean up FTS bulk payload", e);
			}
		}
	}

	private void drainOutbox() throws Exception {
		if (endpoint.isEmpty()) {
			return;
		}
		ensureOutboxDir();
		try (Stream<Path> files = Files.list(outboxDir)) {
			List<Path> pending = files
					.filter(path -> path.getFileName().toString().endsWith(".json"))
					.sorted(Comparator.comparing(path -> path.getFileName().toString()))
					.collect(Collectors.toList());
			for (Path file : pending) {
				try {
					sendPayloadWithRetries(file);
					Files.deleteIfExists(file);
				} catch (Exception e) {
					if (failOnError) {
						throw e;
					}
					logger.error("Ignoring FTS update failure because {}=false", PROP_FAIL_ON_ERROR, e);
					return;
				}
			}
		}
	}

	private void sendPayloadWithRetries(Path payload) throws Exception {
		Exception lastError = null;
		for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
			try {
				sendPayload(payload);
				return;
			} catch (IOException e) {
				lastError = e;
				if (!isRetryable(e) || attempt == MAX_ATTEMPTS) {
					throw e;
				}
				sleepBeforeRetry(attempt, e);
			}
		}
		if (lastError != null) {
			throw lastError;
		}
	}

	private void sendPayload(Path payload) throws Exception {

		JsonNode root = mapper.readTree(payload.toFile());
		JsonNode operationsNode = root.path("operations");

		if (!operationsNode.isArray()) {
			throw new IOException(
					"Invalid FTS outbox payload: missing operations array"
			);
		}

		ArrayNode operations = (ArrayNode) operationsNode;
		PreparedPayload prepared = PreparedPayload.from(operations, mapper);

		if (prepared.hasClear()) {
			sendRequest(
					endpoint + deleteByQueryPath(),
					mapper.writeValueAsString(
							Map.of(
									"query",
									Map.of(
											"match_all",
											Map.of()
									)
							)
					),
					ContentType.APPLICATION_JSON,
					"while clearing FTS index"
			);
		}

		if (prepared.hasClearContexts()) {
			sendRequest(
					endpoint + updateByQueryPath(),
					mapper.writeValueAsString(
							Map.of(
									"query", Map.of("match_all", Map.of()),
									"script", Map.of(
											"lang", "painless",
											"source", CLEAR_CONTEXTS_SCRIPT,
											"params", Map.of("contexts", prepared.clearedContexts())
									)
							)
					),
					ContentType.APPLICATION_JSON,
					"while clearing FTS contexts"
			);
		}

		String ndjson = prepared.toBulkNdjson(mapper);

		if (ndjson.isBlank()) {
			return;
		}

		String url = endpoint + bulkPath;

		HttpPost request = new HttpPost(url);
		request.setConfig(requestConfig);

		request.setEntity(
				new StringEntity(
						ndjson,
						NDJSON
				)
		);

		String responseBody = sendRequest(url, ndjson, NDJSON, "while sending FTS updates");
		validateBulkResponse(responseBody);
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
		private final List<StatementOperation> statementOperations = new ArrayList<>();
		private final List<String> clearedContexts = new ArrayList<>();
		private boolean clear;
		private boolean clearContexts;

		static PreparedPayload from(ArrayNode operations, ObjectMapper mapper) throws IOException {
			PreparedPayload prepared = new PreparedPayload();
			for (JsonNode op : operations) {
				String type = op.path("op").asText();
				if (OP_CLEAR.equals(type)) {
					prepared.clear = true;
					prepared.clearContexts = false;
					prepared.statementOperations.clear();
					prepared.clearedContexts.clear();
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
				} else if (OP_STATEMENTS.equals(type) || OP_UPSERT.equals(type) || OP_REMOVE.equals(type)) {
					prepared.addStatementOperation(StatementOperation.from(op, mapper));
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
			for (StatementOperation operation : statementOperations) {
				operation.appendNdjson(ndjson, mapper);
			}
			return ndjson.toString();
		}

		private void addStatementOperation(StatementOperation current) {
			if (current.isEmpty()) {
				return;
			}
			if (!statementOperations.isEmpty()) {
				StatementOperation previous = statementOperations.get(statementOperations.size() - 1);
				if (previous.canMergeWith(current)) {
					previous.merge(current);
					return;
				}
			}
			statementOperations.add(current);
		}

		private static final class StatementOperation {
			private final Map<String, Map<String, ArrayNode>> addedDocuments = new LinkedHashMap<>();
			private final Map<String, Map<String, ArrayNode>> removedDocuments = new LinkedHashMap<>();

			static StatementOperation from(JsonNode op, ObjectMapper mapper) throws IOException {
				StatementOperation statementOperation = new StatementOperation();
				String type = op.path("op").asText();
				if (OP_STATEMENTS.equals(type)) {
					mergeDocuments(op.get("addedDocuments"), statementOperation.addedDocuments, mapper);
					mergeDocuments(op.get("removedDocuments"), statementOperation.removedDocuments, mapper);
				} else if (OP_UPSERT.equals(type)) {
					mergeDocuments(op.get("documents"), statementOperation.addedDocuments, mapper);
				} else if (OP_REMOVE.equals(type)) {
					mergeDocuments(op.get("documents"), statementOperation.removedDocuments, mapper);
				}
				return statementOperation;
			}

			boolean isEmpty() {
				return addedDocuments.isEmpty() && removedDocuments.isEmpty();
			}

			boolean canMergeWith(StatementOperation other) {
				return hasAddedOnly() && other.hasAddedOnly() || hasRemovedOnly() && other.hasRemovedOnly();
			}

			void merge(StatementOperation other) {
				mergeDocuments(other.addedDocuments, addedDocuments);
				mergeDocuments(other.removedDocuments, removedDocuments);
			}

			void appendNdjson(StringBuilder ndjson, ObjectMapper mapper) throws IOException {
				appendDocuments(ndjson, addedDocuments, true, mapper);
				appendDocuments(ndjson, removedDocuments, false, mapper);
			}

			private boolean hasAddedOnly() {
				return !addedDocuments.isEmpty() && removedDocuments.isEmpty();
			}

			private boolean hasRemovedOnly() {
				return addedDocuments.isEmpty() && !removedDocuments.isEmpty();
			}

			private void appendDocuments(StringBuilder ndjson, Map<String, Map<String, ArrayNode>> documents, boolean upsert,
					ObjectMapper mapper) throws IOException {
				for (Map.Entry<String, Map<String, ArrayNode>> doc : documents.entrySet()) {
					ObjectNode action = mapper.createObjectNode();
					ObjectNode update = action.putObject("update");
					update.put("_id", doc.getKey());

					ObjectNode fields = mapper.createObjectNode();
					for (Map.Entry<String, ArrayNode> field : doc.getValue().entrySet()) {
						fields.set(field.getKey(), field.getValue());
					}

					ObjectNode payload = mapper.createObjectNode();
					if (upsert) {
						ObjectNode script = payload.putObject("script");
						script.put("lang", "painless");
						script.put("source", ADD_SCRIPT);
						ObjectNode params = mapper.createObjectNode();
						params.put("subject", doc.getKey());
						params.set("fields", fields);
						script.set("params", params);
						payload.put("scripted_upsert", true);
						payload.set("upsert", mapper.createObjectNode().put(SUBJECT_FIELD, doc.getKey()));
					} else {
						ObjectNode script = payload.putObject("script");
						script.put("lang", "painless");
						script.put("source", REMOVE_SCRIPT);
						script.set("params", mapper.createObjectNode().set("fields", fields));
					}

					ndjson.append(mapper.writeValueAsString(action)).append('\n');
					ndjson.append(mapper.writeValueAsString(payload)).append('\n');
				}
			}

			private static void mergeDocuments(JsonNode documentsNode, Map<String, Map<String, ArrayNode>> target,
					ObjectMapper mapper) throws IOException {
				if (documentsNode == null || documentsNode.isNull()) {
					return;
				}
				if (!documentsNode.isObject()) {
					throw new IOException("Invalid FTS outbox payload: documents must be an object");
				}
				var documents = documentsNode.fields();
				while (documents.hasNext()) {
					var doc = documents.next();
					if (!doc.getValue().isObject()) {
						throw new IOException("Invalid FTS outbox payload: document fields must be an object");
					}
					Map<String, ArrayNode> mergedFields = target.computeIfAbsent(doc.getKey(), key -> new LinkedHashMap<>());
					var fields = doc.getValue().fields();
					while (fields.hasNext()) {
						var field = fields.next();
						if (!field.getValue().isArray()) {
							throw new IOException("Invalid FTS outbox payload: field values must be an array");
						}
						ArrayNode mergedValues = mergedFields.computeIfAbsent(field.getKey(),
								key -> mapper.createArrayNode());
						for (JsonNode value : field.getValue()) {
							mergedValues.add(value.deepCopy());
						}
					}
				}
			}

			private static void mergeDocuments(Map<String, Map<String, ArrayNode>> source,
					Map<String, Map<String, ArrayNode>> target) {
				for (Map.Entry<String, Map<String, ArrayNode>> doc : source.entrySet()) {
					Map<String, ArrayNode> mergedFields = target.computeIfAbsent(doc.getKey(), key -> new LinkedHashMap<>());
					for (Map.Entry<String, ArrayNode> field : doc.getValue().entrySet()) {
						ArrayNode mergedValues = mergedFields.computeIfAbsent(field.getKey(),
								key -> field.getValue().deepCopy());
						if (mergedValues == field.getValue()) {
							continue;
						}
						for (JsonNode value : field.getValue()) {
							mergedValues.add(value.deepCopy());
						}
					}
				}
			}
		}
	}
}
