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
import org.apache.http.entity.FileEntity;
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
	private static final String REMOVE_SCRIPT = "for (entry in params.fields.entrySet()) { "
			+ "def key = entry.getKey(); "
			+ "if (ctx._source.containsKey(key)) { "
			+ "ctx._source[key].removeAll(entry.getValue()); "
			+ "if (ctx._source[key].isEmpty()) { ctx._source.remove(key); } "
			+ "} "
			+ "} "
			+ "if (ctx._source.isEmpty()) { ctx.op = 'delete'; }";

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
		TransactionState tx = state();
		if (!added.isEmpty()) {
			tx.append(statementBatch("upsert", added));
		}
		if (!removed.isEmpty()) {
			tx.append(statementBatch("remove", removed));
		}
	}

	@Override
	public void clearContexts(Resource... contexts) {
		ObjectNode op = mapper.createObjectNode();
		op.put("op", "clearContexts");
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
		op.put("op", "clear");
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
	private boolean containsOperation(ArrayNode operations, String opName) {
		for (JsonNode op : operations) {
			if (opName.equals(op.path("op").asText())) {
				return true;
			}
		}
		return false;
	}

	private String toBulkNdjson(ArrayNode operations) throws IOException {
		StringBuilder ndjson = new StringBuilder();
		for (JsonNode op : operations) {
			String type = op.path("op").asText();
			if (!("upsert".equals(type) || "remove".equals(type))) {
				continue;
			}
			JsonNode documents = op.path("documents");
			if (!documents.isObject()) {
				continue;
			}

			var docs = documents.fields();
			while (docs.hasNext()) {
				var entry = docs.next();
				String id = entry.getKey();
				JsonNode fields = entry.getValue();

				ObjectNode action = mapper.createObjectNode();
				ObjectNode update = action.putObject("update");
				update.put("_id", id);

				ObjectNode payload = mapper.createObjectNode();
				if ("upsert".equals(type)) {
					payload.set("doc", fields);
					payload.put("doc_as_upsert", true);
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
		return ndjson.toString();
	}

	private String deleteByQueryPath() {
		if (bulkPath.endsWith("/_bulk")) {
			return bulkPath.substring(0, bulkPath.length() - "_bulk".length()) + "_delete_by_query";
		}
		return "/_delete_by_query";
	}

	private void sendRequest(String url, String body, ContentType contentType, String operation) throws IOException {
		HttpPost request = new HttpPost(url);
		request.setEntity(new StringEntity(body, contentType));
		try (CloseableHttpResponse response = client().execute(request)) {
			int status = response.getStatusLine().getStatusCode();
			if (status >= 300) {
				HttpEntity entity = response.getEntity();
				String responseBody = entity == null ? "" : EntityUtils.toString(entity, StandardCharsets.UTF_8);
				IOException error = new IOException("HTTP " + status + " " + operation + ": " + responseBody);
				if (failOnError) {
					throw error;
				}
				logger.error("Ignoring FTS update failure because {}=false", PROP_FAIL_ON_ERROR, error);
			}
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

	private JsonNode statementBatch(String operation, Collection<Statement> statements) {
		ObjectNode op = mapper.createObjectNode();
		op.put("op", operation);
		ObjectNode documents = mapper.createObjectNode();
		op.set("documents", documents);

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
		return op;
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

		if (containsOperation(operations, "clear")) {
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

		if (containsOperation(operations, "clearContexts")) {
			logger.warn(
					"clearContexts is not mapped to native Elasticsearch operations and was ignored."
			);
		}

		String ndjson = toBulkNdjson(operations);

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

		try (CloseableHttpResponse response = client().execute(request)) {

			int status =
					response.getStatusLine().getStatusCode();

			if (status >= 300) {
				HttpEntity entity = response.getEntity();

				String body =
						entity == null
								? ""
								: EntityUtils.toString(
								entity,
								StandardCharsets.UTF_8
						);

				throw new IOException(
						"HTTP "
								+ status
								+ " while sending FTS updates: "
								+ body
				);
			}
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
}
