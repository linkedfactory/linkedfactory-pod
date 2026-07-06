package io.github.linkedfactory.core.rdf4j.fts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpEntity;
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
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Modified;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@Component(service = FtsSearchService.class)
public class HttpFtsSearchService implements FtsSearchService {
	private static final Logger logger = LoggerFactory.getLogger(HttpFtsSearchService.class);

	static final String PROP_ENDPOINT = "fts.endpoint";
	static final String PROP_BULK_PATH = "fts.bulkPath";
	static final String PROP_FAIL_ON_ERROR = "fts.failOnError";

	private final ObjectMapper mapper = new ObjectMapper();
	private volatile String endpoint = "";
	private volatile String bulkPath = "/fts/bulk";
	private volatile boolean failOnError = true;

	private volatile CloseableHttpClient httpClient;
	private final ThreadLocal<TransactionState> txState = new ThreadLocal<>();

	@Activate
	@Modified
	void activate(Map<String, Object> properties) {
		this.endpoint = normalizeEndpoint(stringProp(properties, PROP_ENDPOINT, ""));
		this.bulkPath = normalizePath(stringProp(properties, PROP_BULK_PATH, "/fts/bulk"));
		this.failOnError = booleanProp(properties, PROP_FAIL_ON_ERROR, true);
	}

	@Deactivate
	void deactivate() throws IOException {
		CloseableHttpClient client = httpClient;
		httpClient = null;
		if (client != null) {
			client.close();
		}
	}

	@Override
	public void begin() {
		txState.set(new TransactionState());
	}

	@Override
	public void addRemoveStatements(Set<Statement> added, Set<Statement> removed) {
		TransactionState tx = state();
		if (!added.isEmpty()) {
			tx.operations.add(statementBatch("upsert", added));
		}
		if (!removed.isEmpty()) {
			tx.operations.add(statementBatch("remove", removed));
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
		state().operations.add(op);
	}

	@Override
	public void clear() {
		ObjectNode op = mapper.createObjectNode();
		op.put("op", "clear");
		state().operations.add(op);
	}

	@Override
	public void commit() throws Exception {
		TransactionState tx = txState.get();
		if (tx == null || tx.operations.isEmpty()) {
			txState.remove();
			return;
		}
		if (endpoint.isEmpty()) {
			logger.debug("Skipping FTS update: no {} configured.", PROP_ENDPOINT);
			txState.remove();
			return;
		}

		ObjectNode payload = mapper.createObjectNode();
		ArrayNode operations = payload.putArray("operations");
		for (JsonNode op : tx.operations) {
			operations.add(op);
		}

		String url = endpoint + bulkPath;
		HttpPost request = new HttpPost(url);
		request.setEntity(new StringEntity(mapper.writeValueAsString(payload), ContentType.APPLICATION_JSON));
		try (CloseableHttpResponse response = client().execute(request)) {
			int status = response.getStatusLine().getStatusCode();
			if (status >= 300) {
				HttpEntity entity = response.getEntity();
				String body = entity == null ? "" : EntityUtils.toString(entity, StandardCharsets.UTF_8);
				IOException error = new IOException("HTTP " + status + " while sending FTS updates: " + body);
				if (failOnError) {
					throw error;
				}
				logger.error("Ignoring FTS update failure because {}=false", PROP_FAIL_ON_ERROR, error);
			}
		} finally {
			txState.remove();
		}
	}

	@Override
	public void rollback() {
		txState.remove();
	}

	private CloseableHttpClient client() {
		CloseableHttpClient existing = httpClient;
		if (existing != null) {
			return existing;
		}
		synchronized (this) {
			if (httpClient == null) {
				httpClient = HttpClients.createDefault();
			}
			return httpClient;
		}
	}

	private TransactionState state() {
		TransactionState state = txState.get();
		if (state == null) {
			state = new TransactionState();
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
		String value = Objects.requireNonNullElse(path, "/fts/bulk").trim();
		if (value.isEmpty()) {
			value = "/fts/bulk";
		}
		return value.startsWith("/") ? value : "/" + value;
	}

	private static final class TransactionState {
		private final List<JsonNode> operations = new ArrayList<>();
	}
}
