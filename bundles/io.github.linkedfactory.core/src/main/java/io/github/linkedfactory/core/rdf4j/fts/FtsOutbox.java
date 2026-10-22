package io.github.linkedfactory.core.rdf4j.fts;

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
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Durable FTS outbox for async index updates.
 * Stores FTS mutations as JSON entries that can be replayed on startup.
 */
public class FtsOutbox implements AutoCloseable {
	private static final Logger logger = LoggerFactory.getLogger(FtsOutbox.class);

	public static final String DEFAULT_OUTBOX_DIR = System.getProperty("java.io.tmpdir") + "/linkedfactory-fts-outbox";

	public enum Status {
		PENDING,
		IN_FLIGHT,
		APPLIED,
		FAILED,
		DEAD_LETTER
	}

	/**
	 * Represents a single FTS mutation operation.
	 */
	public record FtsMutation(
		String id,
		Status status,
		String operationType, // "add", "remove", "clear", "clearContext"
		String subject,
		String predicate,
		String object,
		String objectType, // "iri" or "literal"
		String language,
		String datatype,
		String context,
		Instant createdAt,
		Instant lastAttemptedAt,
		int attemptCount,
		String payload
	) {}

	private static final String FILENAME = "outbox.ndjson";

	private final Path outboxDir;
	private final Path outboxFile;
	private final Object lock = new Object();
	private final ConcurrentMap<String, FtsMutation> memoryIndex = new ConcurrentHashMap<>();

	private final int maxAttempts;
	private final long retryBackoffMillis;
	private final long maxStalenessMillis;

	public FtsOutbox(Path outboxDir) {
		this(outboxDir, 5, 100L, 60_000L); // 5 retries, 100ms backoff, 60s staleness
	}

	public FtsOutbox(Path outboxDir, int maxAttempts, long retryBackoffMillis, long maxStalenessMillis) {
		this.outboxDir = outboxDir;
		this.outboxFile = outboxDir.resolve(FILENAME);
		this.maxAttempts = maxAttempts;
		this.retryBackoffMillis = retryBackoffMillis;
		this.maxStalenessMillis = maxStalenessMillis;

		try {
			Files.createDirectories(outboxDir);
			load();
		} catch (IOException e) {
			throw new RuntimeException("Failed to initialize FTS outbox at " + outboxDir, e);
		}
	}

	private void load() throws IOException {
		if (!Files.exists(outboxFile)) {
			return;
		}

		try (var lines = Files.lines(outboxFile, StandardCharsets.UTF_8)) {
			lines.forEach(line -> {
				if (line.isBlank()) return;
				try {
					FtsMutation mutation = parseMutation(line);
					memoryIndex.put(mutation.id(), mutation);
				} catch (Exception e) {
					logger.warn("Failed to parse outbox entry: {}", line.substring(0, Math.min(100, line.length())), e);
				}
			});
		}
		logger.info("Loaded {} FTS mutations from outbox", memoryIndex.size());
	}

	public synchronized String addAddMutation(Statement statement) {
		return addMutation("add", statement);
	}

	public synchronized String addRemoveMutation(Statement statement) {
		return addMutation("remove", statement);
	}

	public synchronized String addClearMutation() {
		String id = nextId();
		FtsMutation mutation = new FtsMutation(
			id,
			Status.PENDING,
			"clear",
			null, null, null, null, null, null, null,
			Instant.now(), null, 0,
			"{}"
		);
		append(mutation);
		return id;
	}

	public synchronized String addClearContextMutation(Resource... contexts) {
		StringBuilder payload = new StringBuilder("{\"contexts\":[");
		if (contexts != null) {
			for (int i = 0; i < contexts.length; i++) {
				if (i > 0) payload.append(',');
				payload.append('"').append(contexts[i].stringValue()).append('"');
			}
		}
		payload.append("]}");

		String id = nextId();
		FtsMutation mutation = new FtsMutation(
			id,
			Status.PENDING,
			"clearContext",
			null, null, null, null, null, null, null,
			Instant.now(), null, 0,
			payload.toString()
		);
		append(mutation);
		return id;
	}

	private String addMutation(String operationType, Statement statement) {
		Value object = statement.getObject();
		String objectType = object instanceof IRI ? "iri" : "literal";

		String subject = statement.getSubject().stringValue();
		String predicate = statement.getPredicate().stringValue();
		String objectValue = object instanceof Literal ? ((Literal) object).getLabel() : object.stringValue();
		String language = object instanceof Literal ? ((Literal) object).getLanguage().orElse(null) : null;
		String datatype = object instanceof Literal ? ((Literal) object).getDatatype() != null ? ((Literal) object).getDatatype().stringValue() : null : null;
		String context = statement.getContext() != null ? statement.getContext().stringValue() : null;

		StringBuilder payload = new StringBuilder();
		payload.append("{\"subject\":\"").append(escapeJson(subject)).append('"');
		payload.append(",\"predicate\":\"").append(escapeJson(predicate)).append('"');
		payload.append(",\"object\":\"").append(escapeJson(objectValue)).append('"');
		payload.append(",\"objectType\":\"").append(objectType).append('"');
		if (language != null) {
			payload.append(",\"language\":\"").append(escapeJson(language)).append('"');
		}
		if (datatype != null) {
			payload.append(",\"datatype\":\"").append(escapeJson(datatype)).append('"');
		}
		if (context != null) {
			payload.append(",\"context\":\"").append(escapeJson(context)).append('"');
		}
		payload.append('}');

		String id = nextId();
		FtsMutation mutation = new FtsMutation(
			id,
			Status.PENDING,
			operationType,
			subject, predicate, objectValue, objectType, language, datatype, context,
			Instant.now(), null, 0,
			payload.toString()
		);
		append(mutation);
		return id;
	}

	private void append(FtsMutation mutation) {
		String json = toJson(mutation);
		try {
			Files.write(outboxFile, (json + '\n').getBytes(StandardCharsets.UTF_8), java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
			memoryIndex.put(mutation.id(), mutation);
		} catch (IOException e) {
			throw new RuntimeException("Failed to write to FTS outbox", e);
		}
	}

	private String toJson(FtsMutation m) {
		return String.format(
			"{\"id\":\"%s\",\"status\":\"%s\",\"operationType\":\"%s\",\"subject\":%s,\"predicate\":%s,\"object\":%s,\"objectType\":%s,\"language\":%s,\"datatype\":%s,\"context\":%s,\"createdAt\":\"%s\",\"lastAttemptedAt\":%s,\"attemptCount\":%d,\"payload\":%s}",
			m.id(),
			m.status().name(),
			m.operationType(),
			quote(m.subject()),
			quote(m.predicate()),
			quote(m.object()),
			quote(m.objectType()),
			quote(m.language()),
			quote(m.datatype()),
			quote(m.context()),
			m.createdAt().toString(),
			m.lastAttemptedAt() != null ? "\"" + m.lastAttemptedAt().toString() + "\"" : "null",
			m.attemptCount(),
			m.payload()
		);
	}

	private String quote(String value) {
		return value == null ? "null" : "\"" + escapeJson(value) + "\"";
	}

	private String escapeJson(String value) {
		if (value == null) return "";
		return value.replace("\\", "\\\\")
			.replace("\"", "\\\"")
			.replace("\n", "\\n")
			.replace("\r", "\\r")
			.replace("\t", "\\t");
	}

	public List<FtsMutation> getPendingOrFailedMutations() {
		return memoryIndex.values().stream()
			.filter(m -> m.status() == Status.PENDING || m.status() == Status.FAILED)
			.filter(m -> {
				if (m.status() == Status.FAILED && m.attemptCount() >= maxAttempts) {
					return false; // Don't retry dead-lettered entries
				}
				// Check staleness
				return System.currentTimeMillis() - m.createdAt().toEpochMilli() <= maxStalenessMillis;
			})
			.toList();
	}

	public FtsMutation markInFlight(String id) {
		FtsMutation existing = memoryIndex.get(id);
		if (existing == null || existing.status() != Status.PENDING) {
			return null;
		}

		FtsMutation updated = new FtsMutation(
			existing.id(),
			Status.IN_FLIGHT,
			existing.operationType(),
			existing.subject(), existing.predicate(), existing.object(), existing.objectType(),
			existing.language(), existing.datatype(), existing.context(),
			existing.createdAt(), Instant.now(), existing.attemptCount(),
			existing.payload()
		);

		synchronized (lock) {
			try {
				overwriteAll();
				memoryIndex.put(id, updated);
				rewriteFile();
			} catch (IOException e) {
				logger.error("Failed to update outbox entry {} to IN_FLIGHT", id, e);
				return null;
			}
		}

		return updated;
	}

	public void markApplied(String id) {
		FtsMutation existing = memoryIndex.get(id);
		if (existing == null) return;

		FtsMutation updated = new FtsMutation(
			existing.id(),
			Status.APPLIED,
			existing.operationType(),
			existing.subject(), existing.predicate(), existing.object(), existing.objectType(),
			existing.language(), existing.datatype(), existing.context(),
			existing.createdAt(), existing.lastAttemptedAt(), existing.attemptCount(),
			existing.payload()
		);

		memoryIndex.put(id, updated);
		// Don't rewrite file immediately - batch updates
	}

	public void markFailed(String id) {
		FtsMutation existing = memoryIndex.get(id);
		if (existing == null) return;

		int newAttemptCount = existing.attemptCount() + 1;
		Status newStatus = newAttemptCount >= maxAttempts ? Status.DEAD_LETTER : Status.FAILED;

		FtsMutation updated = new FtsMutation(
			existing.id(),
			newStatus,
			existing.operationType(),
			existing.subject(), existing.predicate(), existing.object(), existing.objectType(),
			existing.language(), existing.datatype(), existing.context(),
			existing.createdAt(), Instant.now(), newAttemptCount,
			existing.payload()
		);

		memoryIndex.put(id, updated);
	}

	public List<FtsMutation> getDeadLetterMutations() {
		return memoryIndex.values().stream()
			.filter(m -> m.status() == Status.DEAD_LETTER)
			.toList();
	}

	public void clearApplied() {
		memoryIndex.values().stream()
			.filter(m -> m.status() == Status.APPLIED)
			.forEach(m -> memoryIndex.remove(m.id()));
		rewriteFile();
	}

	public long getPendingCount() {
		return memoryIndex.values().stream()
			.filter(m -> m.status() == Status.PENDING)
			.count();
	}

	public long getFailedCount() {
		return memoryIndex.values().stream()
			.filter(m -> m.status() == Status.FAILED)
			.count();
	}

	public long getDeadLetterCount() {
		return memoryIndex.values().stream()
			.filter(m -> m.status() == Status.DEAD_LETTER)
			.count();
	}

	public int maxAttempts() {
		return maxAttempts;
	}

	public long retryBackoffMillis() {
		return retryBackoffMillis;
	}

	@Override
	public void close() {
		// Keep in-memory index for next restart
	}

	private void overwriteAll() throws IOException {
		// Temp file for atomic write
		Path temp = Files.createTempFile(outboxDir, "outbox-", ".tmp");
		try (var out = Files.newBufferedWriter(temp, StandardCharsets.UTF_8)) {
			for (FtsMutation m : memoryIndex.values()) {
				out.write(toJson(m));
				out.write('\n');
			}
		}
		Files.move(temp, outboxFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
	}

	private void rewriteFile() {
		try {
			overwriteAll();
		} catch (IOException e) {
			logger.error("Failed to rewrite outbox file", e);
		}
	}

	private String nextId() {
		return UUID.randomUUID().toString();
	}

	private static FtsMutation parseMutation(String json) {
		// Simple JSON parsing - extract key fields
		String id = extractString(json, "\"id\"");
		Status status = Status.valueOf(extractString(json, "\"status\""));
		String operationType = extractString(json, "\"operationType\"");

		String subject = extractString(json, "\"subject\"");
		String predicate = extractString(json, "\"predicate\"");
		String object = extractString(json, "\"object\"");
		String objectType = extractString(json, "\"objectType\"");
		String language = extractString(json, "\"language\"");
		String datatype = extractString(json, "\"datatype\"");
		String context = extractString(json, "\"context\"");

		String createdAtStr = extractString(json, "\"createdAt\"");
		Instant createdAt = createdAtStr != null ? Instant.parse(createdAtStr) : null;

		String lastAttemptedStr = extractString(json, "\"lastAttemptedAt\"");
		Instant lastAttemptedAt = lastAttemptedStr != null ? Instant.parse(lastAttemptedStr) : null;

		int attemptCount = extractInt(json, "\"attemptCount\"");

		String payload = extractPayload(json);

		return new FtsMutation(
			id, status, operationType,
			subject, predicate, object, objectType, language, datatype, context,
			createdAt, lastAttemptedAt, attemptCount, payload
		);
	}

	private static String extractString(String json, String key) {
		int idx = json.indexOf(key);
		if (idx < 0) return null;
		int start = json.indexOf('"', idx + key.length() + 1);
		if (start < 0) return null;
		int end = json.indexOf('"', start + 1);
		if (end < 0) return null;
		return json.substring(start + 1, end);
	}

	private static int extractInt(String json, String key) {
		int idx = json.indexOf(key);
		if (idx < 0) return 0;
		int start = json.indexOf(':', idx);
		if (start < 0) return 0;
		int end = start + 1;
		while (end < json.length() && Character.isDigit(json.charAt(end))) end++;
		if (end <= start + 1) return 0;
		try {
			return Integer.parseInt(json.substring(start + 1, end));
		} catch (NumberFormatException e) {
			return 0;
		}
	}

	private static String extractPayload(String json) {
		int idx = json.indexOf("\"payload\":");
		if (idx < 0) return "{}";
		int start = json.indexOf('{', idx);
		if (start < 0) return "{}";
		int braceCount = 1;
		int end = start + 1;
		while (end < json.length() && braceCount > 0) {
			char c = json.charAt(end);
			if (c == '{') braceCount++;
			else if (c == '}') braceCount--;
			end++;
		}
		return json.substring(start, end);
	}
}
