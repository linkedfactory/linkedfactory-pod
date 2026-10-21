package io.github.linkedfactory.core.rdf4j.fts;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.lance.Dataset;
import org.lance.WriteParams;
import org.lance.index.IndexOptions;
import org.lance.index.IndexParams;
import org.lance.index.IndexType;
import org.lance.index.scalar.ScalarIndexParams;
import org.lance.ipc.FullTextQuery;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class LanceFtsIndex implements AutoCloseable {
	private static final Schema SCHEMA = new Schema(Arrays.asList(
			Field.nullable("subject", ArrowType.Utf8.INSTANCE),
			Field.nullable("predicate", ArrowType.Utf8.INSTANCE),
			Field.nullable("value", ArrowType.Utf8.INSTANCE),
			Field.nullable("is_iri", ArrowType.Bool.INSTANCE),
			Field.nullable("context", ArrowType.Utf8.INSTANCE)));

	public record FieldValue(String subject, String predicate, String value, boolean iri, String context) {
	}

	private final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
	private final String uri;
	private final Object lock = new Object();
	private Dataset dataset;

	public LanceFtsIndex(String uri) {
		this.uri = uri;
	}

	public void applyAddRemove(List<FieldValue> added, List<FieldValue> removed) throws Exception {
		synchronized (lock) {
			if (!removed.isEmpty()) {
				deleteRows(removed);
			}
			if (!added.isEmpty()) {
				appendRows(added);
			}
		}
	}

	public void clearAll() throws Exception {
		synchronized (lock) {
			if (dataset != null) {
				dataset.delete("true");
			}
		}
	}

	public void clearContexts(Set<String> contexts) throws Exception {
		if (contexts == null || contexts.isEmpty() || dataset == null) {
			return;
		}
		synchronized (lock) {
			StringBuilder inList = new StringBuilder();
			for (String context : contexts) {
				if (inList.length() > 0) {
					inList.append(",");
				}
				inList.append(quote(context));
			}
			dataset.delete("context IN (" + inList + ")");
		}
	}

	public List<FtsSearchHit> search(FtsSearchRequest request) throws Exception {
		synchronized (lock) {
			if (dataset == null) {
				return List.of();
			}
			float boost = request.getBoost() != null ? request.getBoost().floatValue() : 1.0f;
			FullTextQuery query = FullTextQuery.match(
					request.getKeywords(), "value", boost, Optional.empty(), 50, FullTextQuery.Operator.OR, 0);

			List<String> filters = new ArrayList<>();
			if (request.getField() != null && !request.getField().isBlank()) {
				filters.add("predicate = " + quote(request.getField()));
			}
			if (request.getIriFilter() != null && !request.getIriFilter().isBlank()) {
				filters.add("subject = " + quote(request.getIriFilter()));
			}

			ScanOptions.Builder optionsBuilder = new ScanOptions.Builder()
					.fullTextQuery(query)
					.columns(List.of("subject"))
					.limit(request.getLimit() > 0 ? request.getLimit() : 100);
			if (!filters.isEmpty()) {
				optionsBuilder.filter(String.join(" AND ", filters));
			}

			List<FtsSearchHit> hits = new ArrayList<>();
			try (LanceScanner scanner = dataset.newScan(optionsBuilder.build())) {
				try (ArrowReader reader = scanner.scanBatches()) {
					while (reader.loadNextBatch()) {
						VectorSchemaRoot root = reader.getVectorSchemaRoot();
						VarCharVector subjectVector = (VarCharVector) root.getVector("subject");
						for (int i = 0; i < root.getRowCount(); i++) {
							hits.add(new FtsSearchHit(new String(subjectVector.get(i), StandardCharsets.UTF_8), null, null));
						}
					}
				}
			}
			return hits;
		}
	}

	@Override
	public void close() throws Exception {
		synchronized (lock) {
			if (dataset != null) {
				dataset.close();
			}
		}
		allocator.close();
	}

	private void appendRows(List<FieldValue> rows) throws Exception {
		byte[] arrowData = encode(rows);
		try (ByteArrayInputStream in = new ByteArrayInputStream(arrowData);
				ArrowStreamReader streamReader = new ArrowStreamReader(in, allocator)) {
			WriteParams.WriteMode mode = dataset == null ? WriteParams.WriteMode.CREATE : WriteParams.WriteMode.APPEND;
			Dataset written = Dataset.write()
					.allocator(allocator)
					.reader(streamReader)
					.uri(uri)
					.mode(mode)
					.execute();
			if (dataset != null && dataset != written) {
				dataset.close();
			}
			dataset = written;
		}
		reindexValueColumn();
	}

	private void reindexValueColumn() {
		ScalarIndexParams invertedParams = ScalarIndexParams.create(
				"inverted", "{\"base_tokenizer\":\"simple\",\"language\":\"English\",\"with_position\":true}");
		IndexParams indexParams = IndexParams.builder().setScalarIndexParams(invertedParams).build();
		dataset.createIndex(
				IndexOptions.builder(Collections.singletonList("value"), IndexType.INVERTED, indexParams)
						.withIndexName("value_idx")
						.replace(true)
						.build());
	}

	private static final int DELETE_BATCH_SIZE = 5000;

	private void deleteRows(List<FieldValue> rows) throws Exception {
		if (dataset == null) {
			return;
		}
		for (int offset = 0; offset < rows.size(); offset += DELETE_BATCH_SIZE) {
			List<FieldValue> batch = rows.subList(offset, Math.min(offset + DELETE_BATCH_SIZE, rows.size()));
			List<String> predicates = new ArrayList<>();
			for (FieldValue row : batch) {
				StringBuilder predicate = new StringBuilder();
				predicate.append("subject = ").append(quote(row.subject()))
						.append(" AND predicate = ").append(quote(row.predicate()))
						.append(" AND value = ").append(quote(row.value()));
				if (row.context() == null) {
					predicate.append(" AND context IS NULL");
				} else {
					predicate.append(" AND context = ").append(quote(row.context()));
				}
				predicates.add("(" + predicate + ")");
			}
			dataset.delete(String.join(" OR ", predicates));
		}
	}

	private byte[] encode(List<FieldValue> rows) throws Exception {
		try (VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
			VarCharVector subjectVector = (VarCharVector) root.getVector("subject");
			VarCharVector predicateVector = (VarCharVector) root.getVector("predicate");
			VarCharVector valueVector = (VarCharVector) root.getVector("value");
			BitVector iriVector = (BitVector) root.getVector("is_iri");
			VarCharVector contextVector = (VarCharVector) root.getVector("context");
			subjectVector.allocateNew();
			predicateVector.allocateNew();
			valueVector.allocateNew();
			iriVector.allocateNew();
			contextVector.allocateNew();
			for (int i = 0; i < rows.size(); i++) {
				FieldValue row = rows.get(i);
				subjectVector.setSafe(i, row.subject().getBytes(StandardCharsets.UTF_8));
				predicateVector.setSafe(i, row.predicate().getBytes(StandardCharsets.UTF_8));
				valueVector.setSafe(i, row.value().getBytes(StandardCharsets.UTF_8));
				iriVector.setSafe(i, row.iri() ? 1 : 0);
				if (row.context() == null) {
					contextVector.setNull(i);
				} else {
					contextVector.setSafe(i, row.context().getBytes(StandardCharsets.UTF_8));
				}
			}
			root.setRowCount(rows.size());

			ByteArrayOutputStream out = new ByteArrayOutputStream();
			try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
				writer.start();
				writer.writeBatch();
				writer.end();
			}
			return out.toByteArray();
		}
	}

	private String quote(String value) {
		return "'" + value.replace("'", "''") + "'";
	}
}
