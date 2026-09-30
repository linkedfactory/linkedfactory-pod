package io.github.linkedfactory.core.kvin.iceberg;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinListener;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.Record;
import io.github.linkedfactory.core.kvin.parquet.Records;
import io.github.linkedfactory.core.kvin.util.AggregatingIterator;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.commons.iterator.WrappedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ParquetProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.UUID;

/**
 * Iceberg-backed KVIN store. URI IDs and values are stored in Iceberg tables.
 */
public class KvinIceberg implements Kvin {
	private static final Logger log = LoggerFactory.getLogger(KvinIceberg.class);
	private static final int BATCH_SIZE = 8192;
	private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();
	private static final Schema ID_SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()), Types.NestedField.required(2, "value", Types.StringType.get()));
	private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "itemId", Types.LongType.get()), Types.NestedField.required(2, "contextId", Types.LongType.get()), Types.NestedField.required(3, "propertyId", Types.LongType.get()), Types.NestedField.required(4, "time", Types.LongType.get()), Types.NestedField.required(5, "seqNr", Types.IntegerType.get()), Types.NestedField.required(6, "first", Types.BooleanType.get()), Types.NestedField.optional(7, "valueInt", Types.IntegerType.get()), Types.NestedField.optional(8, "valueLong", Types.LongType.get()), Types.NestedField.optional(9, "valueFloat", Types.FloatType.get()), Types.NestedField.optional(10, "valueDouble", Types.DoubleType.get()), Types.NestedField.optional(11, "valueString", Types.StringType.get()), Types.NestedField.optional(12, "valueBool", Types.BooleanType.get()), Types.NestedField.optional(13, "valueObject", Types.BinaryType.get()));
	private static final Comparator<org.apache.iceberg.data.Record> ORDER = Comparator.comparingLong((org.apache.iceberg.data.Record r) -> (Long) r.get(0)).thenComparingLong(r -> (Long) r.get(1)).thenComparingLong(r -> (Long) r.get(2)).thenComparing((a, b) -> Long.compare((Long) b.get(3), (Long) a.get(3))).thenComparing((a, b) -> Integer.compare((Integer) b.get(4), (Integer) a.get(4)));

	private final Path root;
	private final Table table;
	private final IdTable[] ids = new IdTable[3];

	public KvinIceberg(String archiveLocation) {
		this(archiveLocation, null);
	}

	public KvinIceberg(String archiveLocation, Duration retentionPeriod) {
		this.root = Path.of(archiveLocation).toAbsolutePath().normalize();
		if (retentionPeriod != null) {
			throw new UnsupportedOperationException("Iceberg retention cleanup is not implemented");
		}
		try {
			Files.createDirectories(root);
			String location = root.resolve("iceberg").toString();
			HadoopTables tables = new HadoopTables(new Configuration());
			boolean existing = Files.exists(root.resolve("iceberg/metadata"));
			if (existing) {
				table = tables.load(location);
				if (!table.schema().sameSchema(SCHEMA) || !table.spec().isUnpartitioned()) {
					throw new IllegalStateException("Incompatible Iceberg table at " + location);
				}
			} else {
				if (Files.exists(root.resolve("metadata"))) {
					throw new IllegalArgumentException("Existing KvinParquet archive cannot be opened as KvinIceberg: " + root);
				}
				table = tables.create(SCHEMA, SPEC, tableProperties(), location);
			}
			String[] names = {"items", "contexts", "properties"};
			for (int i = 0; i < ids.length; i++) {
				Path path = root.resolve("iceberg-ids").resolve(names[i]);
				if (Files.exists(path.resolve("metadata"))) {
					ids[i] = new IdTable(tables.load(path.toString()));
					if (!ids[i].table.schema().sameSchema(ID_SCHEMA) || !ids[i].table.spec().isUnpartitioned()) {
						throw new IllegalStateException("Incompatible Iceberg ID table at " + path);
					}
				} else {
					if (existing && table.currentSnapshot() != null) {
						throw new IOException("Missing Iceberg ID table at " + path);
					}
					ids[i] = new IdTable(tables.create(ID_SCHEMA, SPEC, tableProperties(), path.toString()));
				}
			}
		} catch (IOException e) {
			throw new UncheckedIOException("Unable to initialize Iceberg store at " + root, e);
		}
	}

	private static Map<String, String> tableProperties() {
		return Map.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name(), TableProperties.FORMAT_VERSION, "2", TableProperties.PARQUET_COMPRESSION, "zstd");
	}

	private long id(int kind, URI uri) {
		return ids[kind].id(uri);
	}

	private final class IdTable {
		final Table table;
		final Cache<URI, Long> forward = CacheBuilder.newBuilder().maximumSize(10000).build();
		final Cache<Long, URI> reverse = CacheBuilder.newBuilder().maximumSize(10000).build();
		long nextId;

		IdTable(Table table) {
			this.table = table;
		}

		void refresh() {
			table.refresh();
			nextId = 0;
			scan(Expressions.alwaysTrue(), row -> nextId = Math.max(nextId, (Long) row.get(0)));
		}

		long id(URI uri) {
			Long cached = forward.getIfPresent(uri);
			if (cached != null) return cached;
			final long[] result = {0};
			scan(Expressions.equal("value", uri.toString()), row -> {
				if (!uri.toString().contentEquals(row.get(1).toString())) return;
				long found = (Long) row.get(0);
				if (result[0] != 0 && result[0] != found) {
					throw new IllegalStateException("Duplicate Iceberg URI mapping for " + uri);
				}
				result[0] = found;
			});
			if (result[0] != 0) {
				forward.put(uri, result[0]);
				reverse.put(result[0], uri);
			}
			return result[0];
		}

		URI uri(long id) {
			URI cached = reverse.getIfPresent(id);
			if (cached != null) return cached;
			final URI[] result = {null};
			scan(Expressions.equal("id", id), row -> {
				if ((Long) row.get(0) != id) return;
				URI found = URIs.createURI(row.get(1).toString());
				if (result[0] != null && !result[0].equals(found)) {
					throw new IllegalStateException("Duplicate Iceberg ID mapping for " + id);
				}
				result[0] = found;
			});
			if (result[0] == null) throw new IllegalStateException("Unknown Iceberg URI ID: " + id);
			reverse.put(id, result[0]);
			forward.put(result[0], id);
			return result[0];
		}

		void scan(Expression filter, java.util.function.Consumer<org.apache.iceberg.data.Record> consumer) {
			try (CloseableIterable<FileScanTask> tasks = table.newScan().filter(filter).planFiles()) {
				for (FileScanTask task : tasks) {
					if (!task.deletes().isEmpty()) {
						throw new IllegalStateException("Iceberg delete files are not supported by KvinIceberg ID reads");
					}
					try (CloseableIterable<org.apache.iceberg.data.Record> rows = Parquet.read(table.io().newInputFile(task.file().location())).project(ID_SCHEMA).filter(filter).createReaderFunc(schema -> GenericParquetReaders.buildReader(ID_SCHEMA, schema)).build()) {
						for (var row : rows) consumer.accept(row);
					}
				}
			} catch (IOException e) {
				throw new UncheckedIOException("Unable to read Iceberg ID table", e);
			}
		}

		void append(Map<URI, Long> mappings) throws IOException {
			if (mappings.isEmpty()) return;
			String path = root.resolve("iceberg-ids/data/" + UUID.randomUUID() + ".parquet").toString();
			Files.createDirectories(Path.of(path).getParent());
			var appender = Parquet.write(table.io().newOutputFile(path)).forTable(table).writerVersion(ParquetProperties.WriterVersion.PARQUET_2_0).createWriterFunc(GenericParquetWriter::buildWriter).<org.apache.iceberg.data.Record>build();
			DataWriter<org.apache.iceberg.data.Record> writer = new DataWriter<>(appender, FileFormat.PARQUET, path, SPEC, null, null);
			try (writer) {
				for (var entry : mappings.entrySet()) {
					GenericRecord row = GenericRecord.create(ID_SCHEMA);
					row.set(0, entry.getValue());
					row.set(1, entry.getKey().toString());
					writer.write(row);
				}
			}
			table.newAppend().appendFile(writer.toDataFile()).commit();
			for (var entry : mappings.entrySet()) {
				forward.put(entry.getKey(), entry.getValue());
				reverse.put(entry.getValue(), entry.getKey());
				nextId = Math.max(nextId, entry.getValue());
			}
		}
	}

	private long assign(int kind, URI uri, Map<URI, Long>[] pending) {
		Long staged = pending[kind].get(uri);
		if (staged != null) return staged;
		long existing = id(kind, uri);
		if (existing != 0) return existing;
		long next = ++ids[kind].nextId;
		pending[kind].put(uri, next);
		return next;
	}

	private void appendMappings(Map<URI, Long>[] pending) throws IOException {
		for (int kind = 0; kind < ids.length; kind++) {
			ids[kind].append(pending[kind]);
			pending[kind].clear();
		}
	}

	@Override
	public void put(KvinTuple... tuples) {
		put(Arrays.asList(tuples));
	}

	@Override
	public synchronized void put(Iterable<KvinTuple> tuples) {
		List<GenericRecord> batch = new ArrayList<>(BATCH_SIZE);
		List<DataFile> files = new ArrayList<>();
		List<String> paths = new ArrayList<>();
		@SuppressWarnings("unchecked") Map<URI, Long>[] pending = new Map[]{new HashMap<>(), new HashMap<>(), new HashMap<>()};
		boolean committing = false;
		try (FileChannel channel = FileChannel.open(root.resolve("iceberg-ids.lock"), StandardOpenOption.CREATE, StandardOpenOption.WRITE); var lock = channel.lock()) {
			for (IdTable idTable : ids) idTable.refresh();
			for (KvinTuple tuple : tuples) {
				GenericRecord row = GenericRecord.create(SCHEMA);
				row.set(0, assign(0, tuple.item, pending));
				row.set(1, assign(1, tuple.context == null ? DEFAULT_CONTEXT : tuple.context, pending));
				row.set(2, assign(2, tuple.property, pending));
				row.set(3, tuple.time);
				row.set(4, tuple.seqNr);
				row.set(5, false);
				Object value = tuple.value;
				if (value instanceof Integer) row.set(6, value);
				else if (value instanceof Long) row.set(7, value);
				else if (value instanceof Float) row.set(8, value);
				else if (value instanceof Double) row.set(9, value);
				else if (value instanceof String) row.set(10, value);
				else if (value instanceof Boolean) row.set(11, value);
				else if (value instanceof byte[] bytes) row.set(12, ByteBuffer.wrap(bytes));
				else if (value instanceof ByteBuffer) row.set(12, value);
				else if (value instanceof Record || value instanceof URI || value instanceof BigInteger || value instanceof BigDecimal || value instanceof Short || value instanceof Object[]) {
					row.set(12, ByteBuffer.wrap(Records.encodeRecord(value)));
				} else if (value != null) {
					throw new IllegalArgumentException("Unsupported KVIN value type: " + value.getClass());
				}
				batch.add(row);
				if (batch.size() == BATCH_SIZE) {
					appendMappings(pending);
					writeBatch(batch, files, paths);
				}
			}
			if (!batch.isEmpty()) {
				appendMappings(pending);
				writeBatch(batch, files, paths);
			}
			if (!files.isEmpty()) {
				var append = table.newAppend();
				files.forEach(append::appendFile);
				committing = true;
				append.commit();
			}
		} catch (IOException e) {
			if (!committing) cleanup(paths);
			throw new UncheckedIOException("Unable to write Iceberg data", e);
		} catch (RuntimeException e) {
			if (!committing) cleanup(paths);
			throw e;
		}
	}

	private void cleanup(List<String> paths) {
		for (String path : paths) {
			try {
				Files.deleteIfExists(Path.of(path));
			} catch (IOException e) {
				log.warn("Unable to remove uncommitted Iceberg file {}", path, e);
			}
		}
	}

	private void writeBatch(List<GenericRecord> batch, List<DataFile> files, List<String> paths) throws IOException {
		batch.sort(ORDER);
		for (int i = 0; i < batch.size(); i++) {
			GenericRecord current = batch.get(i);
			boolean first = i == 0 || !current.get(0).equals(batch.get(i - 1).get(0)) || !current.get(1).equals(batch.get(i - 1).get(1)) || !current.get(2).equals(batch.get(i - 1).get(2));
			current.set(5, first);
		}
		String path = root.resolve("iceberg/data/" + UUID.randomUUID() + ".parquet").toString();
		paths.add(path);
		Files.createDirectories(Path.of(path).getParent());
		var appender = Parquet.write(table.io().newOutputFile(path)).forTable(table).writerVersion(ParquetProperties.WriterVersion.PARQUET_2_0).createWriterFunc(GenericParquetWriter::buildWriter).<org.apache.iceberg.data.Record>build();
		DataWriter<org.apache.iceberg.data.Record> writer = new DataWriter<>(appender, FileFormat.PARQUET, path, SPEC, null, null);
		try (writer) {
			batch.forEach(writer::write);
		}
		files.add(writer.toDataFile());
		batch.clear();
	}

	public synchronized void createBranch(String branchName) {
		table.refresh();
		if (table.currentSnapshot() == null) {
			throw new IllegalStateException("Cannot create a branch without a snapshot");
		}
		table.manageSnapshots().createBranch(branchName, table.currentSnapshot().snapshotId()).commit();
	}

	@Override
	public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long limit) {
		return fetchRows(List.of(item), property == null ? List.of() : List.of(property), context, null, null, limit);
	}

	@Override
	public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long end, long begin, long limit, long interval, String op) {
		return fetch(List.of(item), property == null ? List.of() : List.of(property), context, end, begin, limit, interval, op);
	}

	@Override
	public IExtendedIterator<KvinTuple> fetch(List<URI> items, List<URI> properties, URI context, long end, long begin, long limit, long interval, String op) {
		IExtendedIterator<KvinTuple> rows = fetchRows(items, properties, context, end, begin, op == null ? limit : 0);
		if (op == null) return rows;
		return new AggregatingIterator<>(rows, interval, op.trim().toLowerCase(), limit) {
			@Override
			protected KvinTuple createElement(URI item, URI property, URI context, long time, int seqNr, Object value) {
				return new KvinTuple(item, property, context, time, seqNr, value);
			}
		};
	}

	private synchronized IExtendedIterator<KvinTuple> fetchRows(List<URI> items, List<URI> properties, URI context, Long end, Long begin, long limit) {
		if (items.isEmpty()) return NiceIterator.emptyIterator();
		for (IdTable idTable : ids) idTable.table.refresh();
		Set<Long> itemIds = new HashSet<>();
		for (URI item : items) {
			long id = id(0, item);
			if (id != 0) itemIds.add(id);
		}
		long contextId = id(1, context == null ? DEFAULT_CONTEXT : context);
		if (itemIds.isEmpty() || contextId == 0) return NiceIterator.emptyIterator();
		Set<Long> propertyIds = new HashSet<>();
		for (URI property : properties) {
			long id = id(2, property);
			if (id != 0) propertyIds.add(id);
		}
		if (!properties.isEmpty() && propertyIds.isEmpty()) return NiceIterator.emptyIterator();
		Expression filter = Expressions.and(Expressions.in("itemId", itemIds), Expressions.equal("contextId", contextId));
		if (!properties.isEmpty()) filter = Expressions.and(filter, Expressions.in("propertyId", propertyIds));
		if (begin != null) filter = Expressions.and(filter, Expressions.greaterThanOrEqual("time", begin));
		if (end != null) filter = Expressions.and(filter, Expressions.lessThanOrEqual("time", end));
		table.refresh();
		PriorityQueue<RowCursor> cursors = new PriorityQueue<>(Comparator.comparing(c -> c.row, ORDER));
		try (CloseableIterable<FileScanTask> tasks = table.newScan().filter(filter).planFiles()) {
			for (FileScanTask task : tasks) {
				if (!task.deletes().isEmpty()) {
					throw new IllegalStateException("Iceberg delete files are not supported by KvinIceberg reads");
				}
				CloseableIterable<org.apache.iceberg.data.Record> rows = Parquet.read(table.io().newInputFile(task.file().location())).project(SCHEMA).filter(filter).createReaderFunc(schema -> GenericParquetReaders.buildReader(SCHEMA, schema)).build();
				RowCursor cursor = new RowCursor(rows);
				try {
					if (cursor.advance()) cursors.add(cursor);
					else cursor.close();
				} catch (RuntimeException e) {
					cursor.close();
					throw e;
				}
			}
		} catch (RuntimeException | IOException e) {
			for (RowCursor cursor : cursors) cursor.close();
			throw e instanceof IOException ? new UncheckedIOException((IOException) e) : (RuntimeException) e;
		}
		URI requestedContext = context == null ? DEFAULT_CONTEXT : context;
		return new NiceIterator<>() {
			final Map<String, Long> counts = new HashMap<>();
			String previous;
			KvinTuple next;
			boolean closed;

			@Override
			public boolean hasNext() {
				if (next != null) return true;
				try {
					while (!cursors.isEmpty()) {
						RowCursor cursor = cursors.poll();
						var row = cursor.row;
						try {
							if (cursor.advance()) cursors.add(cursor);
							else cursor.close();
						} catch (RuntimeException e) {
							cursor.close();
							throw e;
						}
						long itemId = (Long) row.get(0);
						long propertyId = (Long) row.get(2);
						long time = (Long) row.get(3);
						int seqNr = (Integer) row.get(4);
						String key = itemId + ":" + propertyId;
						String rowKey = key + ":" + time + ":" + seqNr;
						if (!itemIds.contains(itemId) || (Long) row.get(1) != contextId || (!properties.isEmpty() && !propertyIds.contains(propertyId)) || (begin != null && time < begin) || (end != null && time > end) || rowKey.equals(previous) || (limit > 0 && counts.getOrDefault(key, 0L) >= limit))
							continue;
						previous = rowKey;
						counts.merge(key, 1L, Long::sum);
						Object value = null;
						for (int i = 6; i <= 12; i++) {
							if (row.get(i) != null) {
								value = i == 12 ? Records.decodeRecord(((ByteBuffer) row.get(i)).duplicate()) : row.get(i);
								break;
							}
						}
						next = new KvinTuple(ids[0].uri(itemId), ids[2].uri(propertyId), requestedContext, time, seqNr, value);
						return true;
					}
					close();
					return false;
				} catch (IOException e) {
					close();
					throw new UncheckedIOException(e);
				} catch (RuntimeException e) {
					close();
					throw e;
				}
			}

			@Override
			public KvinTuple next() {
				if (!hasNext()) throw new NoSuchElementException();
				KvinTuple result = next;
				next = null;
				return result;
			}

			@Override
			public void close() {
				if (!closed) {
					closed = true;
					while (!cursors.isEmpty()) cursors.poll().close();
				}
			}
		};
	}

	private static class RowCursor implements AutoCloseable {
		final CloseableIterable<org.apache.iceberg.data.Record> rows;
		final CloseableIterator<org.apache.iceberg.data.Record> iterator;
		org.apache.iceberg.data.Record row;

		RowCursor(CloseableIterable<org.apache.iceberg.data.Record> rows) {
			this.rows = rows;
			this.iterator = rows.iterator();
		}

		boolean advance() {
			if (!iterator.hasNext()) return false;
			row = iterator.next();
			return true;
		}

		@Override
		public void close() {
			try {
				rows.close();
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}
	}

	@Override
	public synchronized IExtendedIterator<URI> properties(URI item, URI context) {
		Set<URI> result = new LinkedHashSet<>();
		try (IExtendedIterator<KvinTuple> rows = fetch(item, null, context, 0)) {
			while (rows.hasNext()) result.add(rows.next().property);
		}
		return WrappedIterator.create(result.iterator());
	}

	@Override
	public long delete(URI item, URI property, URI context, long end, long begin) {
		return 0;
	}

	@Override
	public boolean delete(URI item, URI context) {
		return false;
	}

	@Override
	public IExtendedIterator<URI> descendants(URI item, URI context) {
		return NiceIterator.emptyIterator();
	}

	@Override
	public IExtendedIterator<URI> descendants(URI item, URI context, long limit) {
		return NiceIterator.emptyIterator();
	}

	@Override
	public boolean addListener(KvinListener listener) {
		return false;
	}

	@Override
	public boolean removeListener(KvinListener listener) {
		return false;
	}

	@Override
	public void close() {
	}
}
