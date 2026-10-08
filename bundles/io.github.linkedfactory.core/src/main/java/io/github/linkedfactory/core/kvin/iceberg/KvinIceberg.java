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
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderBuilder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.exceptions.CommitFailedException;
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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.UUID;

/**
 * Iceberg-backed KVIN store.
 *
 * <p>Data files are sorted by item, context, property, descending time, and
 * descending sequence number. Deletes use Iceberg copy-on-write rewrites,
 * therefore deleted tuples are immediately absent from subsequently planned
 * fetch scans.</p>
 */
public class KvinIceberg implements Kvin {
	private static final Logger log = LoggerFactory.getLogger(KvinIceberg.class);

	private static final int BATCH_SIZE = 8192;
	private static final int MAX_COMMIT_RETRIES = 5;
	private static final int ID_CACHE_SIZE = 250_000;

	private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();

	private static final Schema ID_SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()), Types.NestedField.required(2, "value", Types.StringType.get()));

	private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "itemId", Types.LongType.get()), Types.NestedField.required(2, "contextId", Types.LongType.get()), Types.NestedField.required(3, "propertyId", Types.LongType.get()), Types.NestedField.required(4, "time", Types.LongType.get()), Types.NestedField.required(5, "seqNr", Types.IntegerType.get()), Types.NestedField.required(6, "first", Types.BooleanType.get()), Types.NestedField.optional(7, "valueInt", Types.IntegerType.get()), Types.NestedField.optional(8, "valueLong", Types.LongType.get()), Types.NestedField.optional(9, "valueFloat", Types.FloatType.get()), Types.NestedField.optional(10, "valueDouble", Types.DoubleType.get()), Types.NestedField.optional(11, "valueString", Types.StringType.get()), Types.NestedField.optional(12, "valueBool", Types.BooleanType.get()), Types.NestedField.optional(13, "valueObject", Types.BinaryType.get()));

	private static final SortOrder SORT_ORDER = configureSortOrder(SortOrder.builderFor(SCHEMA)).build();

	private static final Comparator<org.apache.iceberg.data.Record> ORDER = Comparator.comparingLong((org.apache.iceberg.data.Record r) -> (Long) r.get(0)).thenComparingLong(r -> (Long) r.get(1)).thenComparingLong(r -> (Long) r.get(2)).thenComparing((a, b) -> Long.compare((Long) b.get(3), (Long) a.get(3))).thenComparing((a, b) -> Integer.compare((Integer) b.get(4), (Integer) a.get(4)));

	private static final Comparator<KvinRow> ROW_ORDER = Comparator.comparingLong(KvinRow::itemId).thenComparingLong(KvinRow::contextId).thenComparingLong(KvinRow::propertyId).thenComparing((a, b) -> Long.compare(b.time(), a.time())).thenComparing((a, b) -> Integer.compare(b.seqNr(), a.seqNr()));

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

				table = tables.create(SCHEMA, SPEC, SORT_ORDER, tableProperties(), location);
			}

			enableManifestCaching(table);

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

				enableManifestCaching(ids[i].table);
			}

			if (!table.sortOrder().sameOrder(SORT_ORDER)) {
				configureSortOrder(table.replaceSortOrder()).commit();
			}
		} catch (IOException e) {
			throw new UncheckedIOException("Unable to initialize Iceberg store at " + root, e);
		}
	}

	private static <T extends SortOrderBuilder<T>> T configureSortOrder(T builder) {
		return builder.asc("itemId").asc("contextId").asc("propertyId").desc("time").desc("seqNr");
	}

	private static Map<String, String> tableProperties() {
		return Map.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name(), TableProperties.FORMAT_VERSION, "2", TableProperties.PARQUET_COMPRESSION, "zstd");
	}

	private static void enableManifestCaching(Table table) {
		Map<String, String> properties = new HashMap<>(table.io().properties());
		properties.put(CatalogProperties.IO_MANIFEST_CACHE_ENABLED, "true");
		table.io().initialize(properties);
	}

	private long id(int kind, URI uri) {
		return ids[kind].id(uri);
	}

	private final class IdTable {
		final Table table;

		final Cache<URI, Long> forward = CacheBuilder.newBuilder().maximumSize(ID_CACHE_SIZE).build();

		final Cache<Long, URI> reverse = CacheBuilder.newBuilder().maximumSize(ID_CACHE_SIZE).build();

		long nextId;
		boolean nextIdInitialized;

		IdTable(Table table) {
			this.table = table;
		}

		void refresh() {
			table.refresh();

			/*
			 * A different process may have appended ID mappings. We retain cached
			 * mappings but calculate the high-water mark again only if new IDs must
			 * be assigned during the current put operation.
			 */
			nextIdInitialized = false;
		}

		synchronized void ensureNextId() {
			if (nextIdInitialized) {
				return;
			}

			final long[] max = {0};

			scan(Expressions.alwaysTrue(), row -> max[0] = Math.max(max[0], (Long) row.get(0)));

			nextId = max[0];
			nextIdInitialized = true;
		}

		long id(URI uri) {
			Long cached = forward.getIfPresent(uri);
			if (cached != null) {
				return cached;
			}

			return resolveIds(List.of(uri), false).getOrDefault(uri, 0L);
		}

		Map<URI, Long> resolveIds(List<URI> uris) {
			return resolveIds(uris, true);
		}

		private Map<URI, Long> resolveIds(List<URI> uris, boolean refresh) {
			Map<URI, Long> result = new HashMap<>();
			Map<String, URI> missing = new HashMap<>();
			for (URI uri : uris) {
				Long cached = forward.getIfPresent(uri);
				if (cached == null) {
					missing.put(uri.toString(), uri);
				} else {
					result.put(uri, cached);
				}
			}

			if (!missing.isEmpty()) {
				if (refresh) {
					table.refresh();
				}
				scan(Expressions.in("value", missing.keySet()), row -> {
					URI uri = missing.get(row.get(1).toString());
					if (uri == null) {
						return;
					}
					long found = (Long) row.get(0);
					Long previous = result.put(uri, found);
					if (previous != null && previous != found) {
						throw new IllegalStateException("Duplicate Iceberg URI mapping for " + uri);
					}
				});
				result.forEach((uri, found) -> {
					forward.put(uri, found);
					reverse.put(found, uri);
				});
			}

			return result;
		}

		Map<Long, URI> resolveUris(Set<Long> requestedIds) {
			Map<Long, URI> result = new HashMap<>();
			Set<Long> missing = new HashSet<>();
			for (long id : requestedIds) {
				URI cached = reverse.getIfPresent(id);
				if (cached == null) {
					missing.add(id);
				} else {
					result.put(id, cached);
				}
			}

			if (!missing.isEmpty()) {
				table.refresh();
				scan(Expressions.in("id", missing), row -> {
					long id = (Long) row.get(0);
					if (!missing.contains(id)) {
						return;
					}
					URI found = URIs.createURI(row.get(1).toString());
					URI previous = result.put(id, found);
					if (previous != null && !previous.equals(found)) {
						throw new IllegalStateException("Duplicate Iceberg ID mapping for " + id);
					}
				});
				for (long id : missing) {
					if (!result.containsKey(id)) {
						throw new IllegalStateException("Unknown Iceberg URI ID: " + id);
					}
				}
				result.forEach((id, uri) -> {
					reverse.put(id, uri);
					forward.put(uri, id);
				});
			}

			return result;
		}

		void scan(Expression filter, java.util.function.Consumer<org.apache.iceberg.data.Record> consumer) {
			try (CloseableIterable<FileScanTask> tasks = table.newScan().filter(filter).planFiles()) {
				for (FileScanTask task : tasks) {
					if (!task.deletes().isEmpty()) {
						throw new IllegalStateException("Iceberg delete files are not supported by KvinIceberg ID reads");
					}

					try (CloseableIterable<org.apache.iceberg.data.Record> rows = Parquet.read(table.io().newInputFile(task.file().location())).split(task.start(), task.length()).project(ID_SCHEMA).filter(filter).createReaderFunc(schema -> GenericParquetReaders.buildReader(ID_SCHEMA, schema)).build()) {
						for (org.apache.iceberg.data.Record row : rows) {
							consumer.accept(row);
						}
					}
				}
			} catch (IOException e) {
				throw new UncheckedIOException("Unable to read Iceberg ID table", e);
			}
		}

		void append(Map<URI, Long> mappings) throws IOException {
			if (mappings.isEmpty()) {
				return;
			}

			String path = root.resolve("iceberg-ids/data/" + UUID.randomUUID() + ".parquet").toString();

			Files.createDirectories(Path.of(path).getParent());

			var appender = Parquet.write(table.io().newOutputFile(path)).forTable(table).writerVersion(ParquetProperties.WriterVersion.PARQUET_2_0).createWriterFunc(message -> GenericParquetWriter.create(ID_SCHEMA, message)).<org.apache.iceberg.data.Record>build();

			DataWriter<org.apache.iceberg.data.Record> writer = new DataWriter<>(appender, FileFormat.PARQUET, path, SPEC, null, null);

			try (writer) {
				for (Map.Entry<URI, Long> entry : mappings.entrySet()) {
					GenericRecord row = GenericRecord.create(ID_SCHEMA);
					row.set(0, entry.getValue());
					row.set(1, entry.getKey().toString());
					writer.write(row);
				}
			}

			commitIdAppend(writer.toDataFile());

			for (Map.Entry<URI, Long> entry : mappings.entrySet()) {
				forward.put(entry.getKey(), entry.getValue());
				reverse.put(entry.getValue(), entry.getKey());
				nextId = Math.max(nextId, entry.getValue());
			}

			nextIdInitialized = true;
		}

		private void commitIdAppend(DataFile file) {
			for (int attempt = 1; attempt <= MAX_COMMIT_RETRIES; attempt++) {
				try {
					table.refresh();
					table.newAppend().appendFile(file).commit();
					return;
				} catch (CommitFailedException e) {
					if (attempt == MAX_COMMIT_RETRIES) {
						throw e;
					}

					log.warn("Iceberg ID append conflicted; retrying ({}/{})", attempt, MAX_COMMIT_RETRIES);
				}
			}
		}
	}

	private long assign(int kind, URI uri, Map<URI, Long>[] pending) {
		Long staged = pending[kind].get(uri);
		if (staged != null) {
			return staged;
		}

		long existing = id(kind, uri);
		if (existing != 0) {
			return existing;
		}

		ids[kind].ensureNextId();

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

		boolean committed = false;

		try (FileChannel channel = FileChannel.open(root.resolve("iceberg-ids.lock"), StandardOpenOption.CREATE, StandardOpenOption.WRITE); var lock = channel.lock()) {

			for (IdTable idTable : ids) {
				idTable.refresh();
			}

			for (KvinTuple tuple : tuples) {
				GenericRecord row = GenericRecord.create(SCHEMA);

				row.set(0, assign(0, tuple.item, pending));
				row.set(1, assign(1, tuple.context == null ? DEFAULT_CONTEXT : tuple.context, pending));
				row.set(2, assign(2, tuple.property, pending));
				row.set(3, tuple.time);
				row.set(4, tuple.seqNr);
				row.set(5, false);

				setValue(row, tuple.value);
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
				commitAppend(files);
				committed = true;
			}
		} catch (IOException e) {
			if (!committed) {
				cleanup(paths);
			}

			throw new UncheckedIOException("Unable to write Iceberg data", e);
		} catch (RuntimeException e) {
			if (!committed) {
				cleanup(paths);
			}

			throw e;
		}
	}

	private void commitAppend(List<DataFile> files) throws IOException {
		for (int attempt = 1; attempt <= MAX_COMMIT_RETRIES; attempt++) {
			try {
				table.refresh();

				var append = table.newAppend();
				files.forEach(append::appendFile);
				append.commit();

				return;
			} catch (CommitFailedException e) {
				if (attempt == MAX_COMMIT_RETRIES) {
					throw e;
				}

				log.warn("Iceberg append conflicted; retrying ({}/{})", attempt, MAX_COMMIT_RETRIES);
			}
		}
	}

	private static void setValue(GenericRecord row, Object value) throws IOException {
		if (value instanceof Integer) {
			row.set(6, value);
		} else if (value instanceof Long) {
			row.set(7, value);
		} else if (value instanceof Float) {
			row.set(8, value);
		} else if (value instanceof Double) {
			row.set(9, value);
		} else if (value instanceof String) {
			row.set(10, value);
		} else if (value instanceof Boolean) {
			row.set(11, value);
		} else if (value instanceof byte[] bytes) {
			row.set(12, ByteBuffer.wrap(bytes));
		} else if (value instanceof ByteBuffer bytes) {
			row.set(12, bytes.duplicate());
		} else if (value instanceof Record || value instanceof URI || value instanceof BigInteger || value instanceof BigDecimal || value instanceof Short || value instanceof Object[]) {
			row.set(12, ByteBuffer.wrap(Records.encodeRecord(value)));
		} else if (value != null) {
			throw new IllegalArgumentException("Unsupported KVIN value type: " + value.getClass());
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

		var appender = Parquet.write(table.io().newOutputFile(path)).forTable(table).writerVersion(ParquetProperties.WriterVersion.PARQUET_2_0).createWriterFunc(message -> GenericParquetWriter.create(SCHEMA, message)).<org.apache.iceberg.data.Record>build();

		DataWriter<org.apache.iceberg.data.Record> writer = new DataWriter<>(appender, FileFormat.PARQUET, path, SPEC, null, null, table.sortOrder());

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

		if (op == null) {
			return rows;
		}

		return new AggregatingIterator<>(rows, interval, op.trim().toLowerCase(), limit) {
			@Override
			protected KvinTuple createElement(URI item, URI property, URI context, long time, int seqNr, Object value) {
				return new KvinTuple(item, property, context, time, seqNr, value);
			}
		};
	}

	private synchronized IExtendedIterator<KvinTuple> fetchRows(List<URI> items, List<URI> properties, URI context, Long end, Long begin, long limit) {
		if (items.isEmpty()) {
			return NiceIterator.emptyIterator();
		}

		Map<Long, URI> itemUris = new HashMap<>();
		ids[0].resolveIds(items).forEach((uri, id) -> itemUris.put(id, uri));
		Set<Long> itemIds = itemUris.keySet();

		URI requestedContext = context == null ? DEFAULT_CONTEXT : context;
		long contextId = ids[1].resolveIds(List.of(requestedContext)).getOrDefault(requestedContext, 0L);

		if (itemIds.isEmpty() || contextId == 0) {
			return NiceIterator.emptyIterator();
		}

		Map<Long, URI> requestedPropertyUris = new HashMap<>();
		ids[2].resolveIds(properties).forEach((uri, id) -> requestedPropertyUris.put(id, uri));
		Set<Long> propertyIds = requestedPropertyUris.keySet();

		if (!properties.isEmpty() && propertyIds.isEmpty()) {
			return NiceIterator.emptyIterator();
		}

		Expression filter = Expressions.and(Expressions.in("itemId", itemIds), Expressions.equal("contextId", contextId));

		if (!properties.isEmpty()) {
			filter = Expressions.and(filter, Expressions.in("propertyId", propertyIds));
		}

		if (begin != null) {
			filter = Expressions.and(filter, Expressions.greaterThanOrEqual("time", begin));
		}

		if (end != null) {
			filter = Expressions.and(filter, Expressions.lessThanOrEqual("time", end));
		}

		table.refresh();

		PriorityQueue<RowCursor> cursors = new PriorityQueue<>(Comparator.comparing(cursor -> cursor.row, ROW_ORDER));

		try (CloseableIterable<FileScanTask> tasks = table.newScan().filter(filter).planFiles()) {
			for (FileScanTask task : tasks) {
				if (!task.deletes().isEmpty()) {
					throw new IllegalStateException("Iceberg delete files are not supported by KvinIceberg reads");
				}

				RowCursor cursor = new RowCursor(table, task, filter);

				try {
					if (cursor.advance()) {
						cursors.add(cursor);
					} else {
						cursor.close();
					}
				} catch (RuntimeException e) {
					cursor.close();
					throw e;
				}
			}
		} catch (RuntimeException | IOException e) {
			closeCursors(cursors);

			if (e instanceof IOException ioException) {
				throw new UncheckedIOException(ioException);
			}

			throw (RuntimeException) e;
		}

		long requestedSeries = properties.isEmpty() ? 0 : (long) itemIds.size() * propertyIds.size();

		return new NiceIterator<>() {
			KvinRow previous;
			long seriesCount;
			long completedSeries;
			long seriesItemId;
			long seriesPropertyId;
			URI seriesItem;
			URI seriesProperty;
			// Advance only when another tuple is requested, so a satisfied limit reads no extra row.
			RowCursor pending;
			KvinTuple next;
			boolean closed;

			@Override
			public boolean hasNext() {
				if (next != null) {
					return true;
				}
				if (closed) {
					return false;
				}

				try {
					while (pending != null || !cursors.isEmpty()) {
						if (pending != null) {
							if (pending.advance()) {
								cursors.add(pending);
							} else {
								pending.close();
							}
							pending = null;
						}
						if (cursors.isEmpty()) {
							break;
						}
						pending = cursors.poll();
						KvinRow row = pending.row;

						long itemId = row.itemId();
						long propertyId = row.propertyId();
						long time = row.time();
						int seqNr = row.seqNr();

						if (!itemIds.contains(itemId) || row.contextId() != contextId || (!properties.isEmpty() && !propertyIds.contains(propertyId)) || (begin != null && time < begin) || (end != null && time > end)) {
							continue;
						}

						if (previous != null && ROW_ORDER.compare(previous, row) == 0) {
							continue;
						}
						if (seriesItem == null || seriesItemId != itemId || seriesPropertyId != propertyId) {
							seriesItemId = itemId;
							seriesPropertyId = propertyId;
							seriesCount = 0;
							seriesItem = itemUris.get(itemId);
							seriesProperty = requestedPropertyUris.get(propertyId);
							if (seriesProperty == null) {
								seriesProperty = ids[2].reverse.getIfPresent(propertyId);
							}
							if (seriesProperty == null) {
								Set<Long> headProperties = new HashSet<>();
								headProperties.add(propertyId);
								for (RowCursor cursor : cursors) {
									KvinRow head = cursor.row;
									if (itemIds.contains(head.itemId()) && head.contextId() == contextId
											&& (begin == null || head.time() >= begin) && (end == null || head.time() <= end)) {
										headProperties.add(head.propertyId());
									}
								}
								seriesProperty = ids[2].resolveUris(headProperties).get(propertyId);
							}
						}
						if (limit > 0 && seriesCount >= limit) {
							continue;
						}

						previous = row;
						if (limit > 0 && ++seriesCount == limit) {
							completedSeries++;
						}

						Object value = row.value();

						if (value instanceof ByteBuffer bytes) {
							value = Records.decodeRecord(bytes.duplicate());
						}

						next = new KvinTuple(seriesItem, seriesProperty, requestedContext, time, seqNr, value);
						if (limit > 0 && requestedSeries > 0 && completedSeries == requestedSeries) {
							close();
						}

						return true;
					}

					close();
					return false;
				} catch (IOException | RuntimeException e) {
					next = null;
					try {
						close();
					} catch (RuntimeException closeFailure) {
						if (e != closeFailure) {
							e.addSuppressed(closeFailure);
						}
					}
					if (e instanceof IOException ioException) {
						throw new UncheckedIOException(ioException);
					}
					throw (RuntimeException) e;
				}
			}

			@Override
			public KvinTuple next() {
				if (!hasNext()) {
					throw new NoSuchElementException();
				}

				KvinTuple result = next;
				next = null;
				return result;
			}

			@Override
			public void close() {
				if (!closed) {
					closed = true;
					if (pending != null) {
						cursors.add(pending);
						pending = null;
					}
					closeCursors(cursors);
				}
			}
		};
	}

	private static void closeCursors(PriorityQueue<RowCursor> cursors) {
		RuntimeException failure = null;

		while (!cursors.isEmpty()) {
			try {
				cursors.poll().close();
			} catch (RuntimeException e) {
				if (failure == null) {
					failure = e;
				} else if (failure != e) {
					failure.addSuppressed(e);
				}
			}
		}

		if (failure != null) {
			throw failure;
		}
	}

	@Override
	public synchronized long delete(URI item, URI property, URI context, long end, long begin) {
		if (property == null) {
			return 0;
		}

		long itemId = id(0, item);
		long propertyId = id(2, property);
		long contextId = id(1, context == null ? DEFAULT_CONTEXT : context);

		if (itemId == 0 || propertyId == 0 || contextId == 0) {
			return 0;
		}

		long lower = Math.min(begin, end);
		long upper = Math.max(begin, end);

		Expression filter = Expressions.and(Expressions.and(Expressions.equal("itemId", itemId), Expressions.equal("contextId", contextId)), Expressions.and(Expressions.equal("propertyId", propertyId), Expressions.and(Expressions.greaterThanOrEqual("time", lower), Expressions.lessThanOrEqual("time", upper))));

		table.refresh();

		Map<String, DataFile> affectedFiles = new LinkedHashMap<>();

		try (CloseableIterable<FileScanTask> tasks = table.newScan().filter(filter).planFiles()) {
			for (FileScanTask task : tasks) {
				if (!task.deletes().isEmpty()) {
					throw new IllegalStateException("Iceberg delete files are not supported by KvinIceberg deletes");
				}

				/*
				 * A file can occur in multiple split tasks. A copy-on-write rewrite
				 * must rewrite each complete data file exactly once.
				 */
				affectedFiles.putIfAbsent(task.file().location(), task.file());
			}
		} catch (IOException e) {
			throw new UncheckedIOException("Unable to plan Iceberg delete", e);
		}

		if (affectedFiles.isEmpty()) {
			return 0;
		}

		Set<DataFile> deleteFiles = new LinkedHashSet<>();
		Set<DataFile> addFiles = new LinkedHashSet<>();
		List<String> temporaryPaths = new ArrayList<>();
		long deleted = 0;
		boolean committed = false;

		try {
			for (DataFile file : affectedFiles.values()) {
				List<GenericRecord> retained = new ArrayList<>();
				long removedFromFile = 0;

				try (CloseableIterable<org.apache.iceberg.data.Record> rows = Parquet.read(table.io().newInputFile(file.location())).project(SCHEMA).createReaderFunc(schema -> GenericParquetReaders.buildReader(SCHEMA, schema)).build()) {
					for (org.apache.iceberg.data.Record record : rows) {
						KvinRow row = toKvinRow(record);

						if (matchesDelete(row, itemId, propertyId, contextId, lower, upper)) {
							removedFromFile++;
						} else {
							retained.add(toGenericRecord(row));
						}
					}
				}

				if (removedFromFile == 0) {
					continue;
				}

				deleted += removedFromFile;
				deleteFiles.add(file);

				if (!retained.isEmpty()) {
					List<DataFile> writtenFiles = new ArrayList<>();
					writeBatch(retained, writtenFiles, temporaryPaths);
					addFiles.addAll(writtenFiles);
				}
			}

			if (deleteFiles.isEmpty()) {
				return 0;
			}

			commitRewrite(deleteFiles, addFiles);
			committed = true;

			return deleted;
		} catch (IOException e) {
			cleanup(temporaryPaths);
			throw new UncheckedIOException("Unable to delete Iceberg tuples", e);
		} catch (RuntimeException e) {
			cleanup(temporaryPaths);
			throw e;
		}
	}

	private void commitRewrite(Set<DataFile> deleteFiles, Set<DataFile> addFiles) {
		for (int attempt = 1; attempt <= MAX_COMMIT_RETRIES; attempt++) {
			try {
				table.refresh();

				table.newRewrite().rewriteFiles(deleteFiles, addFiles).commit();

				return;
			} catch (CommitFailedException e) {
				if (attempt == MAX_COMMIT_RETRIES) {
					throw e;
				}

				log.warn("Iceberg rewrite conflicted; retrying ({}/{})", attempt, MAX_COMMIT_RETRIES);
			}
		}
	}

	private static boolean matchesDelete(KvinRow row, long itemId, long propertyId, long contextId, long begin, long end) {
		return row.itemId() == itemId && row.propertyId() == propertyId && row.contextId() == contextId && row.time() >= begin && row.time() <= end;
	}

	private static GenericRecord toGenericRecord(KvinRow row) throws IOException {
		GenericRecord record = GenericRecord.create(SCHEMA);

		record.set(0, row.itemId());
		record.set(1, row.contextId());
		record.set(2, row.propertyId());
		record.set(3, row.time());
		record.set(4, row.seqNr());
		record.set(5, false);

		setValue(record, row.value());

		return record;
	}

	private static KvinRow toKvinRow(org.apache.iceberg.data.Record record) {
		Object value = null;

		for (int i = 6; i < SCHEMA.columns().size(); i++) {
			value = record.get(i);

			if (value != null) {
				break;
			}
		}

		return new KvinRow((Long) record.get(0), (Long) record.get(1), (Long) record.get(2), (Long) record.get(3), (Integer) record.get(4), value);
	}

	private record KvinRow(long itemId, long contextId, long propertyId, long time, int seqNr, Object value) {
	}

	private static final class RowCursor implements AutoCloseable {
		final CloseableIterable<org.apache.iceberg.data.Record> rows;
		final CloseableIterator<org.apache.iceberg.data.Record> iterator;
		KvinRow row;

		RowCursor(Table table, FileScanTask task, Expression filter) {
			if (!task.deletes().isEmpty()) {
				throw new IllegalStateException("Iceberg delete files are not supported by KvinIceberg reads");
			}

			this.rows = Parquet.read(table.io().newInputFile(task.file().location())).split(task.start(), task.length()).project(SCHEMA).filter(filter).createReaderFunc(schema -> GenericParquetReaders.buildReader(SCHEMA, schema)).build();

			this.iterator = rows.iterator();
		}

		boolean advance() {
			if (!iterator.hasNext()) {
				return false;
			}

			row = toKvinRow(iterator.next());
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
			while (rows.hasNext()) {
				result.add(rows.next().property);
			}
		}

		return WrappedIterator.create(result.iterator());
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
		ManifestFiles.dropCache(table.io());

		for (IdTable idTable : ids) {
			ManifestFiles.dropCache(idTable.table.io());
		}
	}
}