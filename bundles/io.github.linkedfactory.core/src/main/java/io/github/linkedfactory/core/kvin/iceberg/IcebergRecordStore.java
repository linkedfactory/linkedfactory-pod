package io.github.linkedfactory.core.kvin.iceberg;

import io.github.linkedfactory.core.kvin.records.KvinRecordStore;
import io.github.linkedfactory.core.kvin.records.KvinRecord;
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
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableGroup;
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
import java.nio.file.Files;
import java.nio.file.Path;
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
import java.util.TreeSet;
import java.util.UUID;

/**
 * Standalone Iceberg-backed numeric record store, with no URI-ID tables.
 * Null contexts select ID 0; null properties (and the interface's -1 sentinel)
 * select all properties.
 *
 * <p>Data files are sorted by item, context, property, descending time, and
 * descending sequence number. Deletes use Iceberg copy-on-write rewrites,
 * therefore deleted tuples are immediately absent from subsequently planned
 * fetch scans.</p>
 *
 * <p>After a successful append, automatic sorted compaction rewrites small files
 * when {@code kvin.compaction.min-files} (default 32, minimum 2) is reached.
 * Files smaller than half {@code write.target-file-size-bytes} (default 128 MiB)
 * are eligible; merged output rolls at the target size. Existing snapshots and
 * readers retain their original files. Compaction failures are logged and thrown
 * to the caller, but the preceding append remains committed.</p>
 */
public class IcebergRecordStore implements KvinRecordStore {
	private static final Logger log = LoggerFactory.getLogger(IcebergRecordStore.class);

	private static final int BATCH_SIZE = 8192;
	private static final int MAX_COMMIT_RETRIES = 5;
	private static final String COMPACTION_MIN_FILES = "kvin.compaction.min-files";
	private static final int DEFAULT_COMPACTION_MIN_FILES = 32;
	private static final long DEFAULT_COMPACTION_TARGET_SIZE = 128L * 1024 * 1024;
	private static final int COMPACTION_MAX_INPUT_FILES = 128;

	private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();

	private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "itemId", Types.LongType.get()), Types.NestedField.required(2, "contextId", Types.LongType.get()), Types.NestedField.required(3, "propertyId", Types.LongType.get()), Types.NestedField.required(4, "time", Types.LongType.get()), Types.NestedField.required(5, "seqNr", Types.IntegerType.get()), Types.NestedField.required(6, "first", Types.BooleanType.get()), Types.NestedField.optional(7, "valueInt", Types.IntegerType.get()), Types.NestedField.optional(8, "valueLong", Types.LongType.get()), Types.NestedField.optional(9, "valueFloat", Types.FloatType.get()), Types.NestedField.optional(10, "valueDouble", Types.DoubleType.get()), Types.NestedField.optional(11, "valueString", Types.StringType.get()), Types.NestedField.optional(12, "valueBool", Types.BooleanType.get()), Types.NestedField.optional(13, "valueObject", Types.BinaryType.get()));

	private static final SortOrder SORT_ORDER = configureSortOrder(SortOrder.builderFor(SCHEMA)).build();

	private static final Comparator<org.apache.iceberg.data.Record> ORDER = Comparator.comparingLong((org.apache.iceberg.data.Record r) -> (Long) r.get(0)).thenComparingLong(r -> (Long) r.get(1)).thenComparingLong(r -> (Long) r.get(2)).thenComparing((a, b) -> Long.compare((Long) b.get(3), (Long) a.get(3))).thenComparing((a, b) -> Integer.compare((Integer) b.get(4), (Integer) a.get(4)));

	private static final Comparator<KvinRow> ROW_ORDER = Comparator.comparingLong(KvinRow::itemId).thenComparingLong(KvinRow::contextId).thenComparingLong(KvinRow::propertyId).thenComparing((a, b) -> Long.compare(b.time(), a.time())).thenComparing((a, b) -> Integer.compare(b.seqNr(), a.seqNr()));

	private final Path root;
	private final Table table;

	public IcebergRecordStore(String archiveLocation) {
		this(archiveLocation, null);
	}

	public IcebergRecordStore(String archiveLocation, Duration retentionPeriod) {
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

	boolean hasSnapshot() {
		table.refresh();
		return table.currentSnapshot() != null;
	}

	private static Map<String, String> tableProperties() {
		return Map.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name(), TableProperties.FORMAT_VERSION, "2", TableProperties.PARQUET_COMPRESSION, "zstd");
	}

	private static void enableManifestCaching(Table table) {
		Map<String, String> properties = new HashMap<>(table.io().properties());
		properties.put(CatalogProperties.IO_MANIFEST_CACHE_ENABLED, "true");
		table.io().initialize(properties);
	}

	@Override
	public void put(KvinRecord... records) {
		put(Arrays.asList(records));
	}

	@Override
	public synchronized void put(Iterable<KvinRecord> records) {
		List<GenericRecord> batch = new ArrayList<>(BATCH_SIZE);
		List<DataFile> files = new ArrayList<>();
		List<String> paths = new ArrayList<>();

		boolean committed = false;

		try {
			for (KvinRecord record : records) {
				GenericRecord row = GenericRecord.create(SCHEMA);

				row.set(0, record.itemId());
				row.set(1, record.contextId());
				row.set(2, record.propertyId());
				row.set(3, record.time());
				row.set(4, record.seqNr());
				row.set(5, false);

				setValue(row, record.value());
				batch.add(row);

				if (batch.size() == BATCH_SIZE) {
					writeBatch(batch, files, paths);
				}
			}

			if (!batch.isEmpty()) {
				writeBatch(batch, files, paths);
			}

			if (!files.isEmpty()) {
				commitAppend(files);
				committed = true;
				compactSmallFiles();
			}
		} catch (CommitStateUnknownException e) {
			// A possibly committed file must never be removed.
			throw e;
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

	private void compactSmallFiles() throws IOException {
		List<String> paths = new ArrayList<>();
		try {
			table.refresh();
			int minimum = Integer.parseInt(table.properties().getOrDefault(COMPACTION_MIN_FILES, Integer.toString(DEFAULT_COMPACTION_MIN_FILES)));
			long targetSize = Long.parseLong(table.properties().getOrDefault(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, Long.toString(DEFAULT_COMPACTION_TARGET_SIZE)));
			if (minimum < 2 || targetSize <= 0) {
				throw new IllegalArgumentException("Iceberg compaction requires kvin.compaction.min-files >= 2 and a positive write.target-file-size-bytes");
			}
			if (table.currentSnapshot() == null || !table.sortOrder().sameOrder(SORT_ORDER)) {
				return;
			}

			long snapshotId = table.currentSnapshot().snapshotId();
			Map<String, DataFile> candidates = new LinkedHashMap<>();
			int maximum = Math.max(minimum, COMPACTION_MAX_INPUT_FILES);
			try (CloseableIterable<FileScanTask> tasks = table.newScan().useSnapshot(snapshotId).planFiles()) {
				for (FileScanTask task : tasks) {
					if (!task.deletes().isEmpty()) {
						throw new IllegalStateException("Iceberg delete files are not supported by KvinIceberg compaction");
					}
					DataFile file = task.file();
					SortOrder order = table.sortOrders().get(file.sortOrderId());
					if (candidates.size() < maximum && file.format() == FileFormat.PARQUET
							&& file.specId() == table.spec().specId() && order != null && order.sameOrder(SORT_ORDER)
							&& file.fileSizeInBytes() < targetSize / 2) {
						candidates.putIfAbsent(file.location(), file);
					}
				}
			}
			if (candidates.size() < minimum) {
				return;
			}

			Set<DataFile> replacements = mergeCompactionFiles(candidates.values(), targetSize, paths);
			if (replacements.size() >= candidates.size()) {
				cleanup(paths);
				return;
			}
			// Validate against exactly the snapshot read: concurrent appends are safe,
			// but replaced inputs and newly added row-level deletes must conflict.
			table.newRewrite().validateFromSnapshot(snapshotId)
					.rewriteFiles(new LinkedHashSet<>(candidates.values()), replacements).commit();
		} catch (CommitStateUnknownException e) {
			log.error("Iceberg compaction commit state is unknown; appended tuples remain committed and output files are retained", e);
			throw e;
		} catch (IOException | RuntimeException e) {
			cleanup(paths);
			log.error("Iceberg compaction failed after tuples were appended; the append remains committed", e);
			throw e;
		}
	}

	private Set<DataFile> mergeCompactionFiles(Iterable<DataFile> files, long targetSize, List<String> paths) throws IOException {
		Set<DataFile> replacements = new LinkedHashSet<>();
		PriorityQueue<CompactionCursor> queue = new PriorityQueue<>((a, b) -> ORDER.compare(a.row, b.row));
		try (CloseableGroup readers = new CloseableGroup()) {
			for (DataFile file : files) {
				CloseableIterable<org.apache.iceberg.data.Record> rows = Parquet.read(table.io().newInputFile(file.location()))
						.project(SCHEMA).createReaderFunc(schema -> GenericParquetReaders.buildReader(SCHEMA, schema)).build();
				readers.addCloseable(rows);
				CompactionCursor cursor = new CompactionCursor(rows.iterator());
				if (cursor.advance()) {
					queue.add(cursor);
				}
			}
			while (!queue.isEmpty()) {
				DataWriter<org.apache.iceberg.data.Record> writer = newCompactionWriter(targetSize, paths);
				long item = 0, context = 0, property = 0;
				boolean first = true;
				try (writer) {
					do {
						CompactionCursor cursor = queue.remove();
						org.apache.iceberg.data.Record row = cursor.row;
						long nextItem = (Long) row.get(0), nextContext = (Long) row.get(1), nextProperty = (Long) row.get(2);
						row.set(5, first || nextItem != item || nextContext != context || nextProperty != property);
						writer.write(row);
						item = nextItem;
						context = nextContext;
						property = nextProperty;
						first = false;
						if (cursor.advance()) {
							queue.add(cursor);
						}
					} while (!queue.isEmpty() && writer.length() < targetSize);
				}
				replacements.add(writer.toDataFile());
			}
		}
		return replacements;
	}

	private DataWriter<org.apache.iceberg.data.Record> newCompactionWriter(long targetSize, List<String> paths) throws IOException {
		String path = root.resolve("iceberg/data/" + UUID.randomUUID() + ".parquet").toString();
		paths.add(path);
		Files.createDirectories(Path.of(path).getParent());
		var appender = Parquet.write(table.io().newOutputFile(path)).forTable(table)
				.set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Long.toString(Math.min(targetSize, 8L * 1024 * 1024)))
				.writerVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
				.createWriterFunc(message -> GenericParquetWriter.create(SCHEMA, message)).<org.apache.iceberg.data.Record>build();
		return new DataWriter<>(appender, FileFormat.PARQUET, path, SPEC, null, null, table.sortOrder());
	}

	private static final class CompactionCursor {
		private final CloseableIterator<org.apache.iceberg.data.Record> iterator;
		private org.apache.iceberg.data.Record row;

		private CompactionCursor(CloseableIterator<org.apache.iceberg.data.Record> iterator) {
			this.iterator = iterator;
		}

		private boolean advance() {
			if (!iterator.hasNext()) {
				return false;
			}
			row = iterator.next();
			return true;
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
	public IExtendedIterator<KvinRecord> fetch(long itemId, Long propertyId, Long contextId, long limit) {
		return fetchRows(List.of(itemId), properties(propertyId), contextId, null, null, limit);
	}

	private static List<Long> properties(Long propertyId) {
		return propertyId == null || propertyId == -1 ? List.of() : List.of(propertyId);
	}

	@Override
	public IExtendedIterator<KvinRecord> fetch(long itemId, Long propertyId, Long contextId, long end,
			long begin, long limit, long interval, String op) {
		return fetch(List.of(itemId), properties(propertyId), contextId, end, begin, limit, interval, op);
	}

	@Override
	public IExtendedIterator<KvinRecord> fetch(List<Long> items, List<Long> properties, Long context,
			long end, long begin, long limit, long interval, String op) {
		IExtendedIterator<KvinRecord> rows = fetchRows(items,
				properties.contains(-1L) ? List.of() : properties, context, end, begin, op == null ? limit : 0);
		if (op == null) {
			return rows;
		}
		// Numeric URI keys adapt the shared aggregation engine without resolving URI IDs.
		IExtendedIterator<KvinTuple> tuples = rows.mapWith(row -> new KvinTuple(key(row.itemId()),
				key(row.propertyId()), key(row.contextId()), row.time(), row.seqNr(), row.value()));
		return new AggregatingIterator<>(tuples, interval, op.trim().toLowerCase(java.util.Locale.ROOT), limit) {
			@Override
			protected KvinTuple createElement(URI item, URI property, URI context, long time, int seqNr, Object value) {
				return new KvinTuple(item, property, context, time, seqNr, value);
			}
		}.mapWith(tuple -> new KvinRecord(number(tuple.item), number(tuple.context), number(tuple.property),
				tuple.time, tuple.seqNr, tuple.value));
	}

	private static URI key(long id) {
		return URIs.createURI("urn:kvin:id:" + id);
	}

	private static long number(URI key) {
		return Long.parseLong(key.toString().substring("urn:kvin:id:".length()));
	}

	abstract static class RecordIterator extends NiceIterator<KvinRecord> {
		abstract Set<Long> headProperties();
	}

	private synchronized IExtendedIterator<KvinRecord> fetchRows(List<Long> items, List<Long> properties, Long context, Long end, Long begin, long limit) {
		if (items.isEmpty()) {
			return NiceIterator.emptyIterator();
		}

		Set<Long> itemIds = new HashSet<>(items);
		Set<Long> propertyIds = new HashSet<>(properties);
		long contextId = context == null ? DEFAULT_CONTEXT : context;

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

				RowCursor cursor = new RowCursor(table, task, filter, limit > 0);

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

		return new RecordIterator() {
			KvinRow previous;
			long seriesCount;
			long completedSeries;
			long seriesItemId;
			long seriesPropertyId;
			boolean seriesStarted;
			// Advance only when another tuple is requested, so a satisfied limit reads no extra row.
			RowCursor pending;
			KvinRecord next;
			boolean closed;

			@Override
			Set<Long> headProperties() {
				Set<Long> result = new HashSet<>();
				for (RowCursor cursor : cursors) {
					KvinRow head = cursor.row;
					if (itemIds.contains(head.itemId()) && head.contextId() == contextId
							&& (begin == null || head.time() >= begin) && (end == null || head.time() <= end)) {
						result.add(head.propertyId());
					}
				}
				return result;
			}

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
						if (!seriesStarted || seriesItemId != itemId || seriesPropertyId != propertyId) {
							seriesItemId = itemId;
							seriesPropertyId = propertyId;
							seriesCount = 0;
							seriesStarted = true;
						}
						if (limit > 0 && seriesCount >= limit) {
							continue;
						}

						previous = row;
						if (limit > 0 && ++seriesCount == limit) {
							completedSeries++;
							pending.skipThrough(itemId, contextId, propertyId);
							for (RowCursor cursor : cursors) {
								cursor.skipThrough(itemId, contextId, propertyId);
							}
						}

						Object value = row.value();

						if (value instanceof ByteBuffer bytes) {
							value = Records.decodeRecord(bytes.duplicate());
						}

						next = new KvinRecord(itemId, contextId, propertyId, time, seqNr, value);
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
			public KvinRecord next() {
				if (!hasNext()) {
					throw new NoSuchElementException();
				}

				KvinRecord result = next;
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
	public synchronized long delete(long itemId, Long propertyId, Long context, long end, long begin) {
		long contextId = context == null ? DEFAULT_CONTEXT : context;
		long lower = Math.min(begin, end);
		long upper = Math.max(begin, end);

		Expression filter = Expressions.and(Expressions.and(Expressions.equal("itemId", itemId), Expressions.equal("contextId", contextId)), Expressions.and((propertyId == null || propertyId == -1 ? Expressions.alwaysTrue() : Expressions.equal("propertyId", propertyId)), Expressions.and(Expressions.greaterThanOrEqual("time", lower), Expressions.lessThanOrEqual("time", upper))));

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

	private static boolean matchesDelete(KvinRow row, long itemId, Long propertyId, long contextId, long begin, long end) {
		return row.itemId() == itemId && (propertyId == null || propertyId == -1 || row.propertyId() == propertyId) && row.contextId() == contextId && row.time() >= begin && row.time() <= end;
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

		RowCursor(Table table, FileScanTask task, Expression filter, boolean limited) {
			if (!task.deletes().isEmpty()) {
				throw new IllegalStateException("Iceberg delete files are not supported by KvinIceberg reads");
			}

			try {
				this.rows = limited ? new SeriesSkippingReader(SCHEMA, task, filter)
						: Parquet.read(table.io().newInputFile(task.file().location())).split(task.start(), task.length()).project(SCHEMA).filter(filter).createReaderFunc(schema -> GenericParquetReaders.buildReader(SCHEMA, schema)).build();
			} catch (IOException e) {
				throw new UncheckedIOException("Unable to open Iceberg series reader", e);
			}

			this.iterator = rows.iterator();
		}

		void skipThrough(long item, long context, long property) {
			if (rows instanceof SeriesSkippingReader reader) {
				reader.skipThrough(item, context, property);
			}
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
	public synchronized IExtendedIterator<Long> properties(long itemId, Long context) {
		long contextId = context == null ? DEFAULT_CONTEXT : context;
		Expression filter = Expressions.and(Expressions.equal("itemId", itemId),
				Expressions.and(Expressions.equal("contextId", contextId), Expressions.equal("first", true)));
		Schema projection = SCHEMA.select("itemId", "contextId", "propertyId", "first");
		Set<Long> propertyIds = new TreeSet<>();
		table.refresh();
		try (CloseableIterable<FileScanTask> tasks = table.newScan().filter(filter).planFiles()) {
			for (FileScanTask task : tasks) {
				if (!task.deletes().isEmpty()) {
					throw new IllegalStateException("Iceberg delete files are not supported by KvinIceberg reads");
				}
				try (CloseableIterable<org.apache.iceberg.data.Record> rows = Parquet.read(
						table.io().newInputFile(task.file().location())).split(task.start(), task.length())
						.project(projection).filter(filter)
						.createReaderFunc(schema -> GenericParquetReaders.buildReader(projection, schema)).build()) {
					for (org.apache.iceberg.data.Record row : rows) {
						if ((Long) row.get(0) == itemId && (Long) row.get(1) == contextId && (Boolean) row.get(3)) {
							propertyIds.add((Long) row.get(2));
						}
					}
				}
			}
		} catch (IOException e) {
			throw new UncheckedIOException("Unable to read Iceberg properties", e);
		}
		return WrappedIterator.create(propertyIds.iterator());
	}

	@Override
	public boolean delete(long itemId, Long contextId) {
		return delete(itemId, null, contextId, Long.MAX_VALUE, Long.MIN_VALUE) > 0;
	}

	@Override
	public void close() {
		ManifestFiles.dropCache(table.io());
	}
}
