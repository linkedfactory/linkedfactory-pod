package io.github.linkedfactory.core.kvin.iceberg;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.github.linkedfactory.core.kvin.records.KvinRecordStore;
import io.github.linkedfactory.core.kvin.records.KvinRecord;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.Record;
import io.github.linkedfactory.core.kvin.records.Records;
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
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderBuilder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Standalone Iceberg-backed numeric record store, with no URI-ID tables.
 * Null contexts select ID 0; null properties (and the interface's -1 sentinel)
 * select all properties.
 *
 * <p>Data files are sorted by context, item, property, descending time, and
 * descending sequence number. Deletes use Iceberg copy-on-write rewrites,
 * therefore deleted tuples are immediately absent from subsequently planned
 * fetch scans.</p>
 *
 * <p>After a successful append, tiered compaction runs in a background thread, one
 * compaction at a time per store. A file's tier is {@code floor(log_F(recordCount))},
 * where F is {@code kvin.compaction.min-files} (default 8, minimum 2). When a partition
 * has F files of one tier, they are merge-sorted into files of a higher tier, so each
 * record is rewritten at most once per tier. Files smaller than half
 * {@code write.target-file-size-bytes} (default 128 MiB) are eligible; merged output
 * rolls at the target size. Existing snapshots and readers retain their original
 * files. Background compaction failures are logged; {@link #compact()} runs a
 * compaction and throws its failures. The preceding append always remains committed.</p>
 */
public class IcebergRecordStore implements KvinRecordStore {
	private static final Logger log = LoggerFactory.getLogger(IcebergRecordStore.class);

	// Records sorted in memory and written as one data file per partition. Larger
	// batches give fewer, non-overlapping files for bulk writes but need more memory.
	static final String WRITE_BATCH_RECORDS = "kvin.write.batch-records";
	static final int DEFAULT_WRITE_BATCH_RECORDS = 131_072;
	private static final int MAX_COMMIT_RETRIES = 5;
	private static final String COMPACTION_MIN_FILES = "kvin.compaction.min-files";
	private static final int DEFAULT_COMPACTION_MIN_FILES = 8;
	private static final long DEFAULT_COMPACTION_TARGET_SIZE = 128L * 1024 * 1024;
	private static final int COMPACTION_MAX_INPUT_FILES = 128;
	private static final int FOOTER_CACHE_SIZE = 4096;
	// Defaults for data files unless the table sets them. Iceberg's 128 MB row groups
	// and 1 MB pages would make a point fetch decode large parts of a file.
	static final long DEFAULT_ROW_GROUP_SIZE = 8L * 1024 * 1024;
	static final long DEFAULT_PAGE_SIZE = 8L * 1024;
	// Column statistics used to prune, order and lazily open data files during fetches.
	private static final List<String> FETCH_STATS_COLUMNS = List.of("itemId", "contextId", "propertyId", "time");
	private static final int CONTEXT_FIELD_ID = 2;
	private static final int TIME_FIELD_ID = 4;

	private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "itemId", Types.LongType.get()), Types.NestedField.required(2, "contextId", Types.LongType.get()), Types.NestedField.required(3, "propertyId", Types.LongType.get()), Types.NestedField.required(4, "time", Types.LongType.get()), Types.NestedField.required(5, "seqNr", Types.IntegerType.get()), Types.NestedField.required(6, "first", Types.BooleanType.get()), Types.NestedField.optional(7, "valueInt", Types.IntegerType.get()), Types.NestedField.optional(8, "valueLong", Types.LongType.get()), Types.NestedField.optional(9, "valueFloat", Types.FloatType.get()), Types.NestedField.optional(10, "valueDouble", Types.DoubleType.get()), Types.NestedField.optional(11, "valueString", Types.StringType.get()), Types.NestedField.optional(12, "valueBool", Types.BooleanType.get()), Types.NestedField.optional(13, "valueObject", Types.BinaryType.get()));

	private static final SortOrder SORT_ORDER = configureSortOrder(SortOrder.builderFor(SCHEMA)).build();

	private static final Comparator<org.apache.iceberg.data.Record> ORDER = Comparator.comparingLong((org.apache.iceberg.data.Record r) -> (Long) r.get(1)).thenComparingLong(r -> (Long) r.get(0)).thenComparingLong(r -> (Long) r.get(2)).thenComparing((a, b) -> Long.compare((Long) b.get(3), (Long) a.get(3))).thenComparing((a, b) -> Integer.compare((Integer) b.get(4), (Integer) a.get(4)));

	private static final Comparator<KvinRow> ROW_ORDER = Comparator.comparingLong(KvinRow::contextId).thenComparingLong(KvinRow::itemId).thenComparingLong(KvinRow::propertyId).thenComparing((a, b) -> Long.compare(b.time(), a.time())).thenComparing((a, b) -> Integer.compare(b.seqNr(), a.seqNr()));

	private final ArchiveLocation root;
	// opens data files for fetches with the Hadoop configuration of this store
	private final ParquetInputFiles inputFiles;
	private final Table table;
	// A separate table instance, so that compaction neither shares state with nor blocks writes and reads.
	private final Table compactionTable;
	// Runs one compaction at a time; requests while one is queued are coalesced.
	private final ExecutorService compactor;
	private final AtomicBoolean compactionScheduled = new AtomicBoolean();
	/*
	 * Serializes the commits of both table instances. Each commit refreshes its table while
	 * holding the lock, so commits of this store never conflict with each other and need no
	 * retries with backoff. Merging and writing files happens outside of the lock.
	 */
	private final Object commitLock = new Object();
	private volatile boolean closed;
	private final Duration retentionPeriod;
	// Data files are immutable, so cached footers never become stale.
	private final Cache<String, ParquetMetadata> footers = CacheBuilder.newBuilder().maximumSize(FOOTER_CACHE_SIZE).build();
	// Data files of the current snapshot, planned once per snapshot and filtered per fetch.
	private long plannedSnapshotId = -1;
	private List<FileScanTask> plannedFiles = List.of();
	// Page indexes per file and row group start position, for skipping pages in point fetches.
	private final Cache<String, Map<Long, ColumnIndexStore>> columnIndexes =
			CacheBuilder.newBuilder().maximumSize(FOOTER_CACHE_SIZE).build();

	public IcebergRecordStore(String archiveLocation) {
		this(archiveLocation, null);
	}

	public IcebergRecordStore(String archiveLocation, Duration retentionPeriod) {
		this(archiveLocation, retentionPeriod, null);
	}

	/**
	 * Configures partitioning for a new table, or verifies an existing table's spec.
	 * A null factory adopts existing partitioning and creates new tables unpartitioned.
	 * With a retention period, {@link #cleanUp()} removes the records that are older.
	 * Only itemId, contextId, propertyId, time, and seqNr may be partition sources.
	 * Partition evolution must be performed explicitly, outside this constructor.
	 */
	public IcebergRecordStore(String archiveLocation, Duration retentionPeriod,
			Function<Schema, PartitionSpec> partitioning) {
		this(KvinIcebergOptions.builder(archiveLocation).retentionPeriod(retentionPeriod)
				.partitioning(partitioning).build());
	}

	/**
	 * Opens the value table with the given settings, see {@link KvinIcebergOptions}.
	 * The cleanup interval is ignored; call {@link #cleanUp()} to clean up.
	 */
	public IcebergRecordStore(KvinIcebergOptions options) {
		this.root = ArchiveLocation.of(options.location());
		this.retentionPeriod = options.retentionPeriod();
		Function<Schema, PartitionSpec> partitioning = options.partitioning();
		validateTableProperties(options.tableProperties());
		Configuration conf = options.newHadoopConfiguration();
		this.inputFiles = new ParquetInputFiles(conf);

		PartitionSpec requestedSpec = partitioning == null ? null
				: Objects.requireNonNull(partitioning.apply(SCHEMA), "Partitioning factory returned null");
		if (requestedSpec != null) {
			validatePartitionSpec(requestedSpec);
		}

		try {
			if (root.isLocal()) {
				Files.createDirectories(root.localPath());
			}

			String location = root.resolve("iceberg");
			HadoopTables tables = new HadoopTables(conf);
			boolean existing = root.exists("iceberg/metadata", conf);

			if (existing) {
				table = tables.load(location);

				if (!table.schema().sameSchema(SCHEMA)) {
					throw new IllegalStateException("Incompatible Iceberg table at " + location);
				}
				table.specs().values().forEach(IcebergRecordStore::validatePartitionSpec);
				if (requestedSpec != null && !table.spec().compatibleWith(requestedSpec)) {
					throw new IllegalArgumentException("Partition configuration does not match existing Iceberg table at " + location);
				}
			} else {
				if (root.exists("metadata", conf)) {
					// archives of the removed KvinParquet store keep their metadata in this folder
					throw new IllegalArgumentException("Directory contains an unsupported Parquet archive: " + root);
				}

				Map<String, String> properties = new HashMap<>(tableProperties());
				properties.putAll(options.tableProperties());
				table = tables.create(SCHEMA, requestedSpec == null ? PartitionSpec.unpartitioned() : requestedSpec,
						SORT_ORDER, properties, location);
			}
			if (existing) {
				updateTableProperties(table, options.tableProperties());
			}

			enableManifestCaching(table);

			if (!table.sortOrder().sameOrder(SORT_ORDER)) {
				configureSortOrder(table.replaceSortOrder()).commit();
			}

			compactionTable = tables.load(location);
			enableManifestCaching(compactionTable);
		} catch (IOException e) {
			throw new UncheckedIOException("Unable to initialize Iceberg store at " + root, e);
		}
		compactor = Executors.newSingleThreadExecutor(task -> {
			String name = root.toString();
			Thread thread = new Thread(task, "kvin-iceberg-compaction-" + name.substring(name.lastIndexOf('/') + 1));
			thread.setDaemon(true);
			return thread;
		});
	}

	private static void validatePartitionSpec(PartitionSpec spec) {
		for (var field : spec.fields()) {
			var source = SCHEMA.findField(field.sourceId());
			if (source == null || field.sourceId() > 5
					|| !source.equals(spec.schema().findField(field.sourceId()))) {
				throw new IllegalArgumentException("Partition sources must be itemId, contextId, propertyId, time, or seqNr");
			}
		}
	}

	private static <T extends SortOrderBuilder<T>> T configureSortOrder(T builder) {
		return builder.asc("contextId").asc("itemId").asc("propertyId").desc("time").desc("seqNr");
	}

	boolean hasSnapshot() {
		table.refresh();
		return table.currentSnapshot() != null;
	}

	private static Map<String, String> tableProperties() {
		return Map.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name(), TableProperties.FORMAT_VERSION, "2", TableProperties.PARQUET_COMPRESSION, "zstd");
	}

	/**
	 * Rejects configured values of the properties that this store reads, so that a wrong
	 * configuration fails when the store opens and not on the first write or compaction.
	 */
	private static void validateTableProperties(Map<String, String> properties) {
		requireAtLeast(properties, WRITE_BATCH_RECORDS, 1);
		requireAtLeast(properties, COMPACTION_MIN_FILES, 2);
		requireAtLeast(properties, TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, 1);
		requireAtLeast(properties, TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, 1);
		requireAtLeast(properties, TableProperties.PARQUET_PAGE_SIZE_BYTES, 1);
	}

	private static void requireAtLeast(Map<String, String> properties, String key, long minimum) {
		String value = properties.get(key);
		if (value == null) {
			return;
		}
		try {
			if (Long.parseLong(value) >= minimum) {
				return;
			}
		} catch (NumberFormatException e) {
			// reported below
		}
		throw new IllegalArgumentException("Table property " + key + " must be a number of at least " + minimum + ": " + value);
	}

	/**
	 * Commits the configured table properties whose values differ from those of the table.
	 */
	private static void updateTableProperties(Table table, Map<String, String> properties) {
		Map<String, String> current = table.properties();
		var changes = properties.entrySet().stream()
				.filter(property -> !property.getValue().equals(current.get(property.getKey())))
				.toList();
		if (!changes.isEmpty()) {
			var update = table.updateProperties();
			changes.forEach(property -> update.set(property.getKey(), property.getValue()));
			update.commit();
			log.info("Updated properties {} of Iceberg table {}", changes, table.location());
		}
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
		table.refresh();
		int batchSize = PropertyUtil.propertyAsInt(table.properties(), WRITE_BATCH_RECORDS, DEFAULT_WRITE_BATCH_RECORDS);
		if (batchSize < 1) {
			throw new IllegalArgumentException(WRITE_BATCH_RECORDS + " must be at least 1");
		}
		// grows with the input, so small writes do not allocate a full batch
		List<GenericRecord> batch = new ArrayList<>();
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

				if (batch.size() >= batchSize) {
					writeBatch(batch, files, paths);
				}
			}

			if (!batch.isEmpty()) {
				writeBatch(batch, files, paths);
			}

			if (!files.isEmpty()) {
				commitAppend(files);
				committed = true;
				scheduleCompaction();
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
				synchronized (commitLock) {
					table.refresh();

					var append = table.newAppend();
					files.forEach(append::appendFile);
					append.commit();
				}

				return;
			} catch (CommitFailedException e) {
				if (attempt == MAX_COMMIT_RETRIES) {
					throw e;
				}

				log.warn("Iceberg append conflicted; retrying ({}/{})", attempt, MAX_COMMIT_RETRIES);
			}
		}
	}

	/**
	 * Starts a background compaction unless one is already queued. A compaction that is
	 * running when this is called is followed by another one, so new files are considered.
	 */
	private void scheduleCompaction() {
		if (!closed && compactionScheduled.compareAndSet(false, true)) {
			try {
				compactor.execute(() -> {
					compactionScheduled.set(false);
					try {
						compactTiers();
					} catch (IOException | RuntimeException e) {
						// already logged by compactTier
					}
				});
			} catch (RejectedExecutionException e) {
				// the store is closing
				compactionScheduled.set(false);
			}
		}
	}

	/**
	 * Compacts all tiers that have enough files, in the compaction thread. Waits for a
	 * running background compaction first, so that only one compaction runs at a time.
	 *
	 * @throws UncheckedIOException if reading or writing data files fails
	 * @throws org.apache.iceberg.exceptions.ValidationException if a concurrent delete
	 *                                                           replaced an input file
	 */
	public void compact() {
		runInCompactor(() -> {
			compactTiers();
			return null;
		});
	}

	/**
	 * Waits until the background compactions that were scheduled before this call are done.
	 */
	void awaitCompaction() {
		runInCompactor(() -> null);
	}

	private void runInCompactor(Callable<Void> task) {
		try {
			compactor.submit(task).get();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Interrupted while waiting for Iceberg compaction", e);
		} catch (ExecutionException e) {
			Throwable cause = e.getCause();
			if (cause instanceof RuntimeException runtime) {
				throw runtime;
			}
			if (cause instanceof Error error) {
				throw error;
			}
			if (cause instanceof IOException io) {
				throw new UncheckedIOException("Iceberg compaction failed", io);
			}
			throw new IllegalStateException("Iceberg compaction failed", cause);
		}
	}

	private void compactTiers() throws IOException {
		while (!closed && compactTier()) {
			// a merge creates a file of a higher tier, which may complete that tier
		}
	}

	/**
	 * The tier of a file with the given number of records: {@code floor(log_fanIn(records))}.
	 * Merging {@code fanIn} files of tier k yields at least {@code fanIn^(k+1)} records, so the
	 * output always belongs to a higher tier.
	 */
	static int tier(long records, int fanIn) {
		int tier = 0;
		for (long remaining = records; remaining >= fanIn; remaining /= fanIn) {
			tier++;
		}
		return tier;
	}

	/**
	 * Merges, in every partition, the files of the lowest tier that has at least
	 * {@code kvin.compaction.min-files} files, and commits all merges in one rewrite.
	 *
	 * @return whether files were rewritten
	 */
	private boolean compactTier() throws IOException {
		Table table = compactionTable;
		List<String> paths = new ArrayList<>();
		try {
			table.refresh();
			int fanIn = Integer.parseInt(table.properties().getOrDefault(COMPACTION_MIN_FILES, Integer.toString(DEFAULT_COMPACTION_MIN_FILES)));
			long targetSize = Long.parseLong(table.properties().getOrDefault(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, Long.toString(DEFAULT_COMPACTION_TARGET_SIZE)));
			if (fanIn < 2 || targetSize <= 0) {
				throw new IllegalArgumentException("Iceberg compaction requires kvin.compaction.min-files >= 2 and a positive write.target-file-size-bytes");
			}
			if (table.currentSnapshot() == null || !table.sortOrder().sameOrder(SORT_ORDER)) {
				return false;
			}

			long snapshotId = table.currentSnapshot().snapshotId();
			// eligible files per partition and tier
			Map<PartitionGroup, TreeMap<Integer, Map<String, DataFile>>> groups = new LinkedHashMap<>();
			int maximum = Math.max(fanIn, COMPACTION_MAX_INPUT_FILES);
			try (CloseableIterable<FileScanTask> tasks = table.newScan().useSnapshot(snapshotId).planFiles()) {
				for (FileScanTask task : tasks) {
					if (!task.deletes().isEmpty()) {
						throw new IllegalStateException("Iceberg delete files are not supported by KvinIceberg compaction");
					}
					DataFile file = task.file().copy();
					SortOrder order = table.sortOrders().get(file.sortOrderId());
					if (file.format() == FileFormat.PARQUET && order != null && order.sameOrder(SORT_ORDER)
							&& file.fileSizeInBytes() < targetSize / 2) {
						PartitionSpec spec = table.specs().get(file.specId());
						PartitionGroup group = new PartitionGroup(file.specId(),
								StructLikeWrapper.forType(spec.partitionType()).copyFor(file.partition()));
						Map<String, DataFile> candidates = groups.computeIfAbsent(group, key -> new TreeMap<>())
								.computeIfAbsent(tier(file.recordCount(), fanIn), key -> new LinkedHashMap<>());
						if (candidates.size() < maximum) {
							candidates.putIfAbsent(file.location(), file);
						}
					}
				}
			}

			Set<DataFile> inputs = new LinkedHashSet<>();
			Set<DataFile> replacements = new LinkedHashSet<>();
			for (var entry : groups.entrySet()) {
				Map<String, DataFile> candidates = entry.getValue().values().stream()
						.filter(tierFiles -> tierFiles.size() >= fanIn).findFirst().orElse(null);
				if (candidates == null) {
					continue;
				}
				List<String> groupPaths = new ArrayList<>();
				Set<DataFile> merged;
				try {
					merged = mergeCompactionFiles(table, candidates.values(), table.specs().get(entry.getKey().specId()),
							candidates.values().iterator().next().partition(), targetSize, groupPaths);
				} finally {
					paths.addAll(groupPaths);
				}
				if (merged.size() >= candidates.size()) {
					cleanup(groupPaths);
				} else {
					inputs.addAll(candidates.values());
					replacements.addAll(merged);
				}
			}
			if (inputs.isEmpty()) {
				return false;
			}
			// Validate against exactly the snapshot read: concurrent appends are safe,
			// but replaced inputs and newly added row-level deletes must conflict.
			synchronized (commitLock) {
				table.refresh();
				table.newRewrite().validateFromSnapshot(snapshotId)
						.rewriteFiles(inputs, replacements).commit();
			}
			invalidateFooters(inputs);
			return true;
		} catch (CommitStateUnknownException e) {
			log.error("Iceberg compaction commit state is unknown; output files are retained", e);
			throw e;
		} catch (IOException | RuntimeException e) {
			cleanup(paths);
			log.error("Iceberg compaction failed; committed data is unchanged", e);
			throw e;
		}
	}

	private record PartitionGroup(int specId, StructLikeWrapper partition) {
	}

	private Set<DataFile> mergeCompactionFiles(Table table, Iterable<DataFile> files, PartitionSpec spec,
			StructLike partition, long targetSize, List<String> paths) throws IOException {
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
				DataWriter<org.apache.iceberg.data.Record> writer = newWriter(table, spec, partition, targetSize, paths);
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

	private DataWriter<org.apache.iceberg.data.Record> newWriter(Table table, PartitionSpec spec, StructLike partition,
			Long targetSize, List<String> paths) throws IOException {
		String path = root.resolve("iceberg/data/" + UUID.randomUUID() + ".parquet");
		paths.add(path);
		// creates missing parent directories of local files
		var builder = Parquet.write(table.io().newOutputFile(path)).forTable(table);
		Map<String, String> properties = table.properties();
		long rowGroupSize = PropertyUtil.propertyAsLong(properties, TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES,
				DEFAULT_ROW_GROUP_SIZE);
		if (targetSize != null) {
			rowGroupSize = Math.min(targetSize, rowGroupSize);
		}
		builder.set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Long.toString(rowGroupSize));
		builder.set(TableProperties.PARQUET_PAGE_SIZE_BYTES, Long.toString(PropertyUtil.propertyAsLong(properties,
				TableProperties.PARQUET_PAGE_SIZE_BYTES, DEFAULT_PAGE_SIZE)));
		var appender = builder.writerVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
				.createWriterFunc(message -> GenericParquetWriter.create(SCHEMA, message)).<org.apache.iceberg.data.Record>build();
		return new DataWriter<>(appender, FileFormat.PARQUET, path, spec, partition, null, table.sortOrder());
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
				// does nothing if the file was never created
				table.io().deleteFile(path);
			} catch (RuntimeException e) {
				log.warn("Unable to remove uncommitted Iceberg file {}", path, e);
			}
		}
	}

	private void writeBatch(List<GenericRecord> batch, List<DataFile> files, List<String> paths) throws IOException {
		PartitionSpec spec = table.spec();
		validatePartitionSpec(spec);
		PartitionKey key = new PartitionKey(spec, SCHEMA);
		Map<PartitionKey, List<GenericRecord>> partitions = new LinkedHashMap<>();
		for (GenericRecord row : batch) {
			key.partition(row);
			partitions.computeIfAbsent(key.copy(), ignored -> new ArrayList<>()).add(row);
		}
		for (var entry : partitions.entrySet()) {
			writePartition(entry.getValue(), spec, entry.getKey(), files, paths);
		}
		batch.clear();
	}

	private void writePartition(List<GenericRecord> batch, PartitionSpec spec, PartitionKey partition,
			List<DataFile> files, List<String> paths) throws IOException {
		batch.sort(ORDER);

		for (int i = 0; i < batch.size(); i++) {
			GenericRecord current = batch.get(i);

			boolean first = i == 0 || !current.get(0).equals(batch.get(i - 1).get(0)) || !current.get(1).equals(batch.get(i - 1).get(1)) || !current.get(2).equals(batch.get(i - 1).get(2));

			current.set(5, first);
		}

		DataWriter<org.apache.iceberg.data.Record> writer = newWriter(table, spec, partition, null, paths);

		try (writer) {
			batch.forEach(writer::write);
		}

		files.add(writer.toDataFile());
	}

	public synchronized void createBranch(String branchName) {
		table.refresh();

		if (table.currentSnapshot() == null) {
			throw new IllegalStateException("Cannot create a branch without a snapshot");
		}

		synchronized (commitLock) {
			table.refresh();
			table.manageSnapshots().createBranch(branchName, table.currentSnapshot().snapshotId()).commit();
		}
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
		IExtendedIterator<KvinTuple> tuples = rows.mapWith(row -> new KvinTuple(key(row.contextId()),
				key(row.itemId()), key(row.propertyId()), row.time(), row.seqNr(), row.value()));
		return new AggregatingIterator<>(tuples, interval, op.trim().toLowerCase(java.util.Locale.ROOT), limit) {
			@Override
			protected KvinTuple createElement(URI item, URI property, URI context, long time, int seqNr, Object value) {
				return new KvinTuple(context, item, property, time, seqNr, value);
			}
		}.mapWith(tuple -> new KvinRecord(number(tuple.context), number(tuple.item), number(tuple.property),
				tuple.time, tuple.seqNr, tuple.value));
	}

	private static URI key(long id) {
		return URIs.createURI("urn:kvin:id:" + id);
	}

	private static long number(URI key) {
		return Long.parseLong(key.toString().substring("urn:kvin:id:".length()));
	}

	/**
	 * A planned data file with the smallest key, in {@link #ROW_ORDER}, that any
	 * matching row in it can have.
	 */
	private record PlannedFile(FileScanTask task, KvinRow bound) {
	}

	/**
	 * Derives a lower bound for the matching rows of a file from its column
	 * statistics, clamped by the query. The bound is valid lexicographically:
	 * every matching row has the requested context, itemId and propertyId at
	 * least as large, and a time no later than the bound's.
	 */
	private static KvinRow lowerBound(DataFile file, Set<Long> itemIds, long contextId, Set<Long> propertyIds, Long end) {
		long item = Math.max(bound(file.lowerBounds(), 1, Long.MIN_VALUE), Collections.min(itemIds));
		long property = bound(file.lowerBounds(), 3, Long.MIN_VALUE);
		if (!propertyIds.isEmpty()) {
			property = Math.max(property, Collections.min(propertyIds));
		}
		long time = bound(file.upperBounds(), 4, Long.MAX_VALUE);
		if (end != null) {
			time = Math.min(time, end);
		}
		return new KvinRow(contextId, item, property, time, Integer.MAX_VALUE, null);
	}

	private static long bound(Map<Integer, ByteBuffer> bounds, int fieldId, long fallback) {
		ByteBuffer value = bounds == null ? null : bounds.get(fieldId);
		return value == null ? fallback : Conversions.<Long>fromByteBuffer(Types.LongType.get(), value);
	}

	/**
	 * Returns all data files of the current snapshot with the statistics used by
	 * fetches. Planning reads the manifests, so it is repeated only after the
	 * snapshot changed. Call {@code table.refresh()} first.
	 */
	private List<FileScanTask> currentFiles() {
		Snapshot snapshot = table.currentSnapshot();
		if (snapshot == null) {
			return List.of();
		}
		if (snapshot.snapshotId() != plannedSnapshotId) {
			List<FileScanTask> tasks = new ArrayList<>();
			try (CloseableIterable<FileScanTask> planned = table.newScan().useSnapshot(snapshot.snapshotId())
					.includeColumnStats(FETCH_STATS_COLUMNS).planFiles()) {
				planned.forEach(tasks::add);
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
			plannedFiles = List.copyOf(tasks);
			plannedSnapshotId = snapshot.snapshotId();
		}
		return plannedFiles;
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

		// same pruning as a filtered scan: partition values, then column statistics
		Expression fileFilter = filter;
		InclusiveMetricsEvaluator metrics = new InclusiveMetricsEvaluator(table.schema(), fileFilter);
		Map<Integer, Evaluator> partitionFilters = new HashMap<>();
		List<PlannedFile> files = new ArrayList<>();
		for (FileScanTask task : currentFiles()) {
			DataFile file = task.file();
			Evaluator partitionFilter = partitionFilters.computeIfAbsent(file.specId(), specId -> {
				PartitionSpec spec = table.specs().get(specId);
				return new Evaluator(spec.partitionType(), Projections.inclusive(spec).project(fileFilter));
			});
			if (!partitionFilter.eval(file.partition()) || !metrics.eval(file)) {
				continue;
			}
			if (!task.deletes().isEmpty()) {
				throw new IllegalStateException("Iceberg delete files are not supported by KvinIceberg reads");
			}
			files.add(new PlannedFile(task, lowerBound(file, itemIds, contextId, propertyIds, end)));
		}
		// Files are opened lazily in bound order: a file is opened only once its bound
		// is not after the merge head, so it cannot hold a smaller unread matching row.
		files.sort(Comparator.comparing(PlannedFile::bound, ROW_ORDER));

		PriorityQueue<RowCursor> cursors = new PriorityQueue<>(Comparator.comparing(cursor -> cursor.row, ROW_ORDER));
		Expression readFilter = filter;
		long requestedSeries = properties.isEmpty() ? 0 : (long) itemIds.size() * propertyIds.size();

		return new NiceIterator<>() {
			KvinRow previous;
			long seriesCount;
			long completedSeries;
			long seriesItemId;
			long seriesPropertyId;
			boolean seriesStarted;
			// Last series whose limit was reached; applied to files opened later.
			boolean skipping;
			long skipItemId;
			long skipPropertyId;
			int nextFile;
			// Number of opened files, for tests.
			int opened;
			// Advance only when another tuple is requested, so a satisfied limit reads no extra row.
			RowCursor pending;
			KvinRecord next;
			boolean closed;

			private void openFiles() {
				while (nextFile < files.size() && (cursors.isEmpty()
						|| ROW_ORDER.compare(files.get(nextFile).bound(), cursors.peek().row) <= 0)) {
					RowCursor cursor = new RowCursor(files.get(nextFile++).task(), readFilter, footers, columnIndexes, inputFiles);
					opened++;
					try {
						if (skipping) {
							cursor.skipThrough(contextId, skipItemId, skipPropertyId);
						}
						if (cursor.advance()) {
							cursors.add(cursor);
						} else {
							cursor.close();
						}
					} catch (RuntimeException e) {
						try {
							cursor.close();
						} catch (RuntimeException closeFailure) {
							if (e != closeFailure) {
								e.addSuppressed(closeFailure);
							}
						}
						throw e;
					}
				}
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
					while (pending != null || !cursors.isEmpty() || nextFile < files.size()) {
						if (pending != null) {
							if (pending.advance()) {
								cursors.add(pending);
							} else {
								pending.close();
							}
							pending = null;
						}
						openFiles();
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
							skipping = true;
							skipItemId = itemId;
							skipPropertyId = propertyId;
							pending.skipThrough(contextId, itemId, propertyId);
							for (RowCursor cursor : cursors) {
								cursor.skipThrough(contextId, itemId, propertyId);
							}
						}

						Object value = row.value();

						if (value instanceof ByteBuffer bytes) {
							value = Records.decodeRecord(bytes.duplicate());
						}

						next = new KvinRecord(contextId, itemId, propertyId, time, seqNr, value);
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

		return deleteRows(filter, row -> matchesDelete(row, itemId, propertyId, contextId, lower, upper), file -> false);
	}

	/**
	 * Deletes all records of a context. Data files that contain only this context, for
	 * example with an identity partition on {@code contextId}, are removed from the table
	 * without reading or rewriting them. Other files that contain the context are rewritten.
	 */
	@Override
	public synchronized long delete(Long context) {
		long contextId = context == null ? DEFAULT_CONTEXT : context;
		return deleteRows(Expressions.equal("contextId", contextId), row -> row.contextId() == contextId,
				file -> onlyValue(file, CONTEXT_FIELD_ID, contextId));
	}

	/**
	 * Removes the records whose time is older than the retention period, measured from now,
	 * if the store has a retention period. Then expires old snapshots with
	 * {@link #expireSnapshots()}.
	 *
	 * @return the number of removed records
	 */
	public synchronized long cleanUp() {
		long removed = 0;
		if (retentionPeriod != null) {
			long cutoff = System.currentTimeMillis() - retentionPeriod.toMillis();
			log.info("Removing Iceberg records older than {}", Instant.ofEpochMilli(cutoff));
			removed = deleteOlderThan(cutoff);
		}
		expireSnapshots();
		return removed;
	}

	/**
	 * Expires the snapshots that were replaced by a newer snapshot longer ago than the table
	 * property {@code history.expire.max-snapshot-age-ms} (default 5 days), and deletes the
	 * files that only they reference. The table property
	 * {@code history.expire.min-snapshots-to-keep} and branches keep snapshots as well.
	 *
	 * @return the number of expired snapshots
	 */
	public synchronized int expireSnapshots() {
		table.refresh();
		long maxAge = PropertyUtil.propertyAsLong(table.properties(), TableProperties.MAX_SNAPSHOT_AGE_MS,
				TableProperties.MAX_SNAPSHOT_AGE_MS_DEFAULT);
		return expireSnapshotsReplacedBefore(System.currentTimeMillis() - maxAge);
	}

	/**
	 * Expires the snapshots that stopped being current before {@code time}. Fetches read the
	 * files of the snapshot that was current when they started, so a snapshot is not expired
	 * just because it was created long ago.
	 *
	 * @return the number of expired snapshots
	 */
	synchronized int expireSnapshotsReplacedBefore(long time) {
		synchronized (commitLock) {
			return expireSnapshotsReplacedBeforeLocked(time);
		}
	}

	private int expireSnapshotsReplacedBeforeLocked(long time) {
		table.refresh();
		// the snapshot that was current at the given time; all older snapshots were replaced before
		Snapshot currentAtTime = null;
		for (HistoryEntry entry : table.history()) {
			if (entry.timestampMillis() > time) {
				break;
			}
			Snapshot snapshot = table.snapshot(entry.snapshotId());
			if (snapshot != null) {
				currentAtTime = snapshot;
			}
		}
		if (currentAtTime == null) {
			return 0;
		}
		long olderThan = currentAtTime.timestampMillis();
		// Iceberg decides which snapshots expire, for example it keeps snapshots of branches
		List<Snapshot> expired = table.expireSnapshots().expireOlderThan(olderThan).apply();
		if (expired.isEmpty()) {
			return 0;
		}
		/*
		 * Naming the snapshots selects the cleanup that deletes every file the remaining
		 * snapshots do not reach. The default incremental cleanup deletes a data file only
		 * when the snapshot that removed it expires, so files dropped by the latest rewrite
		 * would stay until a later expiry.
		 */
		ExpireSnapshots expire = table.expireSnapshots().expireOlderThan(olderThan)
				.deleteWith(this::deleteExpiredFile);
		expired.forEach(snapshot -> expire.expireSnapshotId(snapshot.snapshotId()));
		expire.commit();
		log.info("Expired {} Iceberg snapshots replaced before {}", expired.size(), Instant.ofEpochMilli(time));
		return expired.size();
	}

	private void deleteExpiredFile(String location) {
		footers.invalidate(location);
		columnIndexes.invalidate(location);
		table.io().deleteFile(location);
	}

	/**
	 * Removes all records with a time before {@code time}. Data files whose records are all
	 * older, for example with a partition on {@code time}, are removed from the table without
	 * reading or rewriting them. Other files that contain older records are rewritten.
	 *
	 * @return the number of removed records
	 */
	public synchronized long deleteOlderThan(long time) {
		return deleteRows(Expressions.lessThan("time", time), row -> row.time() < time,
				file -> file.upperBounds() != null && file.upperBounds().containsKey(TIME_FIELD_ID)
						&& bound(file.upperBounds(), TIME_FIELD_ID, Long.MAX_VALUE) < time);
	}

	/**
	 * Returns whether the column statistics of the file show that all its rows have the given value.
	 */
	private static boolean onlyValue(DataFile file, int fieldId, long value) {
		Map<Integer, ByteBuffer> lowerBounds = file.lowerBounds();
		Map<Integer, ByteBuffer> upperBounds = file.upperBounds();
		return lowerBounds != null && upperBounds != null
				&& lowerBounds.containsKey(fieldId) && upperBounds.containsKey(fieldId)
				&& bound(lowerBounds, fieldId, Long.MIN_VALUE) == value
				&& bound(upperBounds, fieldId, Long.MAX_VALUE) == value;
	}

	/**
	 * Removes the matching rows with a copy-on-write rewrite in one commit.
	 *
	 * @param filter       selects the data files that may contain matching rows
	 * @param matches      selects the rows to delete
	 * @param fullyMatches selects data files whose rows all match, they are dropped without reading them
	 * @return the number of deleted rows
	 */
	private long deleteRows(Expression filter, Predicate<KvinRow> matches, Predicate<DataFile> fullyMatches) {
		table.refresh();

		Map<String, DataFile> affectedFiles = new LinkedHashMap<>();

		try (CloseableIterable<FileScanTask> tasks = table.newScan().filter(filter)
				.includeColumnStats(FETCH_STATS_COLUMNS).planFiles()) {
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

		try {
			for (DataFile file : affectedFiles.values()) {
				if (fullyMatches.test(file)) {
					deleted += file.recordCount();
					deleteFiles.add(file);
					continue;
				}

				List<GenericRecord> retained = new ArrayList<>();
				long removedFromFile = 0;

				try (CloseableIterable<org.apache.iceberg.data.Record> rows = Parquet.read(table.io().newInputFile(file.location())).project(SCHEMA).createReaderFunc(schema -> GenericParquetReaders.buildReader(SCHEMA, schema)).build()) {
					for (org.apache.iceberg.data.Record record : rows) {
						KvinRow row = toKvinRow(record);

						if (matches.test(row)) {
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

			return deleted;
		} catch (CommitStateUnknownException e) {
			throw e;
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
				synchronized (commitLock) {
					table.refresh();

					table.newRewrite().rewriteFiles(deleteFiles, addFiles).commit();
				}
				invalidateFooters(deleteFiles);

				return;
			} catch (CommitFailedException e) {
				if (attempt == MAX_COMMIT_RETRIES) {
					throw e;
				}

				log.warn("Iceberg rewrite conflicted; retrying ({}/{})", attempt, MAX_COMMIT_RETRIES);
			}
		}
	}

	private void invalidateFooters(Set<DataFile> replaced) {
		replaced.forEach(file -> {
			footers.invalidate(file.location());
			columnIndexes.invalidate(file.location());
		});
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

		return new KvinRow((Long) record.get(1), (Long) record.get(0), (Long) record.get(2), (Long) record.get(3), (Integer) record.get(4), value);
	}

	private static final class RowCursor implements AutoCloseable {
		final SeriesSkippingReader rows;
		final CloseableIterator<KvinRow> iterator;
		KvinRow row;

		RowCursor(FileScanTask task, Expression filter, Cache<String, ParquetMetadata> footers,
				Cache<String, Map<Long, ColumnIndexStore>> columnIndexes, ParquetInputFiles inputFiles) {
			if (!task.deletes().isEmpty()) {
				throw new IllegalStateException("Iceberg delete files are not supported by KvinIceberg reads");
			}

			try {
				this.rows = new SeriesSkippingReader(SCHEMA, task, filter, footers, columnIndexes, inputFiles);
			} catch (IOException e) {
				throw new UncheckedIOException("Unable to open Iceberg series reader", e);
			}

			this.iterator = rows.iterator();
		}

		void skipThrough(long context, long item, long property) {
			rows.skipThrough(context, item, property);
		}

		boolean advance() {
			if (!iterator.hasNext()) {
				return false;
			}

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

	/**
	 * Waits for a running compaction to finish; queued compactions are skipped.
	 */
	@Override
	public void close() {
		closed = true;
		compactor.shutdown();
		try {
			while (!compactor.awaitTermination(10, TimeUnit.SECONDS)) {
				log.info("Waiting for Iceberg compaction to finish before closing {}", root);
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		ManifestFiles.dropCache(table.io());
		ManifestFiles.dropCache(compactionTable.io());
	}
}
