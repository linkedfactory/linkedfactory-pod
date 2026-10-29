package io.github.linkedfactory.core.kvin.iceberg;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.github.linkedfactory.core.kvin.ids.IdKind;
import io.github.linkedfactory.core.kvin.ids.IdStore;
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
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ParquetProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Persistent URI-ID mappings in three independent Iceberg tables.
 *
 * <p>New mappings are staged until {@link #flush()} (or close). Allocation holds
 * the archive's file lock until flushing, so callers must flush a batch before
 * waiting for another process to allocate IDs. Instances in this JVM coordinate
 * automatically, flushing a previous allocator before switching writers.
 * Pending batches automatically flush at 8192 mappings.</p>
 */
public class IcebergIdStore implements IdStore, AutoCloseable {
	private static final Logger log = LoggerFactory.getLogger(IcebergIdStore.class);
	private static final int MAX_COMMIT_RETRIES = 5;
	private static final int ID_CACHE_SIZE = 250_000;
	private static final int BATCH_SIZE = 8192;
	private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();
	private static final Schema ID_SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()), Types.NestedField.required(2, "value", Types.StringType.get()));
	private static final Cache<Path, Coordinator> COORDINATORS = CacheBuilder.newBuilder().weakValues().build();

	private final Path root;
	private final IdTable[] ids = new IdTable[3];
	private final Coordinator coordinator;
	@SuppressWarnings("unchecked")
	private final Map<URI, Long>[] pending = new Map[]{new HashMap<>(), new HashMap<>(), new HashMap<>()};
	private FileChannel channel;
	private java.nio.channels.FileLock lock;
	private int pendingCount;
	private boolean closed;

	private static final class Coordinator {
		IcebergIdStore writer;
	}

	public IcebergIdStore(String archiveLocation) {
		this(archiveLocation, false);
	}

	IcebergIdStore(String archiveLocation, boolean requireExisting) {
		root = Path.of(archiveLocation).toAbsolutePath().normalize();
		try {
			Files.createDirectories(root);
			// Canonical paths also coordinate instances opened through directory symlinks.
			coordinator = COORDINATORS.get(root.toRealPath(), Coordinator::new);
			synchronized (coordinator) {
				if (coordinator.writer != null) {
					coordinator.writer.flush();
				}
				try (FileChannel initialization = FileChannel.open(root.resolve("iceberg-ids.lock"),
						StandardOpenOption.CREATE, StandardOpenOption.WRITE); var ignored = initialization.lock()) {
					HadoopTables tables = new HadoopTables(new Configuration());
					String[] names = {"items", "contexts", "properties"};
					if (requireExisting) {
						for (String name : names) {
							Path path = root.resolve("iceberg-ids").resolve(name);
							if (!Files.exists(path.resolve("metadata"))) {
								throw new IOException("Missing Iceberg ID table at " + path);
							}
						}
					}
					for (int kind = 0; kind < names.length; kind++) {
						Path path = root.resolve("iceberg-ids").resolve(names[kind]);
						Table table = Files.exists(path.resolve("metadata")) ? tables.load(path.toString())
								: tables.create(ID_SCHEMA, SPEC, tableProperties(), path.toString());
						if (!table.schema().sameSchema(ID_SCHEMA) || !table.spec().isUnpartitioned()) {
							throw new IllegalStateException("Incompatible Iceberg ID table at " + path);
						}
						enableManifestCaching(table);
						ids[kind] = new IdTable(table);
					}
				}
			}
		} catch (IOException e) {
			throw new UncheckedIOException("Unable to initialize Iceberg IDs at " + root, e);
		} catch (java.util.concurrent.ExecutionException e) {
			throw new IllegalStateException(e);
		}
	}

	private static int index(IdKind kind) {
		return switch (kind) {
			case ITEM -> 0;
			case CONTEXT -> 1;
			case PROPERTY -> 2;
		};
	}

	private void ensureOpen() {
		if (closed) {
			throw new IllegalStateException("Iceberg ID store is closed");
		}
	}

	private void beginAllocation() {
		if (coordinator.writer == this) {
			return;
		}
		if (coordinator.writer != null) {
			coordinator.writer.flush();
		}
		try {
			channel = FileChannel.open(root.resolve("iceberg-ids.lock"),
					StandardOpenOption.CREATE, StandardOpenOption.WRITE);
			lock = channel.lock();
			coordinator.writer = this;
			for (IdTable idTable : ids) {
				idTable.refresh();
			}
		} catch (IOException | RuntimeException e) {
			releaseAllocation();
			if (e instanceof IOException io) {
				throw new UncheckedIOException(io);
			}
			throw (RuntimeException) e;
		}
	}

	@Override
	public long getId(URI uri, IdKind kind, boolean createIfNotExists) {
		return resolveIds(List.of(uri), kind, createIfNotExists).getOrDefault(uri, 0L);
	}

	/**
	 * Resolves a batch with a single scan for uncached, unstaged URIs. Missing
	 * mappings are omitted unless {@code create} is true.
	 */
	public Map<URI, Long> resolveIds(List<URI> uris, IdKind kind, boolean create) {
		synchronized (coordinator) {
			ensureOpen();
			if (create && !uris.isEmpty()) {
				beginAllocation();
			}
			int k = index(kind);
			List<URI> unstaged = uris.stream().filter(uri -> !pending[k].containsKey(uri)).toList();
			Map<URI, Long> result = ids[k].resolveIds(unstaged);
			for (URI uri : uris) {
				Long staged = pending[k].get(uri);
				if (staged != null) {
					result.put(uri, staged);
				}
				if (create && !result.containsKey(uri)) {
					ids[k].ensureNextId();
					long id = ++ids[k].nextId;
					pending[k].put(uri, id);
					result.put(uri, id);
					if (++pendingCount == BATCH_SIZE) {
						// Keep the lock: lookups for the rest of this batch must stay valid.
						flushPending(false);
					}
				}
			}
			return result;
		}
	}

	@Override
	public URI getUri(long id, IdKind kind) {
		return resolveUris(Set.of(id), kind).get(id);
	}

	URI cachedUri(long id, IdKind kind) {
		synchronized (coordinator) {
			return ids[index(kind)].reverse.getIfPresent(id);
		}
	}

	Map<Long, URI> resolveUris(Set<Long> requested, IdKind kind) {
		synchronized (coordinator) {
			ensureOpen();
			Map<Long, URI> result = new HashMap<>();
			int k = index(kind);
			pending[k].forEach((uri, id) -> {
				if (requested.contains(id)) {
					result.put(id, uri);
				}
			});
			Set<Long> missing = new HashSet<>(requested);
			missing.removeAll(result.keySet());
			result.putAll(ids[k].resolveUris(missing));
			return result;
		}
	}

	@Override
	public void flush() {
		synchronized (coordinator) {
			ensureOpen();
			flushPending(true);
		}
	}

	private void flushPending(boolean release) {
		try {
			for (int k = 0; k < ids.length; k++) {
				ids[k].append(pending[k]);
				pendingCount -= pending[k].size();
				pending[k].clear();
			}
		} catch (IOException e) {
			throw new UncheckedIOException("Unable to flush Iceberg IDs", e);
		} finally {
			// Keep the lock after a failed append: published IDs must never be reassigned.
			if (release && pendingCount == 0) {
				releaseAllocation();
			}
		}
	}

	private void releaseAllocation() {
		try {
			if (lock != null) {
				lock.close();
			}
			if (channel != null) {
				channel.close();
			}
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		} finally {
			lock = null;
			channel = null;
			if (coordinator.writer == this) {
				coordinator.writer = null;
			}
		}
	}

	@Override
	public void close() {
		synchronized (coordinator) {
			if (!closed) {
				flush();
				closed = true;
				for (IdTable idTable : ids) {
					ManifestFiles.dropCache(idTable.table.io());
				}
			}

		}
	}
	private static Map<String, String> tableProperties() {
		return Map.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name(), TableProperties.FORMAT_VERSION, "2", TableProperties.PARQUET_COMPRESSION, "zstd");
	}

	private static void enableManifestCaching(Table table) {
		Map<String, String> properties = new HashMap<>(table.io().properties());
		properties.put(CatalogProperties.IO_MANIFEST_CACHE_ENABLED, "true");
		table.io().initialize(properties);
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

}
