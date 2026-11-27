package io.github.linkedfactory.core.kvin.memtable;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.Striped;
import com.google.common.util.concurrent.Uninterruptibles;
import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinException;
import io.github.linkedfactory.core.kvin.KvinListener;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.Record;
import io.github.linkedfactory.core.kvin.records.KvinRecord;
import io.github.linkedfactory.core.kvin.util.AggregatingIterator;
import io.github.linkedfactory.core.kvin.util.Values;
import io.github.linkedfactory.core.kvin.util.Varint;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.commons.iterator.WrappedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.impl.Iq80DBFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

public class KvinMemTable implements Kvin {
	enum EntryType {
		SubjectToId(0, 1),
		PropertyToId(1, 3),
		ResourceToId(2, 5),
		ContextToId(3, 7);

		private final int index;
		private final int id;

		EntryType(int index, int id) {
			this.index = index;
			this.id = id;
		}

		public int getIndex() {
			return index;
		}

		public int getId() {
			return id;
		}

		public int getReverse() {
			return id + 1;
		}
	}

	private static final Logger log = LoggerFactory.getLogger(KvinMemTable.class);

	final Set<KvinListener> listeners = new CopyOnWriteArraySet<>();

	final Striped<ReadWriteLock> locks = Striped.readWriteLock(64);

	final AtomicLong[] nextIds;

	record CacheKey(String uri, int entryTypeId) {
	}

	/**
	 * Number of new IDs after which a put writes its ID batch. Each new ID adds two entries
	 * to the batch, the forward and the reverse mapping.
	 */
	static final int MAX_PENDING_IDS = 5_000;

	// IDs generated into an ID batch that is not yet written, so that later tuples of the
	// same put and concurrent lookups find them. An ID batch removes its IDs after writing,
	// and only one put runs at a time, which bounds the size by MAX_PENDING_IDS.
	final Map<CacheKey, Long> pendingIds = new ConcurrentHashMap<>();

	final Cache<CacheKey, Long> uriToIdCache =
			CacheBuilder.newBuilder().maximumSize(20_000).build();

	record IdCacheKey(long id, int entryTypeId) {
	}

	private final Cache<IdCacheKey, URI> idToUriCache =
			CacheBuilder.newBuilder().maximumSize(20_000).build();

	final KvinRecord END_MARKER = new KvinRecord();
	final Path path;
	final TableFileSet tableFiles;
	final ExecutorService executor = Executors.newSingleThreadExecutor();
	boolean async = true;
	final DB ids;
	final CircularBuffer<KvinRecord> queue = new CircularBuffer<>(10000);

	// whether each put and delete persists its entries before it returns
	final boolean syncWrites;

	/**
	 * Opens the store in the given directory, creating it if required. Written values are
	 * persisted by the operating system, so they survive a crash of the process, but not
	 * necessarily a crash of the operating system.
	 */
	public KvinMemTable(Path path) throws IOException {
		this(path, false);
	}

	/**
	 * Opens the store in the given directory, creating it if required. With
	 * <code>syncWrites</code>, each put and delete persists its values before it returns, so
	 * that they also survive a crash of the operating system or a power loss. This makes small
	 * writes much slower.
	 */
	public KvinMemTable(Path path, boolean syncWrites) throws IOException {
		this(path, syncWrites, TableFileSet::new);
	}

	interface TableFileSetFactory {
		TableFileSet create(Path path) throws IOException;
	}

	/**
	 * Creates a store whose records are kept in the table file set that the factory creates
	 * for the given path.
	 */
	KvinMemTable(Path path, TableFileSetFactory tableFileSetFactory) throws IOException {
		this(path, false, tableFileSetFactory);
	}

	KvinMemTable(Path path, boolean syncWrites, TableFileSetFactory tableFileSetFactory) throws IOException {
		this.path = path;
		this.syncWrites = syncWrites;
		Path valueStorePath = path.resolve("ids.dat");
		this.ids = Iq80DBFactory.factory.open(new File(valueStorePath.toFile(), "ids"),
				new org.iq80.leveldb.Options()
						.createIfMissing(true)
						.compressionType(CompressionType.SNAPPY));
		this.nextIds = Arrays.stream(readNextIds(List.of(EntryType.values())))
				.mapToObj(AtomicLong::new).toArray(AtomicLong[]::new);
		try {
			this.tableFiles = tableFileSetFactory.create(path);
		} catch (IOException | RuntimeException e) {
			ids.close();
			executor.shutdown();
			throw e;
		}
	}

	/**
	 * The new URI-ID mappings of one put. The IDs are visible to lookups through
	 * {@link #pendingIds} until the batch is written.
	 */
	final class IdBatch implements AutoCloseable {
		WriteBatch batch = ids.createWriteBatch();
		final List<CacheKey> keys = new ArrayList<>();
		// the new items of the batch, and the new items of all written batches
		final List<URI> newItems = new ArrayList<>();
		final List<URI> writtenItems = new ArrayList<>();

		void add(URI uri, EntryType entryType, CacheKey cacheKey, long id, byte[] key, byte[] idBytes,
		         byte[] idKeyBytes, byte[] uriBytes) {
			batch.put(key, idBytes);
			batch.put(idKeyBytes, uriBytes);
			keys.add(cacheKey);
			pendingIds.put(cacheKey, id);
			if (entryType == EntryType.SubjectToId) {
				newItems.add(uri);
			}
		}

		boolean isFull() {
			return keys.size() >= MAX_PENDING_IDS;
		}

		/**
		 * Writes the mappings and moves their IDs from the pending IDs to the bounded cache.
		 * The IDs are removed only after the write, so that a concurrent lookup finds them
		 * either as pending or in the database.
		 */
		void write() throws IOException {
			if (keys.isEmpty()) {
				return;
			}
			// Ensure that writes to the id database are always synced to disk.
			ids.write(batch, new WriteOptions().sync(true));
			for (CacheKey key : keys) {
				Long id = pendingIds.remove(key);
				if (id != null) {
					uriToIdCache.put(key, id);
				}
			}
			keys.clear();
			writtenItems.addAll(newItems);
			newItems.clear();
			batch.close();
			batch = ids.createWriteBatch();
		}

		@Override
		public void close() throws IOException {
			// IDs of a batch that could not be written must not be used by later puts
			for (CacheKey key : keys) {
				pendingIds.remove(key);
			}
			keys.clear();
			newItems.clear();
			batch.close();
		}
	}

	private long nextId(EntryType entryType) {
		return nextIds[entryType.getIndex()].getAndIncrement();
	}

	private long[] readNextIds(List<EntryType> entryTypes) {
		long[] result = new long[entryTypes.size()];
		Arrays.fill(result, 1L); // default id is 1

		try (var it = ids.iterator()) {
			byte[] idPrefix = new byte[2];
			idPrefix[1] = (byte) 0xFF;

			for (EntryType t : entryTypes) {
				idPrefix[0] = (byte) t.getReverse();
				it.seek(idPrefix);
				if (it.hasPrev()) {
					var prev = it.peekPrev();
					if (prev.getKey()[0] == (byte) t.getReverse()) {
						result[t.getIndex()] = Varint.readUnsigned(ByteBuffer.wrap(prev.getKey()), 1) + 1L;
					}
				}
			}
		}

		return result;
	}

	private ReentrantReadWriteLock lockFor(URI uri) {
		return (ReentrantReadWriteLock) locks.get(uri);
	}

	private <T> T writeLock(ReentrantReadWriteLock lock, Supplier<T> block) {
		boolean readLock = false;
		try {
			if (lock.getReadHoldCount() > 0) {
				readLock = true;
				lock.readLock().unlock();
			}
			lock.writeLock().lock();
			return block.get();
		} finally {
			lock.writeLock().unlock();
			if (readLock) lock.readLock().lock();
		}
	}

	private void writeLock(ReentrantReadWriteLock lock, Runnable block) {
		writeLock(lock, () -> {
			block.run();
			return null;
		});
	}

	private byte[] uriKey(byte prefix, URI uri) {
		byte[] uriBytes = uri.toString().getBytes(StandardCharsets.UTF_8);
		// append 0 after the uri to ensure that it is not a prefix of another string
		byte[] key = new byte[uriBytes.length + 2];
		key[0] = prefix;
		System.arraycopy(uriBytes, 0, key, 1, uriBytes.length);
		return key;
	}

	private byte[] idKey(byte prefix, byte[] id) {
		byte[] key = new byte[id.length + 1];
		key[0] = prefix;
		System.arraycopy(id, 0, key, 1, id.length);
		return key;
	}

	/**
	 * Returns the ID of the URI, or -1 if it has none and <code>generate</code> is false.
	 * A generated ID is added to the given ID batch, or written immediately if the batch is
	 * null.
	 */
	long toId(URI uri, EntryType entryType, boolean generate, IdBatch idBatch) {
		var cacheKey = new CacheKey(uri.toString(), entryType.getId());
		Long id = uriToIdCache.getIfPresent(cacheKey);
		if (id != null) {
			return id;
		}
		id = pendingIds.get(cacheKey);
		if (id != null) {
			return id;
		}
		byte[] key = uriKey((byte) entryType.getId(), uri);
		byte[] idBytes = ids.get(key);
		if (idBytes != null) {
			id = Varint.readUnsigned(ByteBuffer.wrap(idBytes));
			uriToIdCache.put(cacheKey, id);
			return id;
		}
		if (!generate) {
			return -1;
		}
		ReentrantReadWriteLock lock = lockFor(uri);
		return writeLock(lock, () -> {
			// another put may have generated the ID in the meantime
			Long pendingId = pendingIds.get(cacheKey);
			if (pendingId != null) {
				return pendingId;
			}
			byte[] idBytesInner = ids.get(key);
			if (idBytesInner != null) {
				long existingId = Varint.readUnsigned(ByteBuffer.wrap(idBytesInner));
				uriToIdCache.put(cacheKey, existingId);
				return existingId;
			}

			long newId = nextId(entryType);
			idBytesInner = new byte[Varint.calcLengthUnsigned(newId)];
			Varint.writeUnsigned(idBytesInner, 0, newId);
			byte[] idKeyBytes = idKey((byte) entryType.getReverse(), idBytesInner);
			byte[] uriBytes = uri.toString().getBytes(StandardCharsets.UTF_8);

			if (idBatch != null) {
				idBatch.add(uri, entryType, cacheKey, newId, key, idBytesInner, idKeyBytes, uriBytes);
			} else {
				// Ensure that writes to the id database are always synced to disk.
				// As ids are subject to fewer changes the pages may only
				// be flushed with large delays to disk which may cause data loss.
				try (WriteBatch batch = ids.createWriteBatch()) {
					// forward and reverse mapping
					batch.put(key, idBytesInner);
					batch.put(idKeyBytes, uriBytes);
					ids.write(batch, new WriteOptions().sync(true));
				} catch (IOException e) {
					throw new UncheckedIOException(e);
				}
				uriToIdCache.put(cacheKey, newId);
			}
			return newId;
		});
	}

	public static byte[] encodeObject(Object valueObject) throws IOException {
		if (valueObject instanceof Record) {
			Record r = (Record) valueObject;
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			// marker for an object
			baos.write('O');
			int size = r.size();
			byte[] sizeBytes = new byte[Varint.calcLengthUnsigned(size)];
			Varint.writeUnsigned(sizeBytes, 0, size);
			baos.write(sizeBytes);
			for (Record element : r) {
				// write the property
				URI p = element.getProperty();
				byte[] content = p.toString().getBytes(StandardCharsets.UTF_8);
				int lengthBytes = Varint.calcLengthUnsigned(content.length);
				byte[] uriBytes = new byte[lengthBytes + content.length];
				Varint.writeUnsigned(uriBytes, 0, content.length);
				System.arraycopy(content, 0, uriBytes, lengthBytes, content.length);
				baos.write(uriBytes);

				// write the value
				baos.write(encodeObject(element.getValue()));
			}
			return baos.toByteArray();
		} else if (valueObject instanceof Object[]) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			baos.write('[');
			byte[] length = new byte[Varint.calcLengthUnsigned(((Object[]) valueObject).length)];
			Varint.writeUnsigned(length, 0, ((Object[]) valueObject).length);
			baos.write(length);
			for (Object v : (Object[]) valueObject) {
				baos.write(encodeObject(v));
			}
			return baos.toByteArray();
		} else if (valueObject instanceof URI) {
			URI uri = (URI) valueObject;
			byte[] content = uri.toString().getBytes(StandardCharsets.UTF_8);
			int lengthBytes = Varint.calcLengthUnsigned(content.length);
			byte[] uriBytes = new byte[1 + lengthBytes + content.length];
			uriBytes[0] = 'R';
			Varint.writeUnsigned(uriBytes, 1, content.length);
			System.arraycopy(content, 0, uriBytes, 1 + lengthBytes, content.length);
			return uriBytes;
		} else {
			return Values.encode(valueObject);
		}
	}

	@Override
	public boolean addListener(KvinListener listener) {
		return listeners.add(listener);
	}

	@Override
	public boolean removeListener(KvinListener listener) {
		return listeners.remove(listener);
	}

	/**
	 * Notifies the listeners of items whose IDs were created. Failures of listeners are
	 * logged, as the values are already stored.
	 */
	private void notifyEntitiesCreated(List<URI> items) {
		for (URI item : items) {
			for (KvinListener listener : listeners) {
				try {
					listener.entityCreated(item);
				} catch (RuntimeException e) {
					log.warn("Listener {} failed for the new item {}", listener, item, e);
				}
			}
		}
	}

	private void notifyValuesAdded(Iterable<KvinTuple> tuples) {
		for (KvinTuple tuple : tuples) {
			for (KvinListener listener : listeners) {
				try {
					listener.valueAdded(tuple.item, tuple.property, tuple.context, tuple.time, tuple.seqNr, tuple.value);
				} catch (RuntimeException e) {
					log.warn("Listener {} failed for a value of {}", listener, tuple.item, e);
				}
			}
		}
	}

	@Override
	public void put(KvinTuple... tuples) {
		put(Arrays.asList(tuples));
	}

	/**
	 * Stores the tuples. If storing a record fails, the put stops and throws the failure.
	 * The records stored before the failure are kept. Puts run one after another, as the
	 * queue of the writer takes records from one put at a time.
	 * <p>
	 * After the tuples are stored, the listeners are notified of the items with new IDs and of
	 * each added value, outside of the lock of the put. After a failure, the listeners are only
	 * notified of the items whose IDs were stored, and of no values.
	 */
	@Override
	public void put(Iterable<KvinTuple> tuples) {
		boolean notify = !listeners.isEmpty();
		// an iterable that is not a collection may only be iterated once
		List<KvinTuple> added = notify && !(tuples instanceof Collection<?>) ? new ArrayList<>() : null;
		List<URI> newItems = new ArrayList<>();
		RuntimeException failure = null;
		try {
			store(tuples, added, newItems);
		} catch (RuntimeException e) {
			failure = e;
		}
		if (notify) {
			notifyEntitiesCreated(newItems);
		}
		if (failure != null) {
			throw failure;
		}
		if (notify) {
			notifyValuesAdded(added != null ? added : tuples);
		}
	}

	/**
	 * Stores the tuples, adds them to <code>added</code> unless it is null, and adds the items
	 * whose new IDs were written to <code>newItems</code>.
	 */
	private synchronized void store(Iterable<KvinTuple> tuples, List<KvinTuple> added, List<URI> newItems) {
		Future<Void> writer = async ? executor.submit(this::writeQueuedRecords) : null;
		Throwable failure = null;
		IdBatch idBatch = new IdBatch();
		try {
			for (KvinTuple tuple : tuples) {
				if (added != null) {
					added.add(tuple);
				}
				KvinRecord record = new KvinRecord(
					toId(tuple.context, EntryType.ContextToId, true, idBatch),
					toId(tuple.item, EntryType.SubjectToId, true, idBatch),
					toId(tuple.property, EntryType.PropertyToId, true, idBatch),
					tuple.time,
					tuple.seqNr,
					encodeObject(tuple.value)
				);

				if (writer != null) {
					if (!enqueue(record, writer)) {
						// the writer failed, its failure is thrown below
						break;
					}
				} else {
					tableFiles.put(record);
				}

				if (idBatch.isFull()) {
					idBatch.write();
				}
			}
		} catch (Throwable e) {
			failure = e;
		}
		// also after a failure, so that the stored records have their URI-ID mappings
		try {
			idBatch.write();
		} catch (Throwable e) {
			failure = addFailure(failure, e);
		}
		newItems.addAll(idBatch.writtenItems);
		try {
			idBatch.close();
		} catch (Throwable e) {
			failure = addFailure(failure, e);
		}
		if (writer != null) {
			enqueue(END_MARKER, writer);
			try {
				Uninterruptibles.getUninterruptibly(writer);
			} catch (ExecutionException e) {
				failure = addFailure(failure, e.getCause());
				// the next put must not write the records that the failed writer left behind
				while (queue.remove() != null) {
					// discard
				}
			}
		}
		try {
			// closes the group of the written records, also after a failure, so that they are kept
			if (syncWrites) {
				// the URI-ID mappings are already persisted
				tableFiles.sync();
			} else {
				tableFiles.commit();
			}
		} catch (IOException | RuntimeException e) {
			failure = addFailure(failure, e);
		}
		if (failure != null) {
			throw unchecked(failure);
		}
	}

	/**
	 * Persists the entries of a delete if writes are synced.
	 */
	private <T> T synced(T deleted) {
		if (syncWrites) {
			try {
				tableFiles.sync();
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}
		return deleted;
	}

	/**
	 * Writes the queued records to the table files until the end marker.
	 */
	private Void writeQueuedRecords() throws IOException {
		while (true) {
			KvinRecord record = queue.remove();
			if (record == null) {
				Thread.onSpinWait();
			} else if (record == END_MARKER) {
				return null;
			} else {
				tableFiles.put(record);
			}
		}
	}

	/**
	 * Adds the record to the queue of the writer, waiting while the queue is full.
	 *
	 * @return false if the writer has stopped, as only a failure stops it before the end marker
	 */
	private boolean enqueue(KvinRecord record, Future<Void> writer) {
		while (!writer.isDone()) {
			if (queue.add(record)) {
				return true;
			}
			Thread.onSpinWait();
		}
		return false;
	}

	private static Throwable addFailure(Throwable failure, Throwable e) {
		if (failure == null) {
			return e;
		}
		if (failure != e) {
			failure.addSuppressed(e);
		}
		return failure;
	}

	private static RuntimeException unchecked(Throwable failure) {
		if (failure instanceof Error error) {
			throw error;
		}
		if (failure instanceof RuntimeException runtimeException) {
			return runtimeException;
		}
		if (failure instanceof IOException ioException) {
			return new UncheckedIOException(ioException);
		}
		return new KvinException(failure);
	}

	/**
	 * Decodes a value written by {@link #encodeObject(Object)}.
	 */
	public static Object decodeObject(ByteBuffer b) throws IOException {
		int start = b.position();
		switch (b.get()) {
			// an object
			case 'O': {
				int size = (int) Varint.readUnsigned(b);
				Record record = Record.NULL;
				for (int i = 0; i < size; i++) {
					URI property = readUri(b);
					record = record.append(new Record(property, decodeObject(b)));
				}
				return record;
			}
			// an array
			case '[': {
				int length = (int) Varint.readUnsigned(b);
				Object[] values = new Object[length];
				for (int i = 0; i < length; i++) {
					values[i] = decodeObject(b);
				}
				return values;
			}
			// a URI reference
			case 'R':
				return readUri(b);
			// a scalar value
			default:
				b.position(start);
				return Values.decode(b);
		}
	}

	private static URI readUri(ByteBuffer b) {
		int length = (int) Varint.readUnsigned(b);
		byte[] content = new byte[length];
		b.get(content);
		return URIs.createURI(new String(content, StandardCharsets.UTF_8));
	}

	private static Object decodeValue(Object value) {
		try {
			return decodeObject(ByteBuffer.wrap((byte[]) value));
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private static URI contextOrDefault(URI context) {
		return context == null ? DEFAULT_CONTEXT : context;
	}

	/**
	 * Returns the URI of the ID, or null if the ID is unknown.
	 */
	URI toUri(long id, EntryType entryType) {
		var cacheKey = new IdCacheKey(id, entryType.getId());
		URI uri = idToUriCache.getIfPresent(cacheKey);
		if (uri == null) {
			byte[] idBytes = new byte[Varint.calcLengthUnsigned(id)];
			Varint.writeUnsigned(idBytes, 0, id);
			byte[] uriBytes = ids.get(idKey((byte) entryType.getReverse(), idBytes));
			if (uriBytes == null) {
				return null;
			}
			uri = URIs.createURI(new String(uriBytes, StandardCharsets.UTF_8));
			idToUriCache.put(cacheKey, uri);
		}
		return uri;
	}

	@Override
	public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long limit) {
		return fetch(item, property, context, KvinTuple.TIME_MAX_VALUE, 0L, limit, 0L, null);
	}

	/**
	 * Returns the number of bytes written to the table files, which grows with the number of
	 * values and with the memory of their index.
	 */
	public long sizeInBytes() {
		return tableFiles.length();
	}

	/**
	 * Returns all values of the store, ordered by the IDs of context, item and property, and
	 * then by descending time and sequence number.
	 */
	public IExtendedIterator<KvinTuple> fetchAll() {
		return tuples(tableFiles.fetchAll());
	}

	/**
	 * Returns the newest value of each series of context, item and property, in the order of
	 * {@link #fetchAll()}.
	 */
	public IExtendedIterator<KvinTuple> fetchLatest() {
		return tuples(tableFiles.fetchLatest());
	}

	private IExtendedIterator<KvinTuple> tuples(IExtendedIterator<KvinRecord> records) {
		return new NiceIterator<>() {
			@Override
			public boolean hasNext() {
				return records.hasNext();
			}

			@Override
			public KvinTuple next() {
				KvinRecord r = records.next();
				return new KvinTuple(uri(r.contextId(), EntryType.ContextToId), uri(r.itemId(), EntryType.SubjectToId),
						uri(r.propertyId(), EntryType.PropertyToId), r.time(), r.seqNr(), decodeValue(r.value()));
			}

			@Override
			public void close() {
				records.close();
			}
		};
	}

	private URI uri(long id, EntryType entryType) {
		URI uri = toUri(id, entryType);
		if (uri == null) {
			throw new KvinException("Unknown " + entryType + " ID: " + id);
		}
		return uri;
	}

	/**
	 * Fetches the values of the item, ordered by property URI and then by descending time
	 * and sequence number. The limit applies to each property, 0 means no limit.
	 */
	@Override
	public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long end, long begin, long limit,
	                                          long interval, String op) {
		IExtendedIterator<KvinTuple> results = fetchInternal(item, property, context, end, begin,
				op == null ? limit : 0L, op == null ? interval : 0L);
		if (op != null) {
			results = new AggregatingIterator<>(results, interval, op.trim().toLowerCase(), limit) {
				@Override
				protected KvinTuple createElement(URI item, URI property, URI context, long time, int seqNr,
				                                  Object value) {
					return new KvinTuple(context, item, property, time, seqNr, value);
				}
			};
		}
		return results;
	}

	private IExtendedIterator<KvinTuple> fetchInternal(URI item, URI property, URI context, long end, long begin,
	                                                   long limit, long interval) {
		URI currentContext = contextOrDefault(context);
		long contextId = toId(currentContext, EntryType.ContextToId, false, null);
		long itemId = toId(item, EntryType.SubjectToId, false, null);
		if (contextId < 0 || itemId < 0) {
			return NiceIterator.emptyIterator();
		}
		Iterator<URI> properties = (property == null ? properties(itemId, contextId) : List.of(property)).iterator();
		long recordLimit = limit == 0 ? Long.MAX_VALUE : limit;
		return new NiceIterator<>() {
			URI currentProperty;
			Iterator<KvinRecord> records = Collections.emptyIterator();

			@Override
			public boolean hasNext() {
				while (!records.hasNext() && properties.hasNext()) {
					currentProperty = properties.next();
					long propertyId = toId(currentProperty, EntryType.PropertyToId, false, null);
					if (propertyId >= 0) {
						try (var it = tableFiles.fetch(itemId, propertyId, contextId, end, begin, recordLimit,
								interval, null)) {
							records = it.toList().iterator();
						}
					}
				}
				return records.hasNext();
			}

			@Override
			public KvinTuple next() {
				if (!hasNext()) {
					throw new NoSuchElementException();
				}
				KvinRecord r = records.next();
				return new KvinTuple(currentContext, item, currentProperty, r.time(), r.seqNr(), decodeValue(r.value()));
			}
		};
	}

	/**
	 * Deletes the values of the item and property in the time range [begin, end], or of all
	 * properties of the item if the property is null. Each table file records the delete as
	 * a tombstone, so it also applies after the files are opened again.
	 */
	@Override
	public long delete(URI item, URI property, URI context, long end, long begin) {
		long contextId = toId(contextOrDefault(context), EntryType.ContextToId, false, null);
		long itemId = toId(item, EntryType.SubjectToId, false, null);
		if (contextId < 0 || itemId < 0) {
			return 0;
		}
		Long propertyId = null;
		if (property != null) {
			propertyId = toId(property, EntryType.PropertyToId, false, null);
			if (propertyId < 0) {
				return 0;
			}
		}
		return synced(tableFiles.delete(itemId, propertyId, contextId, end, begin));
	}

	/**
	 * Deletes all values of the item in the context. The IDs are kept, so the item can be
	 * used again afterwards.
	 */
	@Override
	public boolean delete(URI item, URI context) {
		long contextId = toId(contextOrDefault(context), EntryType.ContextToId, false, null);
		long itemId = toId(item, EntryType.SubjectToId, false, null);
		return contextId >= 0 && itemId >= 0 && synced(tableFiles.delete(itemId, contextId));
	}

	/**
	 * Deletes the values of the context in all table files. Each table file records
	 * the delete as a tombstone, so it also applies after the files are opened again.
	 */
	@Override
	public long delete(URI context) {
		long contextId = toId(contextOrDefault(context), EntryType.ContextToId, false, null);
		return contextId < 0 ? 0 : synced(tableFiles.delete(contextId));
	}

	@Override
	public IExtendedIterator<URI> descendants(URI item, URI context) {
		return descendants(item, context, Long.MAX_VALUE);
	}

	/**
	 * Returns the items whose URI starts with the URI of the given item, including the item
	 * itself, and that have values in the context. The items are ordered by URI.
	 */
	@Override
	public IExtendedIterator<URI> descendants(URI item, URI context, long limit) {
		long contextId = toId(contextOrDefault(context), EntryType.ContextToId, false, null);
		if (contextId < 0 || limit <= 0) {
			return NiceIterator.emptyIterator();
		}
		byte[] uriBytes = item.toString().getBytes(StandardCharsets.UTF_8);
		byte[] prefix = new byte[uriBytes.length + 1];
		prefix[0] = (byte) EntryType.SubjectToId.getId();
		System.arraycopy(uriBytes, 0, prefix, 1, uriBytes.length);
		DBIterator it = ids.iterator();
		it.seek(prefix);
		return new NiceIterator<>() {
			URI next;
			long count;
			boolean closed;

			@Override
			public boolean hasNext() {
				if (next != null) {
					return true;
				}
				while (!closed && count < limit && it.hasNext()) {
					var entry = it.next();
					byte[] key = entry.getKey();
					if (key.length < prefix.length
							|| !Arrays.equals(key, 0, prefix.length, prefix, 0, prefix.length)) {
						break;
					}
					long itemId = Varint.readUnsigned(ByteBuffer.wrap(entry.getValue()));
					if (!properties(itemId, contextId).isEmpty()) {
						count++;
						// the key ends with a 0 byte after the URI
						next = URIs.createURI(new String(key, 1, key.length - 2, StandardCharsets.UTF_8));
						return true;
					}
				}
				close();
				return false;
			}

			@Override
			public URI next() {
				if (!hasNext()) {
					throw new NoSuchElementException();
				}
				URI result = next;
				next = null;
				return result;
			}

			@Override
			public void close() {
				if (!closed) {
					closed = true;
					it.close();
				}
			}
		};
	}

	/**
	 * Returns the properties of the item that have values in the context, ordered by URI.
	 */
	@Override
	public IExtendedIterator<URI> properties(URI item, URI context) {
		long contextId = toId(contextOrDefault(context), EntryType.ContextToId, false, null);
		long itemId = toId(item, EntryType.SubjectToId, false, null);
		if (contextId < 0 || itemId < 0) {
			return NiceIterator.emptyIterator();
		}
		return WrappedIterator.create(properties(itemId, contextId).iterator());
	}

	private List<URI> properties(long itemId, long contextId) {
		List<URI> properties = new ArrayList<>();
		try (IExtendedIterator<Long> propertyIds = tableFiles.properties(itemId, contextId)) {
			while (propertyIds.hasNext()) {
				URI property = toUri(propertyIds.next(), EntryType.PropertyToId);
				if (property != null) {
					properties.add(property);
				}
			}
		}
		properties.sort(Comparator.comparing(URI::toString));
		return properties;
	}

	@Override
	public void close() {
		try {
			tableFiles.close();
			ids.close();
			executor.shutdown();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
