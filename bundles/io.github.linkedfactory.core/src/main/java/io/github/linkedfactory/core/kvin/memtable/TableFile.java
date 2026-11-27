package io.github.linkedfactory.core.kvin.memtable;

import io.github.linkedfactory.core.kvin.records.KvinRecord;
import io.github.linkedfactory.core.kvin.records.KvinRecordStore;
import io.github.linkedfactory.core.kvin.util.Varint;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.commons.iterator.WrappedIterator;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.CRC32C;

/**
 * An append-only file of KVIN records with an in-memory index.
 * <p>
 * Each entry starts with the length of its payload, followed by the payload. The payload starts
 * with a kind field. The kinds 0 to 3 are value records, where the kind is the number of leading
 * IDs shared with the previous value record. The kinds 4 to 6 are tombstones of deletes. When an
 * existing file is opened, the entries are replayed in order, so a tombstone removes only the
 * records written before it.
 * <p>
 * The entries form groups, each closed by a commit entry with the CRC32C checksum of the
 * group's bytes, see {@link #commit()}. A group ends with each delete, after
 * {@link #MAX_GROUP_BYTES}, on {@link #sync()} and on {@link #close()}. Replaying applies the
 * entries of a group only if its checksum matches, and stops at the first group that does not
 * match or is not closed, as after a crash of the operating system that persisted only some of
 * the written pages. The rest of the file is then cleared, so that entries after it are not
 * read again later. Written entries are only persisted by {@link #sync()}, or by the operating
 * system.
 */
public class TableFile implements KvinRecordStore {
	static long LENGTH = 5L * 1024 * 1024;

	/**
	 * Tombstone of a context: kind, context ID.
	 */
	static final int CONTEXT_TOMBSTONE = 4;
	/**
	 * Tombstone of an item in a context: kind, context ID, item ID.
	 */
	static final int ITEM_TOMBSTONE = 5;
	/**
	 * Tombstone of a time range: kind, context ID, item ID, property ID + 1 (0 for all properties),
	 * end and begin as 8-byte values, as times may be negative.
	 */
	static final int RANGE_TOMBSTONE = 6;

	/**
	 * Commit entry that closes a group: kind, CRC32C checksum of the bytes of the group as a
	 * 4-byte value.
	 */
	static final int COMMIT = 7;

	/**
	 * Number of bytes after which a group is closed, so that a damaged byte drops at most the
	 * entries of one group of this size, or of one put.
	 */
	static int MAX_GROUP_BYTES = 64 * 1024;

	Path path;
	RandomAccessFile rafile;
	FileChannel channel;
	MappedByteBuffer buffer;
	// every mapping of the file, as growing the file maps it again; unmapped on close
	private final List<MappedByteBuffer> mappings = new ArrayList<>();
	// read while fetches read the mappings, written by close, so that no fetch reads an unmapped buffer
	private final ReentrantReadWriteLock mappingLock = new ReentrantReadWriteLock();
	private volatile boolean closed;
	// the position up to which the entries are persisted by sync
	private int syncedPosition;
	// whether the length of the file is persisted, which changes when the file is created or grows
	private boolean lengthSynced;
	// the start of the group that the next commit entry closes
	private int groupStart;
	// whether the constructor completed, so that closing commits the written entries
	private boolean opened;
	// sorted by context first, so that all items of a context form one range
	final ConcurrentSkipListMap<ItemWithContext, List<ItemData>> items = new ConcurrentSkipListMap<>();

	boolean hasPreviousIds = false;
	long previousItemId;
	long previousContextId;
	long previousPropertyId;

	public TableFile(Path path) throws IOException {
		this.path = path;
		boolean readFile = Files.exists(path);
		rafile = new RandomAccessFile(path.toString(), "rw");
		channel = rafile.getChannel();
		long mappedLength = readFile ? Math.max(channel.size(), LENGTH) : LENGTH;
		try {
			buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, mappedLength);
			mappings.add(buffer);
			if (readFile && channel.size() > 0) {
				reconstructItems(channel.size());
			}
			opened = true;
		} catch (IOException | RuntimeException e) {
			close();
			throw e;
		}
	}

	private void ensureOpen() {
		if (closed) {
			throw new IllegalStateException("Table file is closed: " + path);
		}
	}

	@Override
	public void put(KvinRecord... records) {
		put(Arrays.asList(records));
	}

	@Override
	public void put(Iterable<KvinRecord> records) {
		for (KvinRecord record : records) {
			try {
				put(record);
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}
	}

	synchronized void put(KvinRecord record) throws IOException {
		ensureOpen();
		ItemWithContext icp = new ItemWithContext(record.itemId(), record.contextId());

		int itemLength = compressedIdsLength(record.itemId(), record.contextId(), record.propertyId());
		int keyLength = Varint.calcLengthUnsigned(record.time())
				+ Varint.calcLengthUnsigned(record.seqNr());
		int valueLength = ((byte[]) record.value()).length;

		ensureCapacity(Varint.MAX_BYTES + itemLength + keyLength + valueLength);

		int recordStart = buffer.position();
		Varint.writeUnsigned(buffer, itemLength + keyLength + valueLength);
		writeCompressedIds(record.itemId(), record.contextId(), record.propertyId());
		// write key (time, seqNr) directly, no temporary buffer
		Varint.writeUnsigned(buffer, record.time());
		Varint.writeUnsigned(buffer, record.seqNr());
		buffer.put((byte[]) record.value());

		hasPreviousIds = true;
		previousItemId = record.itemId();
		previousContextId = record.contextId();
		previousPropertyId = record.propertyId();

		addRange(icp, record.propertyId(), recordStart);
		if (buffer.position() - groupStart >= MAX_GROUP_BYTES) {
			commit();
		}
	}

	/**
	 * Closes the group of the entries written since the last commit with a commit entry. Does
	 * nothing if no entries were written.
	 */
	public synchronized void commit() throws IOException {
		ensureOpen();
		int groupEnd = buffer.position();
		if (groupEnd == groupStart) {
			return;
		}
		int crc = checksum(buffer, groupStart, groupEnd);
		int length = Varint.calcLengthUnsigned(COMMIT) + Integer.BYTES;
		ensureCapacity(Varint.MAX_BYTES + length);
		Varint.writeUnsigned(buffer, length);
		Varint.writeUnsigned(buffer, COMMIT);
		buffer.putInt(crc);
		groupStart = buffer.position();
	}

	/**
	 * Grows the mapping of the file if fewer than the given number of bytes are left.
	 */
	private void ensureCapacity(int required) throws IOException {
		if (buffer.remaining() < required) {
			int position = buffer.position();
			long capacity = buffer.capacity();
			while (capacity - position < required) {
				capacity *= 2;
			}
			buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, capacity);
			// fetches may still read the previous mapping, so it is only unmapped on close
			mappings.add(buffer);
			lengthSynced = false;
			// a new mapping starts at position 0
			buffer.position(position);
		}
	}

	private void writeTombstone(int kind, long contextId, long itemId, long propertyField, long end, long begin) {
		int length = Varint.calcLengthUnsigned(kind) + Varint.calcLengthUnsigned(contextId);
		if (kind != CONTEXT_TOMBSTONE) {
			length += Varint.calcLengthUnsigned(itemId);
		}
		if (kind == RANGE_TOMBSTONE) {
			length += Varint.calcLengthUnsigned(propertyField) + 2 * Long.BYTES;
		}
		try {
			// closes the group of the preceding records, as a delete is a group of its own
			commit();
			ensureCapacity(Varint.MAX_BYTES + length);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		Varint.writeUnsigned(buffer, length);
		Varint.writeUnsigned(buffer, kind);
		Varint.writeUnsigned(buffer, contextId);
		if (kind != CONTEXT_TOMBSTONE) {
			Varint.writeUnsigned(buffer, itemId);
		}
		if (kind == RANGE_TOMBSTONE) {
			Varint.writeUnsigned(buffer, propertyField);
			buffer.putLong(end);
			buffer.putLong(begin);
		}
		try {
			commit();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		// tombstones do not change the IDs that the next value record is compressed against
	}

	@Override
	public IExtendedIterator<KvinRecord> fetch(long itemId, Long propertyId, Long contextId, long limit) {
		return fetch(itemId, propertyId, contextId, Long.MAX_VALUE, 0, limit, 0, null);
	}

	@Override
	public IExtendedIterator<KvinRecord> fetch(long itemId, Long propertyId, Long contextId, long end, long begin,
	                                           long limit, long interval, String op) {
		mappingLock.readLock().lock();
		try {
			ensureOpen();
			return fetchMapped(itemId, propertyId, contextId, end, begin, limit, interval);
		} finally {
			mappingLock.readLock().unlock();
		}
	}

	private IExtendedIterator<KvinRecord> fetchMapped(long itemId, Long propertyId, Long contextId, long end, long begin,
	                                                  long limit, long interval) {
		if (limit <= 0) {
			return NiceIterator.emptyIterator();
		}
		long ctx = contextOrDefault(contextId);
		var itemDataList = items.get(new ItemWithContext(itemId, ctx));
		if (itemDataList == null || itemDataList.isEmpty()) {
			return NiceIterator.emptyIterator();
		}
		List<ItemData> selected = getSelectedItemData(itemDataList, propertyId);
		if (selected.isEmpty()) {
			return NiceIterator.emptyIterator();
		}

		List<KvinRecord> result = new ArrayList<>();
		long remaining = limit;
		long nextTimeThreshold = end;

		for (ItemData itemData : selected) {
			OffsetList ranges = getSortedRanges(itemData);
			for (int i = 0; i < ranges.size; i++) {
				int recordStart = ranges.a[i];
				long time = readTime(recordStart);
				if (time > end || time < begin) {
					continue;
				}
				if (interval > 0 && time > nextTimeThreshold) {
					continue;
				}
				int seqNr = readSeqNr(recordStart);
				byte[] value = readValue(recordStart);
				result.add(new KvinRecord(ctx, itemId, itemData.propertyId, time, seqNr, value));
				if (interval > 0) {
					nextTimeThreshold = time - interval;
				}
				if (--remaining == 0) {
					return WrappedIterator.create(result.iterator());
				}
			}
		}
		return WrappedIterator.create(result.iterator());
	}

	@Override
	public synchronized long delete(long itemId, Long propertyId, Long contextId, long end, long begin) {
		ensureOpen();
		long ctx = contextOrDefault(contextId);
		Long property = propertyId == null || propertyId == -1 ? null : propertyId;
		if (!items.containsKey(new ItemWithContext(itemId, ctx))) {
			return 0;
		}
		writeTombstone(RANGE_TOMBSTONE, ctx, itemId, property == null ? 0 : property + 1, end, begin);
		return removeRange(itemId, property, ctx, end, begin);
	}

	@Override
	public synchronized boolean delete(long itemId, Long contextId) {
		ensureOpen();
		long ctx = contextOrDefault(contextId);
		if (!items.containsKey(new ItemWithContext(itemId, ctx))) {
			return false;
		}
		writeTombstone(ITEM_TOMBSTONE, ctx, itemId, 0, 0, 0);
		return items.remove(new ItemWithContext(itemId, ctx)) != null;
	}

	/**
	 * Deletes all records of the context: appends a tombstone and removes the context's range of items from the index.
	 */
	@Override
	public synchronized long delete(Long contextId) {
		ensureOpen();
		long ctx = contextOrDefault(contextId);
		if (contextRange(ctx).isEmpty()) {
			return 0;
		}
		writeTombstone(CONTEXT_TOMBSTONE, ctx, 0, 0, 0, 0);
		return removeContext(ctx);
	}

	private NavigableMap<ItemWithContext, List<ItemData>> contextRange(long contextId) {
		return items.subMap(new ItemWithContext(Long.MIN_VALUE, contextId), true,
				new ItemWithContext(Long.MAX_VALUE, contextId), true);
	}

	private long removeContext(long contextId) {
		long deleted = 0;
		for (var entry : contextRange(contextId).entrySet()) {
			List<ItemData> itemDataList = entry.getValue();
			synchronized (itemDataList) {
				if (items.remove(entry.getKey(), itemDataList)) {
					for (ItemData itemData : itemDataList) {
						deleted += size(itemData);
					}
				}
			}
		}
		return deleted;
	}

	private long removeRange(long itemId, Long propertyId, long contextId, long end, long begin) {
		var key = new ItemWithContext(itemId, contextId);
		var itemDataList = items.get(key);
		if (itemDataList == null || itemDataList.isEmpty()) {
			return 0;
		}

		List<ItemData> selected = getSelectedItemData(itemDataList, propertyId);
		if (selected.isEmpty()) {
			return 0;
		}

		long deleted = 0;
		synchronized (itemDataList) {
			Iterator<ItemData> it = itemDataList.iterator();
			while (it.hasNext()) {
				ItemData itemData = it.next();
				if (!selected.contains(itemData)) {
					continue;
				}
				deleted += removeByRange(itemData, begin, end);
				if (isEmpty(itemData)) {
					it.remove();
				}
			}
			if (itemDataList.isEmpty()) {
				items.remove(key, itemDataList);
			}
		}
		return deleted;
	}

	/**
	 * Applies a tombstone while the file is read.
	 *
	 * @return <code>false</code> if the tombstone is incomplete or has an unknown kind
	 */
	/**
	 * A tombstone read from the file, applied when its group is complete.
	 */
	private record Tombstone(int kind, long contextId, long itemId, long propertyField, long end, long begin) {
		void apply(TableFile file) {
			switch (kind) {
				case CONTEXT_TOMBSTONE -> file.removeContext(contextId);
				case ITEM_TOMBSTONE -> file.items.remove(new ItemWithContext(itemId, contextId));
				default -> file.removeRange(itemId, propertyField == 0 ? null : propertyField - 1, contextId, end, begin);
			}
		}
	}

	/**
	 * A value record read from the file, added to the index when its group is complete.
	 */
	private record PendingRecord(ItemWithContext key, long propertyId, int recordStart) {
	}

	/**
	 * Reads the tombstone after its kind, or returns null if it is invalid.
	 */
	private static Tombstone readTombstone(ByteBuffer bb, int kind, int payloadEnd) {
		long contextId = Varint.readUnsigned(bb);
		if (kind == CONTEXT_TOMBSTONE) {
			return bb.position() > payloadEnd ? null : new Tombstone(kind, contextId, 0, 0, 0, 0);
		}
		long itemId = Varint.readUnsigned(bb);
		if (kind == ITEM_TOMBSTONE) {
			return bb.position() > payloadEnd ? null : new Tombstone(kind, contextId, itemId, 0, 0, 0);
		}
		long propertyField = Varint.readUnsigned(bb);
		if (bb.position() + 2 * Long.BYTES > payloadEnd) {
			return null;
		}
		return new Tombstone(kind, contextId, itemId, propertyField, bb.getLong(), bb.getLong());
	}


	@Override
	public IExtendedIterator<Long> properties(long itemId, Long contextId) {
		var itemDataList = items.get(new ItemWithContext(itemId, contextOrDefault(contextId)));
		if (itemDataList == null || itemDataList.isEmpty()) {
			return NiceIterator.emptyIterator();
		}
		List<Long> properties = new ArrayList<>();
		synchronized (itemDataList) {
			for (ItemData itemData : itemDataList) {
				if (!isEmpty(itemData)) {
					properties.add(itemData.propertyId);
				}
			}
		}
		return WrappedIterator.create(properties.iterator());
	}

	int compressedIdsLength(long itemId, long contextId, long propertyId) {
		int sharedPrefix = sharedPrefix(itemId, contextId, propertyId);
		int length = Varint.calcLengthUnsigned(sharedPrefix);
		if (sharedPrefix < 1) length += Varint.calcLengthUnsigned(itemId);
		if (sharedPrefix < 2) length += Varint.calcLengthUnsigned(contextId);
		if (sharedPrefix < 3) length += Varint.calcLengthUnsigned(propertyId);
		return length;
	}

	void writeCompressedIds(long itemId, long contextId, long propertyId) {
		int sharedPrefix = sharedPrefix(itemId, contextId, propertyId);
		Varint.writeUnsigned(buffer, sharedPrefix);
		if (sharedPrefix < 1) Varint.writeUnsigned(buffer, itemId);
		if (sharedPrefix < 2) Varint.writeUnsigned(buffer, contextId);
		if (sharedPrefix < 3) Varint.writeUnsigned(buffer, propertyId);
	}

	int sharedPrefix(long itemId, long contextId, long propertyId) {
		if (!hasPreviousIds || itemId != previousItemId) return 0;
		if (contextId != previousContextId) return 1;
		if (propertyId != previousPropertyId) return 2;
		return 3;
	}

	public int findIndex(List<ItemData> itemData, long propertyId) {
		int low = 0;
		int high = itemData.size() - 1;
		while (low <= high) {
			int mid = low + ((high - low) / 2);
			long p = itemData.get(mid).propertyId;
			if (p < propertyId) {
				low = mid + 1;
			} else if (p > propertyId) {
				high = mid - 1;
			} else {
				return mid;
			}
		}
		return -(low + 1);
	}

	/**
	 * Closes the file and unmaps it, if supported, see {@link BufferUnmapper}. Waits for
	 * running fetches, and later reads and writes fail.
	 */
	@Override
	public synchronized void close() {
		mappingLock.writeLock().lock();
		try {
			if (closed) {
				return;
			}
			if (opened) {
				// the written entries are complete, so they are kept when the file is opened again
				commit();
			}
			closed = true;
			for (MappedByteBuffer mapping : mappings) {
				BufferUnmapper.unmap(mapping);
			}
			mappings.clear();
			rafile.close();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		} finally {
			mappingLock.writeLock().unlock();
		}
	}

	public int size() {
		return buffer.capacity();
	}

	/**
	 * Returns the number of bytes written to the file, which is less than the mapped {@link #size()}.
	 */
	public synchronized long length() {
		return buffer.position();
	}

	long contextOrDefault(Long contextId) {
		return contextId != null ? contextId : DEFAULT_CONTEXT;
	}

	List<ItemData> getSelectedItemData(List<ItemData> itemDataList, Long propertyId) {
		if (propertyId == null) {
			synchronized (itemDataList) {
				return new ArrayList<>(itemDataList);
			}
		}
		int index = findIndex(itemDataList, propertyId);
		if (index < 0) {
			return Collections.emptyList();
		}
		return Collections.singletonList(itemDataList.get(index));
	}

	OffsetList getSortedRanges(ItemData itemData) {
		synchronized (itemData.unsorted) {
			if (!itemData.unsorted.isEmpty()) {
				OffsetList merged = new OffsetList(
						(itemData.sorted != null ? itemData.sorted.size : 0) + itemData.unsorted.size);
				if (itemData.sorted != null) {
					merged.addAll(itemData.sorted);
				}
				merged.addAll(itemData.unsorted);
				itemData.unsorted.clear();
				sortOffsets(merged);
				itemData.sorted = merged;
			}
			return itemData.sorted != null ? itemData.sorted : OffsetList.EMPTY;
		}
	}

	long size(ItemData itemData) {
		synchronized (itemData.unsorted) {
			OffsetList sorted = itemData.sorted;
			return (sorted != null ? sorted.size : 0) + itemData.unsorted.size;
		}
	}

	boolean isEmpty(ItemData itemData) {
		if (itemData.sorted != null && !itemData.sorted.isEmpty()) {
			return false;
		}
		synchronized (itemData.unsorted) {
			return itemData.unsorted.isEmpty();
		}
	}

	// ---- primitive-offset sort (descending time, then descending seqNr) ----

	void sortOffsets(OffsetList list) {
		quicksort(list.a, 0, list.size - 1);
	}

	private void quicksort(int[] a, int lo, int hi) {
		while (lo < hi) {
			int pivot = a[lo + (hi - lo) / 2];
			int i = lo, j = hi;
			while (i <= j) {
				while (compareOffsets(a[i], pivot) < 0) i++;
				while (compareOffsets(a[j], pivot) > 0) j--;
				if (i <= j) {
					int t = a[i];
					a[i] = a[j];
					a[j] = t;
					i++;
					j--;
				}
			}
			if (j - lo < hi - i) {
				quicksort(a, lo, j);
				lo = i;
			} else {
				quicksort(a, i, hi);
				hi = j;
			}
		}
	}

	int compareOffsets(int a, int b) {
		long timeA = readTime(a);
		long timeB = readTime(b);
		if (timeA != timeB) {
			return Long.compare(timeB, timeA);
		}
		return Integer.compare(readSeqNr(b), readSeqNr(a));
	}

	// ---- record navigation from a single start offset ----

	/**
	 * The CRC32C checksum of the bytes from start to end.
	 */
	static int checksum(ByteBuffer buffer, int start, int end) {
		CRC32C crc = new CRC32C();
		crc.update(buffer.duplicate().limit(end).position(start));
		return (int) crc.getValue();
	}

	/**
	 * Closes the current group and persists the entries written since the last sync, and the
	 * length of the file if it changed.
	 */
	public synchronized void sync() throws IOException {
		ensureOpen();
		commit();
		int position = buffer.position();
		if (position > syncedPosition) {
			buffer.force(syncedPosition, position - syncedPosition);
		}
		if (!lengthSynced) {
			channel.force(true);
			lengthSynced = true;
		}
		syncedPosition = position;
	}

	/**
	 * The number of written bytes that no commit entry closes yet, for tests.
	 */
	synchronized int uncommittedBytes() {
		return buffer.position() - groupStart;
	}

	/**
	 * The number of written bytes that are not persisted by {@link #sync()}, for tests.
	 */
	synchronized int unsyncedBytes() {
		return lengthSynced ? buffer.position() - syncedPosition : buffer.position();
	}

	/**
	 * Clears the bytes after the last valid entry if any of them is set, so that entries after
	 * an incomplete one are not read when the file is opened again after new entries.
	 */
	private void clearAfter(int position, long dataLength) {
		int end = (int) Math.min(dataLength, buffer.capacity());
		int set = -1;
		for (int i = position; i < end; i++) {
			if (buffer.get(i) != 0) {
				set = i;
				break;
			}
		}
		if (set < 0) {
			return;
		}
		for (int i = set; i < end; i++) {
			buffer.put(i, (byte) 0);
		}
		buffer.force(set, end - set);
	}

	private int payloadStart(int recordStart) {
		return recordStart + Varint.firstToLength(buffer.get(recordStart));
	}

	private int payloadEnd(int recordStart) {
		int p = payloadStart(recordStart);
		long len = Varint.readUnsigned(buffer, recordStart);
		return p + (int) len;
	}

	private int keyStart(int recordStart) {
		int p = payloadStart(recordStart);
		long sharedPrefix = Varint.readUnsigned(buffer, p);
		p += Varint.firstToLength(buffer.get(p));
		if (sharedPrefix < 1) p += Varint.firstToLength(buffer.get(p));
		if (sharedPrefix < 2) p += Varint.firstToLength(buffer.get(p));
		if (sharedPrefix < 3) p += Varint.firstToLength(buffer.get(p));
		return p;
	}

	long readTime(int recordStart) {
		return Varint.readUnsigned(buffer, keyStart(recordStart));
	}

	int readSeqNr(int recordStart) {
		int ks = keyStart(recordStart);
		return (int) Varint.readUnsigned(buffer, ks + Varint.firstToLength(buffer.get(ks)));
	}

	byte[] readValue(int recordStart) {
		int ks = keyStart(recordStart);
		int seqPos = ks + Varint.firstToLength(buffer.get(ks));
		int valueStart = seqPos + Varint.firstToLength(buffer.get(seqPos));
		int valueLength = payloadEnd(recordStart) - valueStart;
		byte[] value = new byte[valueLength];
		buffer.get(valueStart, value);
		return value;
	}

	long removeByRange(ItemData itemData, long begin, long end) {
		long removed = 0;

		synchronized (itemData.unsorted) {
			OffsetList kept = new OffsetList(itemData.unsorted.size);
			for (int i = 0; i < itemData.unsorted.size; i++) {
				int rs = itemData.unsorted.a[i];
				long time = readTime(rs);
				if (time >= begin && time <= end) {
					removed++;
				} else {
					kept.add(rs);
				}
			}
			itemData.unsorted.replaceWith(kept);
		}

		if (itemData.sorted != null && !itemData.sorted.isEmpty()) {
			OffsetList remaining = new OffsetList(itemData.sorted.size);
			for (int i = 0; i < itemData.sorted.size; i++) {
				int rs = itemData.sorted.a[i];
				long time = readTime(rs);
				if (time >= begin && time <= end) {
					removed++;
				} else {
					remaining.add(rs);
				}
			}
			itemData.sorted = remaining;
		}
		return removed;
	}

	/**
	 * Replays the complete groups of the file and clears the bytes after them.
	 */
	void reconstructItems(long dataLength) {
		ByteBuffer bb = buffer.duplicate();
		bb.position(0);
		// the end of the last complete group, where new entries are appended
		int validEnd = 0;
		// the entries of the current group, and the IDs that its records are compressed against
		List<Object> pending = new ArrayList<>();
		long itemIdState = previousItemId, contextIdState = previousContextId, propertyIdState = previousPropertyId;
		boolean hasIdsState = hasPreviousIds;

		while (bb.position() < dataLength) {
			if (bb.get(bb.position()) == 0) {
				break;
			}
			int entryStart = bb.position();
			try {
				long lengthValue = Varint.readUnsigned(bb);
				if (lengthValue <= 0 || lengthValue > Integer.MAX_VALUE) {
					break;
				}
				int payloadEnd = bb.position() + (int) lengthValue;
				if (payloadEnd < bb.position() || payloadEnd > dataLength) {
					break;
				}
				long kind = Varint.readUnsigned(bb);
				if (kind == COMMIT) {
					if (bb.position() + Integer.BYTES != payloadEnd
							|| bb.getInt() != checksum(bb, validEnd, entryStart)) {
						break;
					}
					for (Object entry : pending) {
						if (entry instanceof PendingRecord r) {
							addRange(r.key(), r.propertyId(), r.recordStart());
						} else {
							((Tombstone) entry).apply(this);
						}
					}
					pending.clear();
					previousItemId = itemIdState;
					previousContextId = contextIdState;
					previousPropertyId = propertyIdState;
					hasPreviousIds = hasIdsState;
					validEnd = payloadEnd;
				} else if (kind > 3) {
					Tombstone tombstone = kind > RANGE_TOMBSTONE ? null : readTombstone(bb, (int) kind, payloadEnd);
					if (tombstone == null) {
						break;
					}
					pending.add(tombstone);
				} else {
					int sharedPrefix = (int) kind;
					long itemId = sharedPrefix < 1 ? Varint.readUnsigned(bb) : itemIdState;
					long contextId = sharedPrefix < 2 ? Varint.readUnsigned(bb) : contextIdState;
					long propertyId = sharedPrefix < 3 ? Varint.readUnsigned(bb) : propertyIdState;
					// validate that the key can be parsed
					Varint.readUnsigned(bb);
					Varint.readUnsigned(bb);
					if (bb.position() > payloadEnd) {
						break;
					}
					pending.add(new PendingRecord(new ItemWithContext(itemId, contextId), propertyId, entryStart));
					hasIdsState = true;
					itemIdState = itemId;
					contextIdState = contextId;
					propertyIdState = propertyId;
				}
				bb.position(payloadEnd);
			} catch (RuntimeException e) {
				break;
			}
		}
		buffer.position(validEnd);
		groupStart = validEnd;
		clearAfter(validEnd, dataLength);
	}


	void addRange(ItemWithContext icp, long propertyId, int recordStart) {
		var itemDataList = items.computeIfAbsent(icp, k -> new ArrayList<>());
		synchronized (itemDataList) {
			ItemData itemData;
			if (itemDataList.isEmpty()) {
				itemData = new ItemData(propertyId);
				itemDataList.add(itemData);
			} else {
				int index = findIndex(itemDataList, propertyId);
				if (index >= 0) {
					itemData = itemDataList.get(index);
				} else {
					itemData = new ItemData(propertyId);
					itemDataList.add(-index - 1, itemData);
				}
			}
			synchronized (itemData.unsorted) {
				itemData.unsorted.add(recordStart);
			}
		}
	}

	static class ItemWithContext implements Comparable<ItemWithContext> {
		final long itemId;
		final long contextId;

		ItemWithContext(long itemId, long contextId) {
			this.itemId = itemId;
			this.contextId = contextId;
		}

		@Override
		public int compareTo(ItemWithContext o) {
			int c = Long.compare(contextId, o.contextId);
			return c != 0 ? c : Long.compare(itemId, o.itemId);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (!(o instanceof ItemWithContext iwc)) return false;
			return itemId == iwc.itemId && contextId == iwc.contextId;
		}

		@Override
		public int hashCode() {
			long h = itemId * 31 + contextId;
			return Long.hashCode(h);
		}
	}

	public static class ItemData {
		final long propertyId;
		final OffsetList unsorted = new OffsetList();
		volatile OffsetList sorted;

		ItemData(long propertyId) {
			this.propertyId = propertyId;
		}
	}

	/**
	 * Growable primitive int list – stores one record-start offset per record.
	 */
	static final class OffsetList {
		static final OffsetList EMPTY = new OffsetList(0);

		int[] a;
		int size;

		OffsetList() {
			this(8);
		}

		OffsetList(int capacity) {
			a = new int[Math.max(1, capacity)];
		}

		void add(int v) {
			if (size == a.length) {
				a = Arrays.copyOf(a, size + (size >> 1) + 1);
			}
			a[size++] = v;
		}

		void addAll(OffsetList other) {
			for (int i = 0; i < other.size; i++) {
				add(other.a[i]);
			}
		}

		void replaceWith(OffsetList other) {
			this.a = other.a;
			this.size = other.size;
		}

		boolean isEmpty() {
			return size == 0;
		}

		void clear() {
			size = 0;
		}
	}
}