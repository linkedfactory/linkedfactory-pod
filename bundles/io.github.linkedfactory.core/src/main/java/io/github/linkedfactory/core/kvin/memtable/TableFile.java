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

public class TableFile implements KvinRecordStore {
	static long LENGTH = 5L * 1024 * 1024;

	Path path;
	RandomAccessFile rafile;
	FileChannel channel;
	MappedByteBuffer buffer;
	Map<ItemWithContext, List<ItemData>> items = new ConcurrentSkipListMap<>();

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
		buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, mappedLength);
		if (readFile && channel.size() > 0) {
			reconstructItems(channel.size());
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

	void put(KvinRecord record) throws IOException {
		ItemWithContext icp = new ItemWithContext(record.itemId(), record.contextId());

		int itemLength = compressedIdsLength(record.itemId(), record.contextId(), record.propertyId());
		int keyLength = Varint.calcLengthUnsigned(record.time())
				+ Varint.calcLengthUnsigned(record.seqNr());
		int valueLength = ((byte[]) record.value()).length;

		int requiredSpace = Varint.MAX_BYTES + itemLength + keyLength + valueLength;
		if (buffer.remaining() < requiredSpace) {
			buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, buffer.capacity() * 2L);
		}

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
	}

	@Override
	public IExtendedIterator<KvinRecord> fetch(long itemId, Long propertyId, Long contextId, long limit) {
		return fetch(itemId, propertyId, contextId, Long.MAX_VALUE, 0, limit, 0, null);
	}

	@Override
	public IExtendedIterator<KvinRecord> fetch(long itemId, Long propertyId, Long contextId, long end, long begin,
	                                           long limit, long interval, String op) {
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
				result.add(new KvinRecord(itemId, ctx, itemData.propertyId, time, seqNr, value));
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
	public long delete(long itemId, Long propertyId, Long contextId, long end, long begin) {
		var key = new ItemWithContext(itemId, contextOrDefault(contextId));
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

	@Override
	public boolean delete(long itemId, Long contextId) {
		return items.remove(new ItemWithContext(itemId, contextOrDefault(contextId))) != null;
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

	@Override
	public void close() {
		try {
			rafile.close();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public int size() {
		return buffer.capacity();
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

	void reconstructItems(long dataLength) {
		ByteBuffer bb = buffer.duplicate();
		bb.position(0);

		while (bb.position() < dataLength) {
			if (bb.get(bb.position()) == 0) {
				break;
			}
			int recordStart = bb.position();
			long recordLengthValue;
			try {
				recordLengthValue = Varint.readUnsigned(bb);
			} catch (RuntimeException e) {
				break;
			}
			if (recordLengthValue <= 0 || recordLengthValue > Integer.MAX_VALUE) {
				break;
			}
			int recordLength = (int) recordLengthValue;
			int payloadStart = bb.position();
			int payloadEnd = payloadStart + recordLength;
			if (payloadEnd < payloadStart || payloadEnd > dataLength) {
				break;
			}
			try {
				long sharedPrefix = Varint.readUnsigned(bb);
				long itemId = sharedPrefix < 1 ? Varint.readUnsigned(bb) : previousItemId;
				long contextId = sharedPrefix < 2 ? Varint.readUnsigned(bb) : previousContextId;
				long propertyId = sharedPrefix < 3 ? Varint.readUnsigned(bb) : previousPropertyId;
				if (bb.position() >= payloadEnd) {
					break;
				}

				// validate key can be parsed
				Varint.readUnsigned(bb);
				Varint.readUnsigned(bb);
				if (bb.position() > payloadEnd) {
					break;
				}

				addRange(new ItemWithContext(itemId, contextId), propertyId, recordStart);

				hasPreviousIds = true;
				previousItemId = itemId;
				previousContextId = contextId;
				previousPropertyId = propertyId;
				bb.position(payloadEnd);
			} catch (RuntimeException e) {
				bb.position(recordStart);
				break;
			}
		}
		buffer.position(bb.position());
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
			int c = Long.compare(itemId, o.itemId);
			return c != 0 ? c : Long.compare(contextId, o.contextId);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (!(o instanceof ItemWithContext)) return false;
			ItemWithContext iwc = (ItemWithContext) o;
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