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
	static int LENGTH = 5 * 1014 * 1024;
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

		ByteBuffer key = ByteBuffer.allocate(Varint.calcLengthUnsigned(record.time()) +
				Varint.calcLengthUnsigned(record.seqNr()));
		Varint.writeUnsigned(key, record.time());
		Varint.writeUnsigned(key, record.seqNr());
		key.flip();

		int keyLength = key.remaining();
		int valueLength = ((byte[]) record.value()).length;
		int requiredSpace = Varint.MAX_BYTES * 2 + itemLength + keyLength + valueLength;
		if (buffer.remaining() < requiredSpace) {
			buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, buffer.capacity() * 2);
			System.out.println("buffer size: " + buffer.capacity());
		}
		Varint.writeUnsigned(buffer, itemLength + keyLength + valueLength);
		writeCompressedIds(record.itemId(), record.contextId(), record.propertyId());

		int[] ranges = new int[4];
		ranges[0] = buffer.position();
		ranges[1] = keyLength;
		buffer.put(key);
		ranges[2] = buffer.position();
		ranges[3] = valueLength;
		buffer.put(ByteBuffer.wrap((byte[]) record.value()));
		hasPreviousIds = true;
		previousItemId = record.itemId();
		previousContextId = record.contextId();
		previousPropertyId = record.propertyId();

		addRange(icp, record.propertyId(), ranges);
	}

	@Override
	public IExtendedIterator<KvinRecord> fetch(long itemId, Long propertyId, Long contextId, long limit) {
		return fetch(itemId, propertyId, contextId, Long.MAX_VALUE, 0, limit, 0, null);
	}

	@Override
	public IExtendedIterator<KvinRecord> fetch(long itemId, Long propertyId, Long contextId, long end, long begin, long limit,
	                                           long interval, String op) {
		if (limit <= 0) {
			return NiceIterator.emptyIterator();
		}
		var itemDataList = items.get(new ItemWithContext(itemId, contextOrDefault(contextId)));
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
			List<int[]> ranges = getSortedRanges(itemData);
			for (int[] range : ranges) {
				long time = readTime(range);
				if (time > end || time < begin) {
					continue;
				}
				if (interval > 0 && time > nextTimeThreshold) {
					continue;
				}
				int seqNr = readSeqNr(range);
				byte[] value = readValue(range);
				result.add(new KvinRecord(itemId, contextOrDefault(contextId), itemData.propertyId, time, seqNr, value));
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
		if (sharedPrefix < 1) {
			length += Varint.calcLengthUnsigned(itemId);
		}
		if (sharedPrefix < 2) {
			length += Varint.calcLengthUnsigned(contextId);
		}
		if (sharedPrefix < 3) {
			length += Varint.calcLengthUnsigned(propertyId);
		}
		return length;
	}

	void writeCompressedIds(long itemId, long contextId, long propertyId) {
		int sharedPrefix = sharedPrefix(itemId, contextId, propertyId);
		Varint.writeUnsigned(buffer, sharedPrefix);
		if (sharedPrefix < 1) {
			Varint.writeUnsigned(buffer, itemId);
		}
		if (sharedPrefix < 2) {
			Varint.writeUnsigned(buffer, contextId);
		}
		if (sharedPrefix < 3) {
			Varint.writeUnsigned(buffer, propertyId);
		}
	}

	int sharedPrefix(long itemId, long contextId, long propertyId) {
		if (!hasPreviousIds || itemId != previousItemId) {
			return 0;
		}
		if (contextId != previousContextId) {
			return 1;
		}
		if (propertyId != previousPropertyId) {
			return 2;
		}
		return 3;
	}

	public int findIndex(List<ItemData> itemData, long propertyId) {
		int low = 0;
		int high = itemData.size() - 1;

		while (low <= high) {
			int mid = low + ((high - low) / 2);
			if (itemData.get(mid).propertyId < propertyId) {
				low = mid + 1;
			} else if (itemData.get(mid).propertyId > propertyId) {
				high = mid - 1;
			} else if (itemData.get(mid).propertyId == propertyId) {
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

	List<int[]> getSortedRanges(ItemData itemData) {
		synchronized (itemData.unsorted) {
			if (!itemData.unsorted.isEmpty()) {
				List<int[]> merged = new ArrayList<>();
				if (itemData.sorted != null) {
					merged.addAll(itemData.sorted);
				}
				merged.addAll(itemData.unsorted);
				itemData.unsorted.clear();
				merged.sort(this::compareRanges);
				itemData.sorted = merged;
			}
			return itemData.sorted != null ? itemData.sorted : Collections.emptyList();
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

	int compareRanges(int[] a, int[] b) {
		long timeA = readTime(a);
		long timeB = readTime(b);
		if (timeA != timeB) {
			return Long.compare(timeB, timeA);
		}
		return Integer.compare(readSeqNr(b), readSeqNr(a));
	}

	long readTime(int[] range) {
		return Varint.readUnsigned(buffer, range[0]);
	}

	int readSeqNr(int[] range) {
		return (int) Varint.readUnsigned(buffer, 
				range[0] + Varint.firstToLength(buffer.get(range[0])));
	}

	byte[] readValue(int[] range) {
		byte[] value = new byte[range[3]];
		buffer.get(range[2], value);
		return value;
	}

	long removeByRange(ItemData itemData, long begin, long end) {
		long removed = 0;

		synchronized (itemData.unsorted) {
			Iterator<int[]> it = itemData.unsorted.iterator();
			while (it.hasNext()) {
				int[] range = it.next();
				long time = readTime(range);
				if (time >= begin && time <= end) {
					it.remove();
					removed++;
				}
			}
		}

		if (itemData.sorted != null && !itemData.sorted.isEmpty()) {
			List<int[]> remaining = new ArrayList<>(itemData.sorted.size());
			for (int[] range : itemData.sorted) {
				long time = readTime(range);
				if (time >= begin && time <= end) {
					removed++;
				} else {
					remaining.add(range);
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

				int keyStart = bb.position();
				Varint.readUnsigned(bb);
				Varint.readUnsigned(bb);
				int keyEnd = bb.position();
				if (keyEnd > payloadEnd) {
					break;
				}

				int[] ranges = new int[]{keyStart, keyEnd - keyStart, keyEnd, payloadEnd - keyEnd};
				addRange(new ItemWithContext(itemId, contextId), propertyId, ranges);

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

	void addRange(ItemWithContext icp, long propertyId, int[] ranges) {
		var itemDataList = items.computeIfAbsent(icp, k -> new ArrayList<>());
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
		itemData.unsorted.add(ranges);
	}

	static class ItemWithContext implements Comparable<ItemWithContext> {
		long itemId;
		long contextId;

		ItemWithContext(long itemId, long contextId) {
			this.itemId = itemId;
			this.contextId = contextId;
		}

		@Override
		public int compareTo(ItemWithContext o) {
			long diff = itemId - o.itemId;
			if (diff != 0) {
				return diff < 0 ? -1 : 1;
			}
			diff = contextId - o.contextId;
			if (diff != 0) {
				return diff < 0 ? -1 : 1;
			}
			return 0;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			ItemWithContext iwc = (ItemWithContext) o;
			return itemId == iwc.itemId && contextId == iwc.contextId;
		}

		@Override
		public int hashCode() {
			return Objects.hash(itemId, contextId);
		}
	}

	public static class ItemData {
		final long propertyId;
		final List<int[]> unsorted = Collections.synchronizedList(new ArrayList<>());
		volatile List<int[]> sorted;

		ItemData(long propertyId) {
			this.propertyId = propertyId;
		}
	}
}
