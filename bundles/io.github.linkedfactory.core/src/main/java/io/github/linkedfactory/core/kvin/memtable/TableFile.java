package io.github.linkedfactory.core.kvin.memtable;

import io.github.linkedfactory.core.kvin.parquet.records.KvinRecord;
import io.github.linkedfactory.core.kvin.util.Varint;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class TableFile implements AutoCloseable {
	static int LENGTH = 5 * 1014 * 1024;
	Path path;
	RandomAccessFile rafile;
	FileChannel channel;
	MappedByteBuffer buffer;
	Map<ItemWithContext, List<ItemData>> items = new ConcurrentSkipListMap<>();

	public TableFile(Path path) throws IOException {
		this.path = path;
		boolean readFile = Files.exists(path);
		rafile = new RandomAccessFile(path.toString(), "rw");
		channel = rafile.getChannel();
		buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, LENGTH);
	}

	void put(KvinRecord record) throws IOException {
		ItemWithContext icp = new ItemWithContext(record.itemId, record.contextId);

		int itemLength = Varint.calcLengthUnsigned(record.itemId) +
				Varint.calcLengthUnsigned(record.contextId) +
				Varint.calcLengthUnsigned(record.propertyId);

		ByteBuffer key = ByteBuffer.allocate(Varint.calcLengthUnsigned(record.time) +
				Varint.calcLengthUnsigned(record.seqNr));
		Varint.writeUnsigned(key, record.time);
		Varint.writeUnsigned(key, record.seqNr);

		int keyLength = key.remaining();
		int valueLength = ((byte[]) record.value).length;
		int requiredSpace = Varint.MAX_BYTES * 2 + itemLength + keyLength + valueLength;
		if (buffer.remaining() < requiredSpace) {
			buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, buffer.capacity() * 2);
			System.out.println("buffer size: " + buffer.capacity());
		}
		Varint.writeUnsigned(buffer, itemLength + keyLength + valueLength);
		Varint.writeUnsigned(buffer, record.itemId);
		Varint.writeUnsigned(buffer, record.contextId);
		Varint.writeUnsigned(buffer, record.propertyId);

		int[] ranges = new int[4];
		ranges[0] = buffer.position();
		ranges[1] = keyLength;
		buffer.put(key);
		ranges[2] = buffer.position();
		ranges[3] = valueLength;
		buffer.put(ByteBuffer.wrap((byte[]) record.value));

		var itemDataList = items.computeIfAbsent(icp, k -> new ArrayList<>());
		ItemData itemData;
		if (itemDataList.isEmpty()) {
			itemData = new ItemData(record.propertyId);
			itemDataList.add(itemData);
		} else {
			int index = findIndex(itemDataList, record.propertyId);
			if (index >= 0) {
				itemData = itemDataList.get(index);
			} else {
				itemData = new ItemData(record.propertyId);
				itemDataList.add(-index - 1, itemData);
			}
		}
		itemData.unsorted.add(ranges);
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
	public void close() throws IOException {
		rafile.close();
	}

	public int size() {
		return buffer.capacity();
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

	static class ItemData {
		final long propertyId;
		final List<int[]> unsorted = Collections.synchronizedList(new ArrayList<>());
		volatile List<int[]> sorted;

		ItemData(long propertyId) {
			this.propertyId = propertyId;
		}
	}
}
