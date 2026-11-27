package io.github.linkedfactory.core.kvin.memtable;

import io.github.linkedfactory.core.kvin.records.KvinRecord;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TableFileTest {
	private File tempDir;
	private Path tablePath;

	@Before
	public void setup() throws IOException {
		tempDir = Files.createTempDirectory("tablefile-test").toFile();
		tablePath = tempDir.toPath().resolve("table.data");
	}

	@After
	public void cleanup() throws IOException {
		FileUtils.deleteDirectory(tempDir);
	}

	@Test
	public void shouldWriteAndReadRecords() throws IOException {
		try (TableFile tableFile = new TableFile(tablePath)) {
			tableFile.put(
					new KvinRecord(2L, 1L, 3L, 1000L, 1, "first".getBytes(StandardCharsets.UTF_8)),
					new KvinRecord(2L, 1L, 3L, 1002L, 0, "second".getBytes(StandardCharsets.UTF_8)),
					new KvinRecord(2L, 1L, 3L, 1002L, 1, "third".getBytes(StandardCharsets.UTF_8)),
					new KvinRecord(2L, 1L, 4L, 1003L, 0, "other-property".getBytes(StandardCharsets.UTF_8))
			);

			List<KvinRecord> records = tableFile.fetch(1L, 3L, 2L, 10L).toList();
			assertEquals(3, records.size());

			assertEquals(1002L, records.get(0).time());
			assertEquals(1, records.get(0).seqNr());
			assertArrayEquals("third".getBytes(StandardCharsets.UTF_8), (byte[]) records.get(0).value());

			assertEquals(1002L, records.get(1).time());
			assertEquals(0, records.get(1).seqNr());
			assertArrayEquals("second".getBytes(StandardCharsets.UTF_8), (byte[]) records.get(1).value());

			assertEquals(1000L, records.get(2).time());
			assertEquals(1, records.get(2).seqNr());
			assertArrayEquals("first".getBytes(StandardCharsets.UTF_8), (byte[]) records.get(2).value());
		}
	}

	@Test
	public void shouldReadExistingFile() throws IOException {
		try (TableFile tableFile = new TableFile(tablePath)) {
			tableFile.put(
					new KvinRecord(22L, 11L, 33L, 2000L, 0, "persisted-1".getBytes(StandardCharsets.UTF_8)),
					new KvinRecord(22L, 11L, 33L, 1999L, 0, "persisted-2".getBytes(StandardCharsets.UTF_8))
			);
		}

		try (TableFile reopened = new TableFile(tablePath)) {
			List<KvinRecord> records = reopened.fetch(11L, 33L, 22L, 10L).toList();
			assertEquals(2, records.size());

			assertEquals(2000L, records.get(0).time());
			assertArrayEquals("persisted-1".getBytes(StandardCharsets.UTF_8), (byte[]) records.get(0).value());

			assertEquals(1999L, records.get(1).time());
			assertArrayEquals("persisted-2".getBytes(StandardCharsets.UTF_8), (byte[]) records.get(1).value());
		}
	}

	@Test
	public void shouldDeleteContext() throws IOException {
		byte[] value = "v".getBytes(StandardCharsets.UTF_8);
		try (TableFile tableFile = new TableFile(tablePath)) {
			tableFile.put(
					new KvinRecord(2L, 1L, 3L, 1000L, 0, value),
					new KvinRecord(2L, 1L, 3L, 1001L, 0, value),
					new KvinRecord(2L, 1L, 4L, 1000L, 0, value),
					new KvinRecord(5L, 1L, 3L, 1000L, 0, value),
					new KvinRecord(2L, 7L, 3L, 1000L, 0, value),
					new KvinRecord(1L, 1L, 3L, 1000L, 0, value),
					new KvinRecord(3L, 1L, 3L, 1000L, 0, value)
			);
			// sort the offsets of one series, the deleted count includes sorted and unsorted offsets
			assertEquals(2, tableFile.fetch(1L, 3L, 2L, 10L).toList().size());
			tableFile.put(new KvinRecord(2L, 1L, 3L, 1002L, 0, value));

			assertEquals(5, tableFile.delete(2L));
			assertEquals(List.of(), tableFile.fetch(1L, 3L, 2L, 10L).toList());
			assertEquals(List.of(), tableFile.fetch(7L, 3L, 2L, 10L).toList());
			assertEquals(List.of(), tableFile.properties(1L, 2L).toList());

			// other contexts, including the neighbouring IDs, are kept
			assertEquals(1, tableFile.fetch(1L, 3L, 1L, 10L).toList().size());
			assertEquals(1, tableFile.fetch(1L, 3L, 3L, 10L).toList().size());
			assertEquals(1, tableFile.fetch(1L, 3L, 5L, 10L).toList().size());

			assertEquals(0, tableFile.delete(2L));
			assertEquals(0, tableFile.delete(9L));

			// the context can be used again
			tableFile.put(new KvinRecord(2L, 1L, 3L, 2000L, 0, value));
			List<KvinRecord> records = tableFile.fetch(1L, 3L, 2L, 10L).toList();
			assertEquals(1, records.size());
			assertEquals(2000L, records.get(0).time());
			assertTrue(tableFile.properties(1L, 2L).toList().contains(3L));
		}
	}

	private static byte[] bytes(String value) {
		return value.getBytes(StandardCharsets.UTF_8);
	}

	private static List<Long> times(TableFile tableFile, long item, Long property, long context) {
		return tableFile.fetch(item, property, context, 100L).toList().stream().map(KvinRecord::time).toList();
	}

	@Test
	public void shouldKeepContextDeleteAfterReopening() throws IOException {
		try (TableFile tableFile = new TableFile(tablePath)) {
			tableFile.put(
					new KvinRecord(2L, 1L, 3L, 1000L, 0, bytes("deleted")),
					new KvinRecord(2L, 4L, 3L, 1000L, 0, bytes("deleted")),
					new KvinRecord(5L, 1L, 3L, 1000L, 0, bytes("kept")));
			assertEquals(2, tableFile.delete(2L));
			// written after the delete, so the tombstone does not apply to it
			tableFile.put(new KvinRecord(2L, 1L, 3L, 2000L, 0, bytes("re-added")));
		}

		try (TableFile reopened = new TableFile(tablePath)) {
			assertEquals(List.of(2000L), times(reopened, 1L, 3L, 2L));
			assertEquals(List.of(), times(reopened, 4L, 3L, 2L));
			assertEquals(List.of(1000L), times(reopened, 1L, 3L, 5L));
			assertArrayEquals(bytes("re-added"), (byte[]) reopened.fetch(1L, 3L, 2L, 1L).next().value());
			// appending after reopening continues behind the existing entries
			reopened.put(new KvinRecord(5L, 1L, 3L, 3000L, 0, bytes("appended")));
		}

		try (TableFile reopened = new TableFile(tablePath)) {
			assertEquals(List.of(3000L, 1000L), times(reopened, 1L, 3L, 5L));
			assertEquals(List.of(2000L), times(reopened, 1L, 3L, 2L));
		}
	}

	@Test
	public void shouldKeepItemAndRangeDeletesAfterReopening() throws IOException {
		try (TableFile tableFile = new TableFile(tablePath)) {
			tableFile.put(
					new KvinRecord(2L, 1L, 3L, 1000L, 0, bytes("a")),
					new KvinRecord(2L, 1L, 3L, 1001L, 0, bytes("b")),
					new KvinRecord(2L, 1L, 3L, 1002L, 0, bytes("c")),
					new KvinRecord(2L, 1L, 4L, 1001L, 0, bytes("d")),
					new KvinRecord(2L, 6L, 3L, 1000L, 0, bytes("e")),
					new KvinRecord(2L, 7L, 3L, 1000L, 0, bytes("f")),
					new KvinRecord(2L, 7L, 3L, 1500L, 0, bytes("g")));
			// one property, time range
			assertEquals(1, tableFile.delete(1L, 3L, 2L, 1001L, 1001L));
			// all properties, unbounded begin
			assertEquals(1, tableFile.delete(7L, null, 2L, 1200L, Long.MIN_VALUE));
			assertTrue(tableFile.delete(6L, 2L));
			assertFalse(tableFile.delete(6L, 2L));
			assertEquals(0, tableFile.delete(8L, null, 2L, Long.MAX_VALUE, Long.MIN_VALUE));
		}

		try (TableFile reopened = new TableFile(tablePath)) {
			assertEquals(List.of(1002L, 1000L), times(reopened, 1L, 3L, 2L));
			assertEquals(List.of(1001L), times(reopened, 1L, 4L, 2L));
			assertEquals(List.of(), times(reopened, 6L, null, 2L));
			assertEquals(List.of(1500L), times(reopened, 7L, null, 2L));
		}
	}

	@Test
	public void shouldReadCompressedIdsAcrossTombstones() throws IOException {
		try (TableFile tableFile = new TableFile(tablePath)) {
			tableFile.put(new KvinRecord(2L, 1L, 3L, 1000L, 0, bytes("before")),
					new KvinRecord(9L, 8L, 3L, 1000L, 0, bytes("other")));
			assertEquals(1, tableFile.delete(2L));
			// shares all IDs with the record before the tombstone
			tableFile.put(new KvinRecord(9L, 8L, 3L, 1001L, 0, bytes("after")));
		}

		try (TableFile reopened = new TableFile(tablePath)) {
			assertEquals(List.of(1001L, 1000L), times(reopened, 8L, 3L, 9L));
			assertEquals(List.of(), times(reopened, 1L, 3L, 2L));
		}
	}

	@Test
	public void shouldKeepEntriesWhenTheMappingGrows() throws IOException {
		long length = TableFile.LENGTH;
		TableFile.LENGTH = 64;
		try {
			try (TableFile tableFile = new TableFile(tablePath)) {
				for (int i = 0; i < 100; i++) {
					tableFile.put(new KvinRecord(2L, 1L, 3L, i, 0, bytes("value-" + i)));
				}
				assertEquals(10, tableFile.delete(1L, 3L, 2L, 99L, 90L));
				assertEquals(90, times(tableFile, 1L, 3L, 2L).size());
			}
			try (TableFile reopened = new TableFile(tablePath)) {
				List<KvinRecord> records = reopened.fetch(1L, 3L, 2L, 1000L).toList();
				assertEquals(90, records.size());
				assertEquals(89L, records.get(0).time());
				assertArrayEquals(bytes("value-0"), (byte[]) records.get(89).value());
			}
		} finally {
			TableFile.LENGTH = length;
		}
	}

	/**
	 * Returns the number of mappings of the file in this process, or -1 if unknown.
	 */
	static long mappings(Path file) throws IOException {
		Path maps = Path.of("/proc/self/maps");
		if (!Files.isReadable(maps)) {
			return -1;
		}
		String path = file.toRealPath().toString();
		try (var lines = Files.lines(maps)) {
			return lines.filter(line -> line.endsWith(" " + path) || line.endsWith("\t" + path)
					|| line.endsWith(path)).count();
		}
	}

	@Test
	public void unmapsAllMappingsOnClose() throws IOException {
		Assume.assumeTrue("Unmapping is not supported", BufferUnmapper.isSupported());
		Assume.assumeTrue("Mappings are not visible", mappings(tempDir.toPath()) >= 0);
		long previousLength = TableFile.LENGTH;
		TableFile.LENGTH = 1024;
		try {
			TableFile tableFile = new TableFile(tablePath);
			byte[] value = new byte[100];
			for (int i = 0; i < 100; i++) {
				// grows the file beyond the first mappings
				tableFile.put(new KvinRecord(1L, 1L, 1L, i, 0, value));
			}
			assertTrue("The grown file has several mappings", mappings(tablePath) >= 2);

			tableFile.close();
			assertEquals(0, mappings(tablePath));
		} finally {
			TableFile.LENGTH = previousLength;
		}
	}

	@Test
	public void failsAfterClose() throws IOException {
		TableFile tableFile = new TableFile(tablePath);
		tableFile.put(new KvinRecord(1L, 1L, 1L, 1, 0, new byte[]{1}));
		tableFile.close();
		// closing again does nothing
		tableFile.close();

		assertThrows(IllegalStateException.class, () -> tableFile.fetch(1L, 1L, 1L, 10));
		assertThrows(IllegalStateException.class,
				() -> tableFile.put(new KvinRecord(1L, 1L, 1L, 2, 0, new byte[]{2})));
		assertThrows(IllegalStateException.class, () -> tableFile.delete(1L, 1L));
		assertThrows(IllegalStateException.class, () -> tableFile.delete(1L));
		assertThrows(IllegalStateException.class, () -> tableFile.delete(1L, 1L, 1L, 2, 1));

		// the values are kept
		try (TableFile reopened = new TableFile(tablePath)) {
			assertEquals(1, reopened.fetch(1L, 1L, 1L, 10).toList().size());
		}
	}

	@Test(timeout = 60_000)
	public void closesWhileOtherThreadsFetch() throws Exception {
		for (int round = 0; round < 20; round++) {
			Path path = tempDir.toPath().resolve("concurrent-" + round + ".data");
			TableFile tableFile = new TableFile(path);
			for (int i = 0; i < 1000; i++) {
				tableFile.put(new KvinRecord(1L, 1L, 1L, i, 0, new byte[]{(byte) i}));
			}
			java.util.concurrent.CountDownLatch reading = new java.util.concurrent.CountDownLatch(4);
			List<Thread> readers = new java.util.ArrayList<>();
			java.util.concurrent.atomic.AtomicReference<Throwable> failure = new java.util.concurrent.atomic.AtomicReference<>();
			for (int t = 0; t < 4; t++) {
				Thread reader = new Thread(() -> {
					reading.countDown();
					try {
						while (true) {
							assertEquals(1000, tableFile.fetch(1L, 1L, 1L, 1000).toList().size());
						}
					} catch (IllegalStateException e) {
						// closed
					} catch (Throwable e) {
						failure.set(e);
					}
				});
				reader.start();
				readers.add(reader);
			}
			reading.await();
			tableFile.close();
			for (Thread reader : readers) {
				reader.join();
			}
			if (failure.get() != null) {
				throw new AssertionError(failure.get());
			}
		}
	}

	private static KvinRecord value(long time, int length) {
		byte[] value = new byte[length];
		java.util.Arrays.fill(value, (byte) time);
		return new KvinRecord(1L, 1L, 1L, time, 0, value);
	}

	/**
	 * Writes each record as a group of its own and returns the end of each group.
	 */
	private List<Long> write(KvinRecord... records) throws IOException {
		List<Long> ends = new java.util.ArrayList<>();
		try (TableFile tableFile = new TableFile(tablePath)) {
			for (KvinRecord record : records) {
				tableFile.put(record);
				tableFile.commit();
				ends.add(tableFile.length());
			}
		}
		return ends;
	}

	// length of the commit entry of a group: length, kind and checksum
	private static final int COMMIT_ENTRY_BYTES = 1 + 1 + Integer.BYTES;

	private void overwrite(long position, byte[] bytes) throws IOException {
		try (var file = new java.io.RandomAccessFile(tablePath.toFile(), "rw")) {
			file.seek(position);
			file.write(bytes);
		}
	}

	private List<Long> times() throws IOException {
		try (TableFile tableFile = new TableFile(tablePath)) {
			return tableFile.fetch(1L, 1L, 1L, 100).toList().stream().map(KvinRecord::time).toList();
		}
	}

	@Test
	public void dropsATornLastEntry() throws IOException {
		List<Long> ends = write(value(1, 10), value(2, 10), value(3, 300));
		// only the first bytes of the last entry reached the disk
		overwrite(ends.get(1) + 4, new byte[(int) (ends.get(2) - ends.get(1) - 4)]);
		assertEquals(List.of(2L, 1L), times());

		// new entries follow the last complete one
		try (TableFile tableFile = new TableFile(tablePath)) {
			assertEquals((long) ends.get(1), tableFile.length());
			tableFile.put(value(4, 10));
		}
		assertEquals(List.of(4L, 2L, 1L), times());
	}

	@Test
	public void stopsAtAnEntryWithAChangedByte() throws IOException {
		List<Long> ends = write(value(1, 10), value(2, 10), value(3, 10));
		overwrite(ends.get(1) - COMMIT_ENTRY_BYTES - 1, new byte[]{(byte) 0x55});
		// the entries after it may depend on it, so they are dropped as well
		assertEquals(List.of(1L), times());
	}

	@Test
	public void clearsTheEntriesAfterATornEntry() throws IOException {
		List<Long> ends = write(value(1, 10), value(2, 300), value(3, 10), value(4, 10));
		// a page in the middle did not reach the disk, the following ones did
		overwrite(ends.get(0) + 4, new byte[100]);
		assertEquals(List.of(1L), times());

		byte[] content = Files.readAllBytes(tablePath);
		for (long i = ends.get(0); i < ends.get(3); i++) {
			assertEquals("Byte " + i + " is cleared", 0, content[(int) i]);
		}
		try (TableFile tableFile = new TableFile(tablePath)) {
			tableFile.put(value(5, 10));
		}
		assertEquals(List.of(5L, 1L), times());
	}

	@Test
	public void keepsValuesOfATornDelete() throws IOException {
		long valueEnd;
		try (TableFile tableFile = new TableFile(tablePath)) {
			tableFile.put(value(1, 10));
			// a delete closes the group of the preceding values
			assertTrue(tableFile.delete(1L, 1L));
			valueEnd = tableFile.length() - (1 + 1 + 1 + 1 + COMMIT_ENTRY_BYTES);
		}
		// the tombstone did not reach the disk completely
		overwrite(valueEnd + 2, new byte[2]);
		assertEquals(List.of(1L), times());
	}

	@Test
	public void syncsWrittenEntries() throws IOException {
		long previousLength = TableFile.LENGTH;
		TableFile.LENGTH = 1024;
		try (TableFile tableFile = new TableFile(tablePath)) {
			tableFile.put(value(1, 10));
			assertTrue(tableFile.unsyncedBytes() > 0);
			tableFile.sync();
			assertEquals(0, tableFile.unsyncedBytes());

			// growing the file changes its length
			for (int i = 2; i < 20; i++) {
				tableFile.put(value(i, 100));
			}
			assertTrue(tableFile.unsyncedBytes() > 0);
			tableFile.sync();
			assertEquals(0, tableFile.unsyncedBytes());
			tableFile.delete(1L, 1L);
			assertTrue(tableFile.unsyncedBytes() > 0);
			tableFile.sync();
			assertEquals(0, tableFile.unsyncedBytes());
		} finally {
			TableFile.LENGTH = previousLength;
		}
	}

	@Test
	public void keepsAGroupCompletelyOrNotAtAll() throws IOException {
		long groupEnd;
		try (TableFile tableFile = new TableFile(tablePath)) {
			tableFile.put(value(1, 10));
			tableFile.commit();
			groupEnd = tableFile.length();
			tableFile.put(value(2, 10));
			tableFile.put(value(3, 10));
			tableFile.put(value(4, 10));
		}
		// the end of the second record of the group did not reach the disk
		overwrite(groupEnd + 20, new byte[4]);
		assertEquals(List.of(1L), times());
	}

	@Test
	public void dropsEntriesOfAnUnclosedGroup() throws IOException {
		int previousMaxGroupBytes = TableFile.MAX_GROUP_BYTES;
		TableFile.MAX_GROUP_BYTES = 200;
		Path copy = tempDir.toPath().resolve("copy.data");
		try (TableFile tableFile = new TableFile(tablePath)) {
			for (int i = 1; i <= 25; i++) {
				tableFile.put(value(i, 20));
			}
			// the state of the file if the process stopped now, as the mapping writes to the file
			Files.copy(tablePath, copy);
		} finally {
			TableFile.MAX_GROUP_BYTES = previousMaxGroupBytes;
		}
		try (TableFile reopened = new TableFile(copy)) {
			List<Long> times = reopened.fetch(1L, 1L, 1L, 100).toList().stream().map(KvinRecord::time).toList();
			// groups close after 200 bytes, the records of the last group are not closed yet
			assertTrue(times.size() > 0 && times.size() < 25);
			assertEquals(times.size() * 1L, (long) times.get(0));
			assertEquals(1L, (long) times.get(times.size() - 1));
		}
		// closing the file closes the last group
		assertEquals(25, times().size());
	}
}
