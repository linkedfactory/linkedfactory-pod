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
}
