package io.github.linkedfactory.core.kvin.memtable;

import io.github.linkedfactory.core.kvin.records.KvinRecord;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TableFileSetTest {
	private static final long CTX = 1L;
	private static final long OTHER_CTX = 2L;
	private static final long ITEM = 10L;
	private static final long PROPERTY = 20L;
	private static final long OTHER_PROPERTY = 21L;

	private long previousLength;
	private Path directory;

	@Before
	public void setup() throws IOException {
		// keep the mapped files small, as the tests create many of them
		previousLength = TableFile.LENGTH;
		TableFile.LENGTH = 1024;
		directory = Files.createTempDirectory("tablefileset-test");
	}

	@After
	public void cleanup() throws IOException {
		TableFile.LENGTH = previousLength;
		FileUtils.deleteDirectory(directory.toFile());
	}

	private static KvinRecord record(long ctx, long item, long property, long time, int seqNr, String value) {
		return new KvinRecord(ctx, item, property, time, seqNr, value.getBytes(StandardCharsets.UTF_8));
	}

	private static String value(KvinRecord record) {
		return new String((byte[]) record.value(), StandardCharsets.UTF_8);
	}

	private static List<String> values(List<KvinRecord> records) {
		return records.stream().map(TableFileSetTest::value).collect(Collectors.toList());
	}

	private List<String> fileNames() throws IOException {
		try (Stream<Path> paths = Files.list(directory)) {
			return paths.map(p -> p.getFileName().toString()).sorted().collect(Collectors.toList());
		}
	}

	/**
	 * Opens a set that starts a new file for every record.
	 */
	private TableFileSet open() throws IOException {
		return new TableFileSet(directory, 1);
	}

	@Test
	public void startsNewFilesWhenTheMaximumLengthIsReached() throws IOException {
		try (TableFileSet set = open()) {
			set.put(record(CTX, ITEM, PROPERTY, 1, 0, "a"), record(CTX, ITEM, PROPERTY, 2, 0, "b"));
			set.put(record(CTX, ITEM, PROPERTY, 3, 0, "c"));
			assertEquals(3, set.getFiles().size());
		}
		assertEquals(List.of("entries_0.dat", "entries_1.dat", "entries_2.dat"), fileNames());

		try (TableFileSet set = new TableFileSet(directory)) {
			// with the default maximum, new records are appended to the newest file
			set.put(record(CTX, ITEM, PROPERTY, 4, 0, "d"), record(CTX, ITEM, PROPERTY, 5, 0, "e"));
			assertEquals(3, set.getFiles().size());
			assertEquals(List.of("e", "d", "c", "b", "a"), values(set.fetch(ITEM, PROPERTY, CTX, 10).toList()));
		}
	}

	@Test
	public void continuesNumberingAfterReopening() throws IOException {
		try (TableFileSet set = open()) {
			set.put(record(CTX, ITEM, PROPERTY, 1, 0, "a"), record(CTX, ITEM, PROPERTY, 2, 0, "b"));
		}
		try (TableFileSet set = open()) {
			assertEquals(2, set.getFiles().size());
			set.put(record(CTX, ITEM, PROPERTY, 3, 0, "c"));
		}
		assertEquals(List.of("entries_0.dat", "entries_1.dat", "entries_2.dat"), fileNames());
	}

	@Test
	public void mergesValuesOfAllFiles() throws IOException {
		try (TableFileSet set = open()) {
			set.put(record(CTX, ITEM, PROPERTY, 2, 0, "t2"),
					record(CTX, ITEM, PROPERTY, 5, 0, "t5"),
					record(CTX, ITEM, PROPERTY, 1, 0, "t1"),
					record(CTX, ITEM, PROPERTY, 4, 1, "t4s1"),
					record(CTX, ITEM, PROPERTY, 4, 0, "t4s0"),
					record(CTX, ITEM, PROPERTY, 3, 0, "t3"));

			assertEquals(List.of("t5", "t4s1", "t4s0", "t3", "t2", "t1"),
					values(set.fetch(ITEM, PROPERTY, CTX, 10).toList()));
			assertEquals(List.of("t5", "t4s1"), values(set.fetch(ITEM, PROPERTY, CTX, 2).toList()));
			assertEquals(List.of("t4s1", "t4s0", "t3"),
					values(set.fetch(ITEM, PROPERTY, CTX, 4, 3, 10, 0, null).toList()));
		}
	}

	@Test
	public void newerFilesShadowEqualKeys() throws IOException {
		try (TableFileSet set = open()) {
			set.put(record(CTX, ITEM, PROPERTY, 1, 0, "old"),
					record(CTX, ITEM, PROPERTY, 2, 0, "other"),
					record(CTX, ITEM, PROPERTY, 1, 0, "new"));
			assertEquals(List.of("other", "new"), values(set.fetch(ITEM, PROPERTY, CTX, 10).toList()));
		}
	}

	@Test
	public void appliesTheIntervalToTheMergedValues() throws IOException {
		try (TableFileSet set = open()) {
			for (int time = 1; time <= 10; time++) {
				set.put(record(CTX, ITEM, PROPERTY, time, 0, "t" + time));
			}
			assertEquals(List.of("t10", "t7", "t4", "t1"),
					values(set.fetch(ITEM, PROPERTY, CTX, 10, 0, 10, 3, null).toList()));
			assertEquals(List.of("t10", "t7"),
					values(set.fetch(ITEM, PROPERTY, CTX, 10, 0, 2, 3, null).toList()));
		}
	}

	@Test
	public void fetchesAllPropertiesWithALimit() throws IOException {
		try (TableFileSet set = open()) {
			set.put(record(CTX, ITEM, OTHER_PROPERTY, 1, 0, "o1"),
					record(CTX, ITEM, PROPERTY, 1, 0, "p1"),
					record(CTX, ITEM, PROPERTY, 2, 0, "p2"),
					record(CTX, ITEM, OTHER_PROPERTY, 2, 0, "o2"));

			assertEquals(List.of(PROPERTY, OTHER_PROPERTY), set.properties(ITEM, CTX).toList());
			assertEquals(List.of("p2", "p1", "o2", "o1"), values(set.fetch(ITEM, null, CTX, 10).toList()));
			assertEquals(List.of("p2", "p1", "o2"), values(set.fetch(ITEM, null, CTX, 3).toList()));
		}
	}

	@Test
	public void deletesContextInAllFilesAndKeepsLaterValues() throws IOException {
		try (TableFileSet set = open()) {
			set.put(record(CTX, ITEM, PROPERTY, 1, 0, "a"),
					record(OTHER_CTX, ITEM, PROPERTY, 1, 0, "other"),
					record(CTX, ITEM, PROPERTY, 2, 0, "b"));
			assertEquals(2, set.delete(CTX));
			assertEquals(List.of(), set.fetch(ITEM, PROPERTY, CTX, 10).toList());

			set.put(record(CTX, ITEM, PROPERTY, 3, 0, "c"));
		}
		try (TableFileSet set = open()) {
			assertEquals(List.of("c"), values(set.fetch(ITEM, PROPERTY, CTX, 10).toList()));
			assertEquals(List.of("other"), values(set.fetch(ITEM, PROPERTY, OTHER_CTX, 10).toList()));
		}
	}

	@Test
	public void deletesItemsAndRangesInAllFiles() throws IOException {
		try (TableFileSet set = open()) {
			set.put(record(CTX, ITEM, PROPERTY, 1, 0, "a"),
					record(CTX, ITEM, PROPERTY, 2, 0, "b"),
					record(CTX, ITEM, PROPERTY, 3, 0, "c"),
					record(CTX, ITEM, PROPERTY, 4, 0, "d"),
					record(CTX, ITEM + 1, PROPERTY, 1, 0, "x"),
					record(CTX, ITEM + 1, PROPERTY, 2, 0, "y"));

			assertEquals(2, set.delete(ITEM, PROPERTY, CTX, 3, 2));
			assertTrue(set.delete(ITEM + 1, CTX));
			assertFalse(set.delete(ITEM + 1, CTX));
		}
		try (TableFileSet set = open()) {
			assertEquals(List.of("d", "a"), values(set.fetch(ITEM, PROPERTY, CTX, 10).toList()));
			assertEquals(List.of(), set.fetch(ITEM + 1, PROPERTY, CTX, 10).toList());
		}
	}

	@Test
	public void ignoresOtherFiles() throws IOException {
		Files.writeString(directory.resolve("notes.txt"), "not a table file");
		try (TableFileSet set = open()) {
			assertEquals(0, set.getFiles().size());
			assertEquals(List.of(), set.fetch(ITEM, PROPERTY, CTX, 10).toList());
			assertEquals(0, set.delete(CTX));
		}
	}

	@Test
	public void fetchesAllRecordsInKeyOrder() throws IOException {
		try (TableFileSet set = open()) {
			set.put(record(OTHER_CTX, ITEM, PROPERTY, 1, 0, "other context"),
					record(CTX, ITEM, OTHER_PROPERTY, 1, 0, "other property"),
					record(CTX, ITEM, PROPERTY, 1, 0, "t1"),
					record(CTX, ITEM, PROPERTY, 2, 0, "t2 old"),
					record(CTX, ITEM, PROPERTY, 2, 0, "t2 new"),
					record(CTX, ITEM + 1, PROPERTY, 5, 0, "next item"));
			set.delete(ITEM + 1, CTX);

			// a record of a newer file replaces the record with the same key
			assertEquals(List.of("t2 new", "t1", "other property", "other context"),
					values(set.fetchAll().toList()));
		}
	}
}
