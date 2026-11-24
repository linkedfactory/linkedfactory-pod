package io.github.linkedfactory.core.kvin.parquet;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.util.KvinTupleGenerator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Tests that compaction does not lose data if it is interrupted or if data is
 * written while it runs.
 */
public class CompactorTest {
	// 02.10.2023 0:00, week 40
	static final long START_TIME = 1696197600000L;
	static final URI PROPERTY = URIs.createURI("http://example.org/1");

	File archiveDir;
	KvinParquet store;

	static class SimulatedCrash extends RuntimeException {
	}

	@Before
	public void setup() throws IOException {
		archiveDir = Files.createTempDirectory("archive").toFile();
		store = new KvinParquet(archiveDir.toString());
		// two puts to the same week create two data files and two files per mapping type
		put("first");
		put("second");
		assertEquals(2, dataFiles().length);
		assertTrue(values("first", "second").values().stream().allMatch(count -> count == 3));
	}

	@After
	public void cleanup() throws IOException {
		FileUtils.deleteDirectory(archiveDir);
	}

	void put(String name) {
		store.put(new KvinTupleGenerator().setStartTime(START_TIME)
				.setItems(5)
				.setPropertiesPerItem(2)
				.setValuesPerProperty(3)
				.setItemPattern("http://example.org/" + name + "/{}")
				.setPropertyPattern("http://example.org/{}")
				.generate());
	}

	File weekFolder() {
		return new File(new File(archiveDir, "2023"), "40");
	}

	String[] dataFiles() {
		String[] names = weekFolder().list((dir, name) -> name.endsWith(".parquet"));
		Arrays.sort(names);
		return names;
	}

	/**
	 * Returns the number of values of each item.
	 */
	Map<String, Integer> values(String... names) {
		Map<String, Integer> values = new LinkedHashMap<>();
		for (String name : names) {
			for (int i = 1; i <= 5; i++) {
				URI item = URIs.createURI("http://example.org/" + name + "/" + i);
				values.put(item.toString(), store.fetch(item, PROPERTY, Kvin.DEFAULT_CONTEXT, 0).toList().size());
			}
		}
		return values;
	}

	/**
	 * Simulates a restart of the store after a crash.
	 */
	void restart() {
		store.close();
		store = new KvinParquet(archiveDir.toString());
	}

	void assertCompacted(Map<String, Integer> expectedValues, String... names) {
		assertFalse(new File(archiveDir, Compactor.COMPACTION_FOLDER).exists());
		assertEquals(List.of("data__1.parquet"), List.of(dataFiles()));
		assertEquals(expectedValues, values(names));
	}

	@Test
	public void testCompaction() throws IOException {
		Map<String, Integer> before = values("first", "second");
		new Compactor(store, 2, 2).execute();
		assertCompacted(before, "first", "second");
	}

	@Test
	public void testCrashBeforeJournalDiscardsCompaction() throws IOException {
		Map<String, Integer> before = values("first", "second");
		Compactor compactor = new Compactor(store, 2, 2) {
			@Override
			void writeJournal(Map<String, List<String>> replacements) {
				throw new SimulatedCrash();
			}
		};
		assertThrows(SimulatedCrash.class, compactor::execute);
		assertTrue(new File(archiveDir, Compactor.COMPACTION_FOLDER).exists());

		restart();
		assertFalse(new File(archiveDir, Compactor.COMPACTION_FOLDER).exists());
		assertEquals(List.of("data__1.parquet", "data__2.parquet"), List.of(dataFiles()));
		assertEquals(before, values("first", "second"));
	}

	@Test
	public void testCrashAfterJournalFinishesCompaction() throws IOException {
		Map<String, Integer> before = values("first", "second");
		Compactor compactor = new Compactor(store, 2, 2) {
			@Override
			void replaceFiles(Map<String, List<String>> replacements) {
				throw new SimulatedCrash();
			}
		};
		assertThrows(SimulatedCrash.class, compactor::execute);

		restart();
		assertCompacted(before, "first", "second");
	}

	@Test
	public void testCrashDuringReplacementFinishesCompaction() throws IOException {
		Map<String, Integer> before = values("first", "second");
		Compactor compactor = new Compactor(store, 2, 2) {
			@Override
			void replaceFiles(Map<String, List<String>> replacements) throws IOException {
				// replace the first compacted file and delete the originals of the second one
				var entries = List.copyOf(replacements.entrySet());
				Map<String, List<String>> partial = new LinkedHashMap<>();
				partial.put(entries.get(0).getKey(), entries.get(0).getValue());
				super.replaceFiles(partial);
				for (String original : entries.get(1).getValue()) {
					Files.delete(archiveDir.toPath().resolve(original));
				}
				throw new SimulatedCrash();
			}
		};
		assertThrows(SimulatedCrash.class, compactor::execute);

		restart();
		assertCompacted(before, "first", "second");
	}

	@Test
	public void testCrashDuringReplacementIsFinishedByNextCompaction() throws IOException {
		Map<String, Integer> before = values("first", "second");
		Compactor compactor = new Compactor(store, 2, 2) {
			@Override
			void replaceFiles(Map<String, List<String>> replacements) {
				throw new SimulatedCrash();
			}
		};
		assertThrows(SimulatedCrash.class, compactor::execute);

		// without restart
		new Compactor(store, 2, 2).execute();
		assertCompacted(before, "first", "second");
	}

	@Test
	public void testDataWrittenDuringCompactionIsKept() throws IOException {
		Map<String, Integer> before = values("first", "second");
		Compactor compactor = new Compactor(store, 2, 2) {
			@Override
			void writeJournal(Map<String, List<String>> replacements) throws IOException {
				// written after the files were compacted and before they are replaced
				put("third");
				super.writeJournal(replacements);
			}
		};
		compactor.execute();

		assertFalse(new File(archiveDir, Compactor.COMPACTION_FOLDER).exists());
		assertEquals(List.of("data__1.parquet", "data__3.parquet"), List.of(dataFiles()));
		Map<String, Integer> expected = new LinkedHashMap<>(before);
		for (int i = 1; i <= 5; i++) {
			expected.put("http://example.org/third/" + i, 3);
		}
		assertEquals(expected, values("first", "second", "third"));
	}
}
