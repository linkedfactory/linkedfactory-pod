package io.github.linkedfactory.core.kvin.iceberg;

import io.github.linkedfactory.core.kvin.KvinTuple;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Parquet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class KvinIcebergCompactionTest {
	private static final Comparator<org.apache.iceberg.data.Record> ORDER = Comparator
			.comparingLong((org.apache.iceberg.data.Record row) -> (Long) row.get(0))
			.thenComparingLong(row -> (Long) row.get(1)).thenComparingLong(row -> (Long) row.get(2))
			.thenComparing((a, b) -> Long.compare((Long) b.get(3), (Long) a.get(3)))
			.thenComparing((a, b) -> Integer.compare((Integer) b.get(4), (Integer) a.get(4)));
	private Path directory;
	private KvinIceberg store;
	private final URI item = URIs.createURI("urn:compaction:item");
	private final URI property = URIs.createURI("urn:compaction:property");

	@Before
	public void setUp() throws Exception {
		directory = Path.of("target", "iceberg-compaction-tests", UUID.randomUUID().toString()).toAbsolutePath();
		Files.createDirectories(directory);
		store = new KvinIceberg(directory.toString());
	}

	@After
	public void tearDown() throws Exception {
		if (store != null) {
			store.close();
		}
		FileUtils.deleteDirectory(directory.toFile());
	}

	private Table table() {
		return new HadoopTables(new Configuration()).load(directory.resolve("iceberg").toString());
	}

	private Table spyStoreTable() throws Exception {
		var field = KvinIceberg.class.getDeclaredField("table");
		field.setAccessible(true);
		Table spy = spy((Table) field.get(store));
		field.set(store, spy);
		return spy;
	}

	private List<DataFile> files() throws Exception {
		return files(table().currentSnapshot().snapshotId());
	}

	private List<DataFile> files(long snapshotId) throws Exception {
		Map<String, DataFile> files = new LinkedHashMap<>();
		try (CloseableIterable<FileScanTask> tasks = table().newScan().useSnapshot(snapshotId).planFiles()) {
			for (FileScanTask task : tasks) {
				files.putIfAbsent(task.file().location(), task.file());
			}
		}
		return new ArrayList<>(files.values());
	}

	private List<GenericRecord> rows(DataFile file) throws Exception {
		Table table = table();
		List<GenericRecord> result = new ArrayList<>();
		try (CloseableIterable<org.apache.iceberg.data.Record> rows = Parquet
				.read(table.io().newInputFile(file.location())).project(table.schema())
				.createReaderFunc(schema -> GenericParquetReaders.buildReader(table.schema(), schema)).build()) {
			for (org.apache.iceberg.data.Record row : rows) {
				result.add((GenericRecord) row.copy());
			}
		}
		return result;
	}

	private List<GenericRecord> allRows() throws Exception {
		List<GenericRecord> result = new ArrayList<>();
		for (DataFile file : files()) {
			result.addAll(rows(file));
		}
		result.sort(ORDER);
		return result;
	}

	private static List<Object> identity(org.apache.iceberg.data.Record row) {
		List<Object> identity = new ArrayList<>();
		for (int i = 0; i < 13; i++) {
			if (i != 5) {
				identity.add(row.get(i));
			}
		}
		return identity;
	}

	private void assertSortedAndMarked(List<DataFile> files) throws Exception {
		List<List<GenericRecord>> outputs = new ArrayList<>();
		for (DataFile file : files) {
			List<GenericRecord> rows = rows(file);
			assertFalse(rows.isEmpty());
			Set<List<Object>> seen = new HashSet<>();
			for (int i = 0; i < rows.size(); i++) {
				GenericRecord row = rows.get(i);
				List<Object> series = List.of(row.get(0), row.get(1), row.get(2));
				assertEquals(seen.add(series), row.get(5));
				if (i > 0) {
					assertTrue("Output must retain full KVIN sort order", ORDER.compare(rows.get(i - 1), row) <= 0);
				}
			}
			outputs.add(rows);
		}
		outputs.sort((a, b) -> ORDER.compare(a.get(0), b.get(0)));
		for (int i = 1; i < outputs.size(); i++) {
			List<GenericRecord> previous = outputs.get(i - 1);
			assertTrue("Output file ranges must be globally ordered",
					ORDER.compare(previous.get(previous.size() - 1), outputs.get(i).get(0)) <= 0);
		}
	}

	@Test
	public void compactsAtThresholdWithInterleavedSeriesAndDuplicates() throws Exception {
		table().updateProperties().set("kvin.compaction.min-files", "3").commit();
		List<List<KvinTuple>> batches = List.of(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
		int position = 0;
		for (URI currentItem : List.of(item, URIs.createURI("urn:compaction:other-item"))) {
			for (URI context : List.of(URIs.createURI("urn:compaction:context1"), URIs.createURI("urn:compaction:context2"))) {
				for (URI currentProperty : List.of(property, URIs.createURI("urn:compaction:other-property"))) {
					for (int time = 0; time < 7; time++) {
						for (int sequence = 0; sequence < 3; sequence++) {
							KvinTuple tuple = new KvinTuple(currentItem, currentProperty, context, time, sequence, "value-" + time + "-" + sequence);
							batches.get(position++ % 3).add(tuple);
							if (time == 4 && sequence == 1) {
								batches.get(position++ % 3).add(tuple);
							}
						}
					}
				}
			}
		}
		store.put(batches.get(0));
		store.put(batches.get(1));
		List<DataFile> original = files();
		assertEquals("No rewrite before threshold", 2, original.size());
		store.put(batches.get(2));
		List<DataFile> compacted = files();
		assertEquals(1, compacted.size());
		List<GenericRecord> expected = new ArrayList<>();
		for (DataFile file : files(table().currentSnapshot().parentId())) {
			expected.addAll(rows(file));
		}
		List<GenericRecord> actual = allRows();
		assertEquals(expected.size(), actual.size());
		List<List<Object>> expectedValues = new ArrayList<>();
		for (GenericRecord row : expected) {
			expectedValues.add(identity(row));
		}
		List<List<Object>> actualValues = new ArrayList<>();
		for (GenericRecord row : actual) {
			actualValues.add(identity(row));
		}
		for (List<Object> value : expectedValues) {
			assertTrue("All physical values and duplicates survive", actualValues.remove(value));
		}
		assertTrue(actualValues.isEmpty());
		assertSortedAndMarked(compacted);
		for (DataFile file : original) {
			assertTrue("Files pinned by old readers/snapshots remain available", Files.exists(Path.of(file.location())));
			assertFalse(rows(file).isEmpty());
		}
		store.close();
		store = new KvinIceberg(directory.toString());
		try (IExtendedIterator<KvinTuple> fetched = store.fetch(item, property, URIs.createURI("urn:compaction:context1"), 0)) {
			long previousTime = Long.MAX_VALUE;
			int previousSequence = Integer.MAX_VALUE;
			int count = 0;
			while (fetched.hasNext()) {
				KvinTuple tuple = fetched.next();
				assertTrue(tuple.time < previousTime || tuple.time == previousTime && tuple.seqNr < previousSequence);
				previousTime = tuple.time;
				previousSequence = tuple.seqNr;
				count++;
			}
			assertEquals("Fetch retains its existing duplicate suppression", 21, count);
		}
	}

	@Test
	public void defaultThresholdDoesNotCompactSmallNumberOfFiles() throws Exception {
		for (int i = 0; i < 3; i++) {
			store.put(new KvinTuple(item, property, null, i, 0, i));
		}
		assertEquals(3, files().size());
	}

	@Test
	public void activeIteratorKeepsItsSnapshotWhileCompactionReplacesFiles() throws Exception {
		table().updateProperties().set("kvin.compaction.min-files", "3").commit();
		store.put(new KvinTuple(item, property, null, 5, 0, "five"),
				new KvinTuple(item, property, null, 1, 0, "one"));
		store.put(new KvinTuple(item, property, null, 4, 0, "four"),
				new KvinTuple(item, property, null, 2, 0, "two"));
		List<DataFile> pinnedFiles = files();
		try (IExtendedIterator<KvinTuple> pinned = store.fetch(item, property, null, 0)) {
			assertEquals(5L, pinned.next().time);
			store.put(new KvinTuple(item, property, null, 6, 0, "six"),
					new KvinTuple(item, property, null, 3, 0, "three"));
			assertEquals(1, files().size());
			for (long expected : new long[]{4, 2, 1}) {
				assertTrue(pinned.hasNext());
				assertEquals(expected, pinned.next().time);
			}
			assertFalse("Pinned readers must not observe the triggering append", pinned.hasNext());
		}
		for (DataFile file : pinnedFiles) {
			assertTrue(Files.exists(Path.of(file.location())));
		}
		try (IExtendedIterator<KvinTuple> current = store.fetch(item, property, null, 0)) {
			for (long expected = 6; expected >= 1; expected--) {
				assertTrue(current.hasNext());
				assertEquals(expected, current.next().time);
			}
			assertFalse(current.hasNext());
		}
	}

	@Test
	public void rollsTargetSizedOutputsAndDoesNotRecompactLargeFiles() throws Exception {
		table().updateProperties().set("kvin.compaction.min-files", "100").commit();
		Random random = new Random(42);
		for (int file = 0; file < 4; file++) {
			List<KvinTuple> batch = new ArrayList<>();
			for (int row = 0; row < 256; row++) {
				byte[] value = new byte[512];
				random.nextBytes(value);
				batch.add(new KvinTuple(item, property, null, row * 4L + file, 0, value));
			}
			store.put(batch);
		}
		List<GenericRecord> expected = allRows();
		long maxSize = files().stream().mapToLong(DataFile::fileSizeInBytes).max().orElseThrow();
		long targetSize = maxSize * 2 + 1024;
		table().updateProperties().set("kvin.compaction.min-files", "4")
				.set(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, Long.toString(targetSize)).commit();
		store.put(new KvinTuple(item, property, null, 1024, 0, new byte[]{1, 2, 3}));
		List<DataFile> outputs = files();
		assertTrue("Target size must roll output files", outputs.size() > 1);
		assertTrue("Compaction must reduce file fanout", outputs.size() < 5);
		assertSortedAndMarked(outputs);
		for (DataFile output : outputs) {
			List<GenericRecord> outputRows = rows(output);
			assertEquals("A continuing series starts each output file with a marker", true, outputRows.get(0).get(5));
			assertEquals("Only the first row is marked in a single-series output", 1,
					outputRows.stream().filter(row -> Boolean.TRUE.equals(row.get(5))).count());
		}
		try (IExtendedIterator<URI> properties = store.properties(item, null)) {
			assertTrue(properties.hasNext());
			assertEquals(property, properties.next());
			assertFalse("Per-file markers must not duplicate property results", properties.hasNext());
		}
		List<GenericRecord> actual = allRows();
		assertEquals(expected.size() + 1, actual.size());
		assertEquals(ByteBuffer.wrap(new byte[]{1, 2, 3}), actual.get(0).get(12));
		for (int i = 0; i < expected.size(); i++) {
			assertEquals(identity(expected.get(i)), identity(actual.get(i + 1)));
		}
		table().updateProperties().set("kvin.compaction.min-files", "2").commit();
		Set<String> largeFiles = new HashSet<>();
		for (DataFile file : outputs) {
			if (file.fileSizeInBytes() >= targetSize / 2) {
				largeFiles.add(file.location());
			}
		}
		assertFalse(largeFiles.isEmpty());
		store.put(new KvinTuple(item, property, null, 1025, 0, "tail"));
		Set<String> remainingFiles = new HashSet<>();
		for (DataFile file : files()) {
			remainingFiles.add(file.location());
		}
		assertTrue("Already large files are excluded from later rewrites", remainingFiles.containsAll(largeFiles));
	}

	@Test
	public void compactionFailureDoesNotRollBackCommittedAppend() throws Exception {
		table().updateProperties().set("kvin.compaction.min-files", "invalid").commit();
		assertThrows(NumberFormatException.class, () -> store.put(new KvinTuple(item, property, null, 1, 0, "committed")));
		assertEquals(1, files().size());
		store.close();
		store = new KvinIceberg(directory.toString());
		try (IExtendedIterator<KvinTuple> rows = store.fetch(item, property, null, 0)) {
			assertEquals("committed", rows.next().value);
			assertFalse(rows.hasNext());
		}
	}

	@Test
	public void concurrentDeleteConflictsAndCleansOnlyUncommittedCompactionFiles() throws Exception {
		table().updateProperties().set("kvin.compaction.min-files", "3").commit();
		store.put(new KvinTuple(item, property, null, 1, 0, "deleted"),
				new KvinTuple(item, property, null, 2, 0, "retained"));
		store.put(new KvinTuple(item, property, null, 3, 0, "retained"));
		Table table = spyStoreTable();
		RewriteFiles rewrite = spy(table.newRewrite());
		doReturn(rewrite).when(table).newRewrite();
		Set<DataFile> outputs = new HashSet<>();
		doAnswer(invocation -> {
			outputs.addAll(invocation.getArgument(1));
			return invocation.callRealMethod();
		}).when(rewrite).rewriteFiles(anySet(), anySet());
		doAnswer(invocation -> {
			try (KvinIceberg concurrent = new KvinIceberg(directory.toString())) {
				assertEquals(1, concurrent.delete(item, property, null, 1, 1));
			}
			return invocation.callRealMethod();
		}).when(rewrite).commit();
		assertThrows(ValidationException.class,
				() -> store.put(new KvinTuple(item, property, null, 4, 0, "appended")));
		assertFalse(outputs.isEmpty());
		for (DataFile output : outputs) {
			assertFalse("Only failed rewrite outputs are removed", Files.exists(Path.of(output.location())));
		}
		try (IExtendedIterator<KvinTuple> rows = store.fetch(item, property, null, 0)) {
			assertEquals("appended", rows.next().value);
			assertEquals(3L, rows.next().time);
			assertEquals(2L, rows.next().time);
			assertFalse(rows.hasNext());
		}
	}

	@Test
	public void uncertainCompactionCommitNeverRemovesPotentiallyCommittedFiles() throws Exception {
		table().updateProperties().set("kvin.compaction.min-files", "2").commit();
		store.put(new KvinTuple(item, property, null, 1, 0, "first"));
		Table table = spyStoreTable();
		RewriteFiles rewrite = spy(table.newRewrite());
		doReturn(rewrite).when(table).newRewrite();
		doAnswer(invocation -> {
			invocation.callRealMethod();
			throw new CommitStateUnknownException(new IllegalStateException("Lost commit response"));
		}).when(rewrite).commit();
		assertThrows(CommitStateUnknownException.class,
				() -> store.put(new KvinTuple(item, property, null, 2, 0, "second")));
		assertEquals(1, files().size());
		assertEquals(2, allRows().size());
		store.close();
		store = new KvinIceberg(directory.toString());
		try (IExtendedIterator<KvinTuple> rows = store.fetch(item, property, null, 0)) {
			assertEquals("second", rows.next().value);
			assertEquals("first", rows.next().value);
			assertFalse(rows.hasNext());
		}
	}
}
