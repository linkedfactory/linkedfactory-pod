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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class KvinIcebergCompactionTest {
	private static final Comparator<org.apache.iceberg.data.Record> ORDER = Comparator
			.comparingLong((org.apache.iceberg.data.Record row) -> (Long) row.get(1))
			.thenComparingLong(row -> (Long) row.get(0)).thenComparingLong(row -> (Long) row.get(2))
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

	private IcebergRecordStore recordStore() {
		return (IcebergRecordStore) store.getRecordStore();
	}

	private void awaitCompaction() {
		recordStore().awaitCompaction();
	}

	private Table spyCompactionTable() throws Exception {
		var field = IcebergRecordStore.class.getDeclaredField("compactionTable");
		field.setAccessible(true);
		Table spy = spy((Table) field.get(store.getRecordStore()));
		field.set(store.getRecordStore(), spy);
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
							KvinTuple tuple = new KvinTuple(context, currentItem, currentProperty, time, sequence, "value-" + time + "-" + sequence);
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
		awaitCompaction();
		List<DataFile> original = files();
		assertEquals("No rewrite before threshold", 2, original.size());
		store.put(batches.get(2));
		awaitCompaction();
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
		for (int i = 0; i < 7; i++) {
			store.put(new KvinTuple(null, item, property, i, 0, i));
		}
		awaitCompaction();
		assertEquals(7, files().size());
		store.put(new KvinTuple(null, item, property, 7, 0, 7));
		awaitCompaction();
		assertEquals("The default merges eight files of one tier", 1, files().size());
	}

	@Test
	public void activeIteratorKeepsItsSnapshotWhileCompactionReplacesFiles() throws Exception {
		table().updateProperties().set("kvin.compaction.min-files", "3").commit();
		store.put(new KvinTuple(null, item, property, 5, 0, "five"),
				new KvinTuple(null, item, property, 1, 0, "one"));
		store.put(new KvinTuple(null, item, property, 4, 0, "four"),
				new KvinTuple(null, item, property, 2, 0, "two"));
		awaitCompaction();
		List<DataFile> pinnedFiles = files();
		try (IExtendedIterator<KvinTuple> pinned = store.fetch(item, property, null, 0)) {
			assertEquals(5L, pinned.next().time);
			store.put(new KvinTuple(null, item, property, 6, 0, "six"),
					new KvinTuple(null, item, property, 3, 0, "three"));
			awaitCompaction();
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
				batch.add(new KvinTuple(null, item, property, row * 4L + file, 0, value));
			}
			store.put(batch);
		}
		awaitCompaction();
		List<GenericRecord> expected = allRows();
		long maxSize = files().stream().mapToLong(DataFile::fileSizeInBytes).max().orElseThrow();
		long targetSize = maxSize * 2 + 1024;
		table().updateProperties().set("kvin.compaction.min-files", "4")
				.set(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, Long.toString(targetSize)).commit();
		store.put(new KvinTuple(null, item, property, 1024, 0, new byte[]{1, 2, 3}));
		awaitCompaction();
		List<DataFile> outputs = files();
		assertTrue("Target size must roll output files", outputs.size() > 2);
		assertTrue("Compaction must reduce file fanout", outputs.size() < 5);
		assertTrue("The single new record stays in its own tier",
				outputs.stream().anyMatch(file -> file.recordCount() == 1));
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
		store.put(new KvinTuple(null, item, property, 1025, 0, "tail"));
		awaitCompaction();
		Set<String> remainingFiles = new HashSet<>();
		for (DataFile file : files()) {
			remainingFiles.add(file.location());
		}
		assertTrue("Already large files are excluded from later rewrites", remainingFiles.containsAll(largeFiles));
	}

	@Test
	public void writeBatchSizeControlsDataFilesPerPut() throws Exception {
		// the default keeps a large write in one sorted file
		List<KvinTuple> tuples = new ArrayList<>();
		for (int time = 0; time < 20_000; time++) {
			tuples.add(new KvinTuple(null, item, property, time, 0, (long) time));
		}
		store.put(tuples);
		assertEquals(1, files().size());

		table().updateProperties().set(IcebergRecordStore.WRITE_BATCH_RECORDS, "100").commit();
		List<KvinTuple> more = new ArrayList<>();
		for (int time = 20_000; time < 20_250; time++) {
			more.add(new KvinTuple(null, item, property, time, 0, (long) time));
		}
		store.put(more);
		// 250 records in batches of 100
		assertEquals(4, files().size());
		assertSortedAndMarked(files());
		try (IExtendedIterator<KvinTuple> rows = store.fetch(item, property, null, 0)) {
			assertEquals(20_250, rows.toList().size());
		}
	}

	@Test
	public void invalidWriteBatchSizeIsRejectedBeforeWriting() throws Exception {
		store.put(new KvinTuple(null, item, property, 1, 0, "committed"));
		table().updateProperties().set(IcebergRecordStore.WRITE_BATCH_RECORDS, "0").commit();
		assertThrows(IllegalArgumentException.class, () -> store.put(new KvinTuple(null, item, property, 2, 0, "rejected")));
		assertEquals(1, files().size());
	}

	@Test
	public void compactionFailureDoesNotRollBackCommittedAppend() throws Exception {
		table().updateProperties().set("kvin.compaction.min-files", "invalid").commit();
		// the background compaction fails and logs the failure, the write succeeds
		store.put(new KvinTuple(null, item, property, 1, 0, "committed"));
		awaitCompaction();
		assertThrows(NumberFormatException.class, () -> recordStore().compact());
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
		store.put(new KvinTuple(null, item, property, 1, 0, "deleted"));
		store.put(new KvinTuple(null, item, property, 2, 0, "retained"));
		store.put(new KvinTuple(null, item, property, 3, 0, "retained"));
		awaitCompaction();
		table().updateProperties().set("kvin.compaction.min-files", "3").commit();
		Table table = spyCompactionTable();
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
		assertThrows(ValidationException.class, () -> recordStore().compact());
		assertFalse(outputs.isEmpty());
		for (DataFile output : outputs) {
			assertFalse("Only failed rewrite outputs are removed", Files.exists(Path.of(output.location())));
		}
		try (IExtendedIterator<KvinTuple> rows = store.fetch(item, property, null, 0)) {
			assertEquals(3L, rows.next().time);
			assertEquals(2L, rows.next().time);
			assertFalse(rows.hasNext());
		}
	}

	@Test
	public void uncertainCompactionCommitNeverRemovesPotentiallyCommittedFiles() throws Exception {
		store.put(new KvinTuple(null, item, property, 1, 0, "first"));
		store.put(new KvinTuple(null, item, property, 2, 0, "second"));
		awaitCompaction();
		table().updateProperties().set("kvin.compaction.min-files", "2").commit();
		Table table = spyCompactionTable();
		RewriteFiles rewrite = spy(table.newRewrite());
		doReturn(rewrite).when(table).newRewrite();
		doAnswer(invocation -> {
			invocation.callRealMethod();
			throw new CommitStateUnknownException(new IllegalStateException("Lost commit response"));
		}).when(rewrite).commit();
		assertThrows(CommitStateUnknownException.class, () -> recordStore().compact());
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

	@Test
	public void tierIsTheFloorOfTheLogarithmOfTheRecordCount() {
		assertEquals(0, IcebergRecordStore.tier(0, 8));
		assertEquals(0, IcebergRecordStore.tier(7, 8));
		assertEquals(1, IcebergRecordStore.tier(8, 8));
		assertEquals(1, IcebergRecordStore.tier(63, 8));
		assertEquals(2, IcebergRecordStore.tier(64, 8));
		assertEquals(62, IcebergRecordStore.tier(Long.MAX_VALUE, 2));
	}

	@Test
	public void mergesOnlyFilesOfTheSameTier() throws Exception {
		table().updateProperties().set("kvin.compaction.min-files", "2").commit();
		List<KvinTuple> large = new ArrayList<>();
		for (int time = 0; time < 16; time++) {
			large.add(new KvinTuple(null, item, property, time, 0, (long) time));
		}
		// 16 records: tier 4
		store.put(large);
		// 1 record: tier 0, alone in its tier
		store.put(new KvinTuple(null, item, property, 100, 0, 100L));
		awaitCompaction();
		assertEquals(2, files().size());
		String largeFile = files().stream().filter(file -> file.recordCount() == 16).findFirst().orElseThrow().location();
		// a second tier-0 file completes tier 0, the merged file has 2 records: tier 1
		store.put(new KvinTuple(null, item, property, 101, 0, 101L));
		awaitCompaction();
		List<DataFile> files = files();
		assertEquals(2, files.size());
		assertTrue("Files of other tiers are not rewritten",
				files.stream().anyMatch(file -> file.location().equals(largeFile)));
		assertTrue(files.stream().anyMatch(file -> file.recordCount() == 2));
		try (IExtendedIterator<KvinTuple> rows = store.fetch(item, property, null, 0)) {
			assertEquals(18, rows.toList().size());
		}
	}

	@Test
	public void cascadesMergesThroughHigherTiers() throws Exception {
		table().updateProperties().set("kvin.compaction.min-files", "2").commit();
		// with a fan-in of 2, single-record writes merge into files of 2, 4 and 8 records
		for (int time = 0; time < 8; time++) {
			store.put(new KvinTuple(null, item, property, time, 0, (long) time));
			awaitCompaction();
		}
		List<DataFile> files = files();
		assertEquals(1, files.size());
		assertEquals(8, files.getFirst().recordCount());
		assertSortedAndMarked(files);
	}

	@Test
	public void writesDoNotWaitForARunningCompaction() throws Exception {
		table().updateProperties().set("kvin.compaction.min-files", "2").commit();
		store.put(new KvinTuple(null, item, property, 1, 0, "first"));
		awaitCompaction();
		Table table = spyCompactionTable();
		CountDownLatch started = new CountDownLatch(1);
		CountDownLatch release = new CountDownLatch(1);
		Set<String> threads = java.util.concurrent.ConcurrentHashMap.newKeySet();
		AtomicBoolean blocked = new AtomicBoolean();
		// blocks the first compaction while it plans, before it merges and commits
		doAnswer(newScan -> {
			threads.add(Thread.currentThread().getName());
			if (blocked.compareAndSet(false, true)) {
				started.countDown();
				assertTrue(release.await(30, TimeUnit.SECONDS));
			}
			return newScan.callRealMethod();
		}).when(table).newScan();
		store.put(new KvinTuple(null, item, property, 2, 0, "second"));
		assertTrue(started.await(30, TimeUnit.SECONDS));
		// writes and reads proceed while the compaction is running
		store.put(new KvinTuple(null, item, property, 3, 0, "third"));
		store.put(new KvinTuple(null, item, property, 4, 0, "fourth"));
		try (IExtendedIterator<KvinTuple> rows = store.fetch(item, property, null, 0)) {
			assertEquals(4, rows.toList().size());
		}
		release.countDown();
		awaitCompaction();
		assertEquals("All compactions run in one thread", 1, threads.size());
		assertTrue(threads.iterator().next().startsWith("kvin-iceberg-compaction-"));
		assertEquals("Four single records merge into one file", 1, files().size());
		try (IExtendedIterator<KvinTuple> rows = store.fetch(item, property, null, 0)) {
			assertEquals(List.of(4L, 3L, 2L, 1L), rows.toList().stream().map(tuple -> tuple.time).toList());
		}
	}

	@Test
	public void closeWaitsForARunningCompaction() throws Exception {
		table().updateProperties().set("kvin.compaction.min-files", "2").commit();
		store.put(new KvinTuple(null, item, property, 1, 0, "first"));
		awaitCompaction();
		Table table = spyCompactionTable();
		CountDownLatch started = new CountDownLatch(1);
		AtomicBoolean committed = new AtomicBoolean();
		doAnswer(newRewrite -> {
			RewriteFiles rewrite = spy((RewriteFiles) newRewrite.callRealMethod());
			doAnswer(commit -> {
				started.countDown();
				Thread.sleep(500);
				Object result = commit.callRealMethod();
				committed.set(true);
				return result;
			}).when(rewrite).commit();
			return rewrite;
		}).when(table).newRewrite();
		store.put(new KvinTuple(null, item, property, 2, 0, "second"));
		assertTrue(started.await(30, TimeUnit.SECONDS));
		store.close();
		store = null;
		assertTrue("Close returns after the running compaction committed", committed.get());
		assertEquals(1, files().size());
	}
}
