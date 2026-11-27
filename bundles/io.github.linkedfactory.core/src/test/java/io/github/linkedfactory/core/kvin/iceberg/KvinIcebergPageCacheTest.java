package io.github.linkedfactory.core.kvin.iceberg;

import io.github.linkedfactory.core.kvin.records.KvinRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.LocalInputFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.*;

public class KvinIcebergPageCacheTest {
	private Path directory;

	@Before
	public void setUp() throws Exception {
		directory = Path.of("target", "iceberg-page-cache-tests", UUID.randomUUID().toString()).toAbsolutePath();
		Files.createDirectories(directory);
	}

	@After
	public void tearDown() throws Exception {
		FileUtils.deleteDirectory(directory.toFile());
	}

	private String location(String name) {
		return directory.resolve(name).toString();
	}

	private static Table table(String location) {
		return new HadoopTables(new Configuration()).load(location + "/iceberg");
	}

	/**
	 * Values of all types, with several pages per column and series of different lengths.
	 */
	private static List<KvinRecord> records(int count, long seed) {
		Random random = new Random(seed);
		List<KvinRecord> records = new ArrayList<>();
		for (int i = 0; i < count; i++) {
			long item = random.nextInt(50);
			long property = random.nextInt(3);
			Object value = switch (random.nextInt(7)) {
				case 0 -> random.nextInt();
				case 1 -> random.nextLong();
				case 2 -> random.nextFloat();
				case 3 -> random.nextDouble();
				case 4 -> "text-" + random.nextInt(100);
				case 5 -> random.nextBoolean();
				default -> null;
			};
			records.add(new KvinRecord(0, item, property, random.nextInt(1_000_000), random.nextInt(3), value));
		}
		return records;
	}

	private static List<KvinRecord> fetchAll(IcebergRecordStore store, long limit) {
		List<KvinRecord> result = new ArrayList<>();
		for (long item = 0; item < 50; item++) {
			try (var rows = store.fetch(item, null, null, limit)) {
				result.addAll(rows.toList());
			}
		}
		return result;
	}

	@Test
	public void cachedAndUncachedReadsReturnTheSameRows() {
		List<KvinRecord> records = records(30_000, 1);
		try (var cached = new IcebergRecordStore(KvinIcebergOptions.builder(location("cached")).build());
			 var uncached = new IcebergRecordStore(KvinIcebergOptions.builder(location("uncached")).pageCacheSize(0).build())) {
			cached.put(records.subList(0, 20_000));
			cached.put(records.subList(20_000, records.size()));
			uncached.put(records.subList(0, 20_000));
			uncached.put(records.subList(20_000, records.size()));
			for (long limit : new long[]{0, 1, 3}) {
				List<KvinRecord> expected = fetchAll(uncached, limit);
				assertEquals(expected, fetchAll(cached, limit));
				// the second time from the cache
				assertEquals(expected, fetchAll(cached, limit));
			}
			assertEquals(0, uncached.readCaches().pages.size());
			assertTrue(cached.readCaches().pages.size() > 0);
		}
	}

	@Test
	public void repeatedReadsUseCachedPagesAndOneFileHandle() throws Exception {
		try (var store = new IcebergRecordStore(location("store"))) {
			store.put(records(20_000, 2));
			Table table = table(location("store"));
			ReadCaches caches = store.readCaches();
			List<KvinRow> first = new ArrayList<>();
			try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
				FileScanTask task = tasks.iterator().next();
				try (var reader = new SeriesSkippingReader(table.schema(), task, Expressions.equal("itemId", 7L), caches)) {
					reader.forEachRemaining(first::add);
					assertFalse(reader.pagesRead.isEmpty());
				}
				try (var reader = new SeriesSkippingReader(table.schema(), task, Expressions.equal("itemId", 7L), caches)) {
					List<KvinRow> second = new ArrayList<>();
					reader.forEachRemaining(second::add);
					assertEquals(first, second);
					assertEquals("pages come from the cache", java.util.Map.of(), reader.pagesRead);
				}
			}
			assertFalse(first.isEmpty());
			assertEquals("the file stays open for later reads", 1, caches.handles.openFiles());
		}
	}

	@Test
	public void compactionAndDeletesDropCachedPagesOfReplacedFiles() throws Exception {
		String location = location("store");
		try (var store = new IcebergRecordStore(location)) {
			table(location).updateProperties().set("kvin.compaction.min-files", "2").commit();
			store.put(records(5_000, 3));
			fetchAll(store, 0);
			List<String> before = files(location);
			store.put(records(5_000, 4));
			store.awaitCompaction();
			List<String> after = files(location);
			assertEquals(1, after.size());
			for (var key : store.readCaches().pages.asMap().keySet()) {
				assertFalse("pages of compacted files are dropped", before.contains(key.location()));
			}
			fetchAll(store, 0);
			assertTrue(store.readCaches().pages.asMap().keySet().stream().allMatch(key -> after.contains(key.location())));

			assertTrue(store.delete(7, null, 0L, Long.MAX_VALUE, Long.MIN_VALUE) > 0);
			for (var key : store.readCaches().pages.asMap().keySet()) {
				assertFalse("pages of rewritten files are dropped", after.contains(key.location()));
			}
			try (var rows = store.fetch(7, null, null, 0)) {
				assertFalse(rows.hasNext());
			}
		}
	}

	private static List<String> files(String location) throws Exception {
		List<String> files = new ArrayList<>();
		try (CloseableIterable<FileScanTask> tasks = table(location).newScan().planFiles()) {
			tasks.forEach(task -> files.add(task.file().location()));
		}
		return files;
	}

	@Test
	public void handlesAreSharedAndClosedAfterTheirLastReader() throws Exception {
		Path file = directory.resolve("data.bin");
		Files.write(file, new byte[]{1, 2, 3, 4, 5, 6, 7, 8});
		try (var handles = new DataFileHandles(new Configuration(), 2)) {
			var first = handles.acquire(file.toString());
			var second = handles.acquire(file.toString());
			assertSame(first, second);
			handles.invalidate(file.toString());
			// still readable by its readers after it was dropped from the cache
			byte[] bytes = new byte[2];
			first.read(List.of(new DataFileHandles.Range(3, bytes)));
			assertArrayEquals(new byte[]{4, 5}, bytes);
			first.close();
			second.read(List.of(new DataFileHandles.Range(6, bytes)));
			assertArrayEquals(new byte[]{7, 8}, bytes);
			second.close();
			assertThrows(java.nio.channels.ClosedChannelException.class,
					() -> second.read(List.of(new DataFileHandles.Range(0, bytes))));

			var third = handles.acquire(file.toString());
			assertNotSame(first, third);
			third.read(List.of(new DataFileHandles.Range(0, bytes)));
			assertArrayEquals(new byte[]{1, 2}, bytes);
			third.close();
		}
	}

	@Test
	public void aChannelClosedByAnotherThreadsInterruptIsReopened() throws Exception {
		Path file = directory.resolve("data.bin");
		Files.write(file, new byte[]{1, 2, 3, 4});
		try (var handles = new DataFileHandles(new Configuration(), 2)) {
			var handle = handles.acquire(file.toString());
			byte[] bytes = new byte[2];
			// an interrupted reader closes the channel for all readers
			Thread.currentThread().interrupt();
			assertThrows(ClosedByInterruptException.class, () -> handle.read(List.of(new DataFileHandles.Range(0, bytes))));
			assertTrue(Thread.interrupted());
			// another reader reopens it
			var field = handle.getClass().getDeclaredField("channel");
			field.setAccessible(true);
			assertFalse(((FileChannel) field.get(handle)).isOpen());
			handle.read(List.of(new DataFileHandles.Range(2, bytes)));
			assertArrayEquals(new byte[]{3, 4}, bytes);
			handle.close();
		}
	}

	@Test
	public void readsPagesOfOtherCodecsAndVersionOnePages() throws Exception {
		List<KvinRecord> records = records(10_000, 5);
		List<KvinRecord> expected;
		try (var reference = new IcebergRecordStore(location("reference"))) {
			reference.put(records);
			expected = fetchAll(reference, 0);
		}
		for (String codec : new String[]{"uncompressed", "snappy", "gzip"}) {
			String location = location(codec);
			try (var store = new IcebergRecordStore(KvinIcebergOptions.builder(location)
					.tableProperty(TableProperties.PARQUET_COMPRESSION, codec).build())) {
				store.put(records);
				assertEquals(codec, expected, fetchAll(store, 0));
			}
		}
		// data file with version 1 pages, as written by Iceberg's own Parquet writer
		String location = location("v1");
		try (var store = new IcebergRecordStore(location)) {
			Table table = table(location);
			table.newAppend().appendFile(writeVersionOneFile(table, records)).commit();
			assertEquals(expected, fetchAll(store, 0));
		}
	}

	private static DataFile writeVersionOneFile(Table table, List<KvinRecord> records) throws Exception {
		List<KvinRecord> sorted = new ArrayList<>(records);
		sorted.sort(Comparator.comparingLong(KvinRecord::contextId).thenComparingLong(KvinRecord::itemId)
				.thenComparingLong(KvinRecord::propertyId).thenComparing(Comparator.comparingLong(KvinRecord::time).reversed())
				.thenComparing(Comparator.comparingInt(KvinRecord::seqNr).reversed()));
		String path = table.location() + "/data/" + UUID.randomUUID() + ".parquet";
		DataWriter<org.apache.iceberg.data.Record> writer = Parquet.writeData(table.io().newOutputFile(path))
				.forTable(table).withSpec(PartitionSpec.unpartitioned())
				.set(TableProperties.PARQUET_PAGE_SIZE_BYTES, "4096")
				.createWriterFunc(message -> GenericParquetWriter.create(table.schema(), message))
				.build();
		try (writer) {
			KvinRecord previous = null;
			for (KvinRecord record : sorted) {
				GenericRecord row = GenericRecord.create(table.schema());
				row.setField("itemId", record.itemId());
				row.setField("contextId", record.contextId());
				row.setField("propertyId", record.propertyId());
				row.setField("time", record.time());
				row.setField("seqNr", record.seqNr());
				row.setField("first", previous == null || previous.itemId() != record.itemId()
						|| previous.propertyId() != record.propertyId());
				Object value = record.value();
				String column = value instanceof Integer ? "valueInt" : value instanceof Long ? "valueLong"
						: value instanceof Float ? "valueFloat" : value instanceof Double ? "valueDouble"
						: value instanceof String ? "valueString" : value instanceof Boolean ? "valueBool" : null;
				if (column != null) {
					row.setField(column, value);
				}
				writer.write(row);
				previous = record;
			}
		}
		try (var file = ParquetFileReader.open(new LocalInputFile(Path.of(path)))) {
			var encodings = file.getRowGroups().getFirst().getColumns().getFirst().getEncodingStats();
			assertFalse("expected version 1 pages", encodings.usesV2Pages());
		}
		return writer.toDataFile();
	}

	@Test
	public void offsetIndexesBuiltFromPageHeadersMatchTheStoredOnes() throws Exception {
		try (var store = new IcebergRecordStore(location("store"))) {
			store.put(records(20_000, 6));
			Table table = table(location("store"));
			try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
				FileScanTask task = tasks.iterator().next();
				try (var reader = new SeriesSkippingReader(table.schema(), task, Expressions.alwaysTrue(), store.readCaches());
					 var file = ParquetFileReader.open(new LocalInputFile(Path.of(task.file().location())))) {
					for (ColumnChunkMetaData chunk : file.getRowGroups().getFirst().getColumns()) {
						var stored = file.readOffsetIndex(chunk);
						var built = reader.scanOffsetIndex(chunk);
						assertEquals(chunk.getPath().toDotString(), stored.getPageCount(), built.getPageCount());
						for (int page = 0; page < stored.getPageCount(); page++) {
							assertEquals(stored.getOffset(page), built.getOffset(page));
							assertEquals(stored.getCompressedPageSize(page), built.getCompressedPageSize(page));
							assertEquals(stored.getFirstRowIndex(page), built.getFirstRowIndex(page));
						}
					}
				}
			}
		}
	}

	@Test
	public void concurrentFetchesShareTheCaches() throws Exception {
		try (var store = new IcebergRecordStore(KvinIcebergOptions.builder(location("store"))
				.pageCacheSize(256 * 1024).build())) {
			store.put(records(40_000, 7));
			List<KvinRecord> expected = fetchAll(store, 2);
			var executor = Executors.newFixedThreadPool(4);
			try {
				List<Future<List<KvinRecord>>> results = new ArrayList<>();
				for (int i = 0; i < 8; i++) {
					results.add(executor.submit((Callable<List<KvinRecord>>) () -> fetchAll(store, 2)));
				}
				for (var result : results) {
					assertEquals(expected, result.get());
				}
			} finally {
				executor.shutdownNow();
			}
		}
	}

	@Test
	public void searchingSortedFilesReturnsTheRowsOfScanning() throws Exception {
		Random random = new Random(8);
		List<KvinRecord> records = new ArrayList<>();
		for (int i = 0; i < 30_000; i++) {
			records.add(new KvinRecord(random.nextInt(3), random.nextInt(300), random.nextInt(4),
					random.nextInt(1000), random.nextInt(2), (long) i));
		}
		try (var store = new IcebergRecordStore(location("store"))) {
			store.put(records);
			Table table = table(location("store"));
			try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
				FileScanTask task = tasks.iterator().next();
				long scanned = 0;
				long searched = 0;
				for (int i = 0; i < 200; i++) {
					List<Object> items = new ArrayList<>();
					for (int n = 1 + random.nextInt(4); n > 0; n--) {
						items.add((long) random.nextInt(320));
					}
					org.apache.iceberg.expressions.Expression filter = Expressions.and(
							Expressions.in("itemId", items), Expressions.equal("contextId", (long) random.nextInt(4)));
					if (random.nextBoolean()) {
						filter = Expressions.and(filter, Expressions.in("propertyId", (long) random.nextInt(5), (long) random.nextInt(5)));
					}
					if (random.nextBoolean()) {
						filter = Expressions.and(filter, Expressions.lessThanOrEqual("time", (long) random.nextInt(1000)));
					}
					boolean limited = random.nextBoolean();
					var expected = read(table, task, filter, store.readCaches(), false, limited);
					var actual = read(table, task, filter, store.readCaches(), true, limited);
					assertEquals(filter.toString(), expected.rows, actual.rows);
					scanned += expected.scanned;
					searched += actual.scanned;
				}
				assertTrue("searching looks at fewer rows: " + searched + " of " + scanned, searched * 5 < scanned);
			}
		}
	}

	private record Read(List<KvinRow> rows, long scanned) {
	}

	/**
	 * Reads all rows, or with {@code limited} only the first row of each series, as fetches
	 * with a limit of 1 do.
	 */
	private static Read read(Table table, FileScanTask task, org.apache.iceberg.expressions.Expression filter,
			ReadCaches caches, boolean sorted, boolean limited) throws Exception {
		List<KvinRow> rows = new ArrayList<>();
		try (var reader = new SeriesSkippingReader(table.schema(), task, filter, caches, sorted)) {
			while (reader.hasNext()) {
				KvinRow row = reader.next();
				rows.add(row);
				if (limited) {
					reader.skipThrough(row.contextId(), row.itemId(), row.propertyId());
				}
			}
			return new Read(rows, reader.rowsScanned);
		}
	}
}
