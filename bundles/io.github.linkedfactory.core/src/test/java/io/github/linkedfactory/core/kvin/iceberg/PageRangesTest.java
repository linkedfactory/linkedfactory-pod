package io.github.linkedfactory.core.kvin.iceberg;

import io.github.linkedfactory.core.kvin.records.KvinRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.io.LocalInputFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.*;

public class PageRangesTest {
	private Path directory;

	@Before
	public void setUp() throws Exception {
		directory = Path.of("target", "iceberg-page-range-tests", UUID.randomUUID().toString()).toAbsolutePath();
		Files.createDirectories(directory);
	}

	@After
	public void tearDown() throws Exception {
		FileUtils.deleteDirectory(directory.toFile());
	}

	@Test
	public void intersectsAndUnitesSortedRanges() {
		long[] a = {0, 4, 10, 19, 30, 30};
		long[] b = {3, 12, 18, 31};
		assertArrayEquals(new long[]{3, 4, 10, 12, 18, 19, 30, 30}, PageRanges.intersect(a, b));
		assertArrayEquals(new long[]{0, 31}, PageRanges.union(a, b));
		assertArrayEquals(new long[]{0, 4, 6, 9}, PageRanges.union(new long[]{6, 9}, new long[]{0, 4}));
		assertArrayEquals(new long[]{0, 9}, PageRanges.union(new long[]{5, 9}, new long[]{0, 4}));
		assertArrayEquals(PageRanges.NONE, PageRanges.intersect(new long[]{0, 4}, new long[]{5, 9}));
		assertArrayEquals(PageRanges.NONE, PageRanges.intersect(PageRanges.NONE, new long[]{5, 9}));
		assertEquals(8, PageRanges.rowCount(new long[]{3, 4, 10, 12, 18, 19, 30, 30}));
	}

	@Test
	public void rangesHoldEveryMatchingRowAndExcludePages() throws Exception {
		String location = directory.resolve("store").toString();
		Random random = new Random(3);
		try (var store = new IcebergRecordStore(KvinIcebergOptions.builder(location)
				.tableProperty(TableProperties.PARQUET_PAGE_SIZE_BYTES, "1024").build())) {
			List<KvinRecord> records = new ArrayList<>();
			for (int i = 0; i < 20_000; i++) {
				records.add(new KvinRecord(random.nextInt(3), random.nextInt(200), random.nextInt(4),
						random.nextInt(100_000), random.nextInt(2), (long) i));
			}
			store.put(records);
			Table table = new HadoopTables(new Configuration()).load(location + "/iceberg");
			try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
				FileScanTask task = tasks.iterator().next();
				List<KvinRow> rows = new ArrayList<>();
				try (var reader = new SeriesSkippingReader(table.schema(), task, Expressions.alwaysTrue(), store.readCaches())) {
					reader.forEachRemaining(rows::add);
				}
				PageRanges.RowGroupIndexes indexes;
				try (var file = ParquetFileReader.open(new LocalInputFile(Path.of(task.file().location())))) {
					assertEquals("a single row group", 1, file.getRowGroups().size());
					indexes = indexes(file);
				}
				for (int i = 0; i < 300; i++) {
					Expression filter = randomFilter(random);
					long[] ranges = PageRanges.compute(filter, indexes, rows.size());
					KeyFilter matcher = KeyFilter.of(table.schema(), filter);
					for (int r = 0; r < rows.size(); r++) {
						KvinRow row = rows.get(r);
						if (matcher.matches(row.itemId(), row.contextId(), row.propertyId(), row.time(), row.seqNr())) {
							assertTrue(filter + " excludes matching row " + r, contains(ranges, r));
						}
					}
				}
				// a fetch of one series reads only some of the pages
				long[] point = PageRanges.compute(Expressions.and(Expressions.equal("contextId", 1L),
						Expressions.in("itemId", 17L)), indexes, rows.size());
				assertTrue("expected few rows, got " + PageRanges.rowCount(point), PageRanges.rowCount(point) < rows.size() / 10);
			}
		}
	}

	private static PageRanges.RowGroupIndexes indexes(ParquetFileReader file) throws Exception {
		var block = file.getRowGroups().getFirst();
		Map<ColumnPath, ColumnIndex> columnIndexes = new HashMap<>();
		Map<ColumnPath, OffsetIndex> offsetIndexes = new HashMap<>();
		for (ColumnChunkMetaData chunk : block.getColumns()) {
			ColumnIndex columnIndex = file.readColumnIndex(chunk);
			if (columnIndex != null) {
				columnIndexes.put(chunk.getPath(), columnIndex);
			}
			offsetIndexes.put(chunk.getPath(), file.readOffsetIndex(chunk));
		}
		return new PageRanges.RowGroupIndexes(new ColumnIndexStore() {
			@Override
			public ColumnIndex getColumnIndex(ColumnPath column) {
				return columnIndexes.get(column);
			}

			@Override
			public OffsetIndex getOffsetIndex(ColumnPath column) {
				return offsetIndexes.get(column);
			}
		}, block.getRowCount());
	}

	private static boolean contains(long[] ranges, long row) {
		for (int i = 0; i < ranges.length; i += 2) {
			if (row >= ranges[i] && row <= ranges[i + 1]) {
				return true;
			}
		}
		return false;
	}

	private static Expression randomFilter(Random random) {
		return switch (random.nextInt(6)) {
			case 0 -> Expressions.and(randomPredicate(random), randomPredicate(random));
			case 1 -> Expressions.or(randomPredicate(random), randomPredicate(random));
			case 2 -> Expressions.not(randomPredicate(random));
			case 3 -> Expressions.and(randomPredicate(random), Expressions.or(randomPredicate(random), randomPredicate(random)));
			default -> randomPredicate(random);
		};
	}

	private static Expression randomPredicate(Random random) {
		String column = new String[]{"itemId", "contextId", "propertyId", "time", "seqNr"}[random.nextInt(5)];
		int bound = switch (column) {
			case "itemId" -> 200;
			case "contextId" -> 3;
			case "propertyId" -> 4;
			case "time" -> 100_000;
			default -> 2;
		};
		Object value = column.equals("seqNr") ? (Object) random.nextInt(bound) : (Object) (long) random.nextInt(bound);
		return switch (random.nextInt(8)) {
			case 0 -> Expressions.equal(column, value);
			case 1 -> Expressions.notEqual(column, value);
			case 2 -> Expressions.lessThan(column, value);
			case 3 -> Expressions.lessThanOrEqual(column, value);
			case 4 -> Expressions.greaterThan(column, value);
			case 5 -> Expressions.greaterThanOrEqual(column, value);
			case 6 -> Expressions.in(column, value, column.equals("seqNr") ? (Object) 1 : (Object) (long) random.nextInt(bound));
			default -> Expressions.isNull(column);
		};
	}
}
