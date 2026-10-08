package io.github.linkedfactory.core.kvin.iceberg;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.Record;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class KvinIcebergTest {
	private File directory;
	private KvinIceberg store;
	private final URI item = URIs.createURI("urn:test:item");
	private final URI other = URIs.createURI("urn:test:other");
	private final URI property = URIs.createURI("urn:test:property");

	@Before
	public void setUp() throws Exception {
		directory = Files.createTempDirectory("kvin-iceberg").toFile();
		store = new KvinIceberg(directory.toString());
	}

	@After
	public void tearDown() throws Exception {
		store.close();
		FileUtils.deleteDirectory(directory);
	}

	@Test
	public void reopensEmptyTableBeforeFirstWrite() {
		store.close();
		store = new KvinIceberg(directory.toString());
		try (IExtendedIterator<KvinTuple> values = store.fetch(item, property, null, 0)) {
			assertFalse(values.hasNext());
		}
	}

	private Table valueTable() {
		return new HadoopTables(new Configuration()).load(directory.toPath().resolve("iceberg").toString());
	}

	private List<Table> storeTables() throws ReflectiveOperationException {
		var tableField = KvinIceberg.class.getDeclaredField("table");
		tableField.setAccessible(true);
		List<Table> tables = new ArrayList<>();
		tables.add((Table) tableField.get(store));
		var idsField = KvinIceberg.class.getDeclaredField("ids");
		idsField.setAccessible(true);
		for (Object ids : (Object[]) idsField.get(store)) {
			var idTableField = ids.getClass().getDeclaredField("table");
			idTableField.setAccessible(true);
			tables.add((Table) idTableField.get(ids));
		}
		return tables;
	}

	private Object idTable(int kind) throws ReflectiveOperationException {
		var field = KvinIceberg.class.getDeclaredField("ids");
		field.setAccessible(true);
		return ((Object[]) field.get(store))[kind];
	}

	private Table spyIdTable(int kind) throws ReflectiveOperationException {
		Object ids = idTable(kind);
		var field = ids.getClass().getDeclaredField("table");
		field.setAccessible(true);
		Table spy = spy((Table) field.get(ids));
		field.set(ids, spy);
		return spy;
	}

	private PriorityQueue<?> cursors(IExtendedIterator<KvinTuple> values) throws ReflectiveOperationException {
		for (var field : values.getClass().getDeclaredFields()) {
			if (field.getType() == PriorityQueue.class) {
				field.setAccessible(true);
				return (PriorityQueue<?>) field.get(values);
			}
		}
		throw new AssertionError("Missing merge queue");
	}

	@Test
	public void singleSeriesLimitClosesReadersWithoutAdvancing() throws Exception {
		store.put(new KvinTuple(item, property, null, 3, 0, "new"),
				new KvinTuple(item, property, null, 1, 0, "old"));
		store.put(new KvinTuple(item, property, null, 2, 0, "middle"));
		try (IExtendedIterator<KvinTuple> values = store.fetch(item, property, null, 1)) {
			PriorityQueue<?> cursors = cursors(values);
			assertEquals(2, cursors.size());
			List<CloseableIterator<?>> readers = new ArrayList<>();
			List<CloseableIterable<?>> resources = new ArrayList<>();
			for (Object cursor : cursors) {
				var field = cursor.getClass().getDeclaredField("iterator");
				field.setAccessible(true);
				CloseableIterator<?> reader = mock(CloseableIterator.class);
				field.set(cursor, reader);
				readers.add(reader);
				var rowsField = cursor.getClass().getDeclaredField("rows");
				rowsField.setAccessible(true);
				CloseableIterable<?> rows = spy((CloseableIterable<?>) rowsField.get(cursor));
				rowsField.set(cursor, rows);
				resources.add(rows);
			}
			assertTrue(values.hasNext());
			assertTrue(values.hasNext());
			assertTrue(cursors.isEmpty());
			assertEquals("new", values.next().value);
			assertFalse(values.hasNext());
			for (CloseableIterator<?> reader : readers) {
				verifyNoInteractions(reader);
			}
			values.close();
			for (CloseableIterable<?> rows : resources) {
				verify(rows, times(1)).close();
			}
		}
	}

	@Test
	public void deferredReadFailureClosesAllReaders() throws Exception {
		store.put(new KvinTuple(item, property, null, 3, 0, 3),
				new KvinTuple(item, property, null, 1, 0, 1));
		store.put(new KvinTuple(item, property, null, 2, 0, 2));
		try (IExtendedIterator<KvinTuple> values = store.fetch(item, property, null, 0)) {
			PriorityQueue<?> cursors = cursors(values);
			List<CloseableIterable<?>> resources = new ArrayList<>();
			IllegalStateException failure = new IllegalStateException("read failed");
			for (Object cursor : cursors) {
				var field = cursor.getClass().getDeclaredField("iterator");
				field.setAccessible(true);
				CloseableIterator<?> reader = mock(CloseableIterator.class);
				when(reader.hasNext()).thenThrow(failure);
				field.set(cursor, reader);
				var rowsField = cursor.getClass().getDeclaredField("rows");
				rowsField.setAccessible(true);
				CloseableIterable<?> rows = spy((CloseableIterable<?>) rowsField.get(cursor));
				rowsField.set(cursor, rows);
				resources.add(rows);
			}
			assertEquals(3L, values.next().time);
			assertSame(failure, assertThrows(IllegalStateException.class, values::hasNext));
			assertTrue(cursors.isEmpty());
			assertFalse(values.hasNext());
			for (CloseableIterable<?> rows : resources) {
				verify(rows, times(1)).close();
			}
		}
	}

	@Test
	public void explicitMultiSeriesLimitsCloseReadersAndIgnoreDuplicates() throws Exception {
		URI secondProperty = URIs.createURI("urn:test:second");
		for (int round = 0; round < 2; round++) {
			store.put(
					new KvinTuple(item, property, null, 3, 0, 3),
					new KvinTuple(item, property, null, 2, 0, 2),
					new KvinTuple(item, property, null, 1, 0, 1),
					new KvinTuple(item, secondProperty, null, 3, 0, 3),
					new KvinTuple(item, secondProperty, null, 2, 0, 2),
					new KvinTuple(item, secondProperty, null, 1, 0, 1),
					new KvinTuple(other, property, null, 3, 0, 3),
					new KvinTuple(other, property, null, 2, 0, 2),
					new KvinTuple(other, property, null, 1, 0, 1),
					new KvinTuple(other, secondProperty, null, 3, 0, 3),
					new KvinTuple(other, secondProperty, null, 2, 0, 2),
					new KvinTuple(other, secondProperty, null, 1, 0, 1));
		}
		try (IExtendedIterator<KvinTuple> values = store.fetch(List.of(item, other),
				List.of(property, secondProperty), null, 3, 1, 2, 0, null)) {
			PriorityQueue<?> cursors = cursors(values);
			List<KvinTuple> result = new ArrayList<>();
			for (int i = 0; i < 8; i++) {
				result.add(values.next());
			}
			assertTrue(cursors.isEmpty());
			assertFalse(values.hasNext());
			assertEquals(List.of(3L, 2L, 3L, 2L, 3L, 2L, 3L, 2L),
					result.stream().map(tuple -> tuple.time).toList());
			assertEquals(List.of(item, item, item, item, other, other, other, other),
					result.stream().map(tuple -> tuple.item).toList());
			assertEquals(List.of(property, property, secondProperty, secondProperty,
					property, property, secondProperty, secondProperty),
					result.stream().map(tuple -> tuple.property).toList());
		}
	}

	@Test
	public void batchesColdUriLookupsAndReusesCaches() throws Exception {
		URI secondProperty = URIs.createURI("urn:test:second");
		URI missing = URIs.createURI("urn:test:missing");
		store.put(new KvinTuple(item, property, null, 1, 0, 1),
				new KvinTuple(other, secondProperty, null, 2, 0, 2));
		store.close();
		store = new KvinIceberg(directory.toString());
		Table items = spyIdTable(0);
		Table properties = spyIdTable(2);
		for (int round = 0; round < 2; round++) {
			try (IExtendedIterator<KvinTuple> values = store.fetch(List.of(item, other, item, missing),
					List.of(property, secondProperty, property, missing), null, 2, 1, 1, 0, null)) {
				assertEquals(List.of(1, 2), values.toList().stream().map(tuple -> tuple.value).toList());
			}
		}
		// Missing URIs are not negatively cached and must be checked again.
		verify(items, times(2)).newScan();
		verify(properties, times(2)).newScan();
		try (IExtendedIterator<KvinTuple> values = store.fetch(List.of(item, other),
				List.of(property, secondProperty), null, 2, 1, 0, 0, null)) {
			assertEquals(2, values.toList().size());
		}
		verify(items, times(2)).newScan();
		verify(properties, times(2)).newScan();
	}

	@Test
	public void batchesColdReversePropertyLookupsFromFileHeads() throws Exception {
		URI secondProperty = URIs.createURI("urn:test:second");
		store.put(new KvinTuple(item, property, null, 1, 0, 1));
		store.put(new KvinTuple(item, secondProperty, null, 2, 0, 2));
		store.close();
		store = new KvinIceberg(directory.toString());
		Table properties = spyIdTable(2);
		try (IExtendedIterator<KvinTuple> values = store.fetch(item, null, null, 0)) {
			assertEquals(List.of(property, secondProperty),
					values.toList().stream().map(tuple -> tuple.property).toList());
		}
		verify(properties, times(1)).newScan();
	}

	@Test
	public void missingMappingsBecomeVisibleAfterAnotherInstanceWrites() {
		URI newProperty = URIs.createURI("urn:test:new-property");
		store.put(new KvinTuple(item, property, null, 1, 0, 1));
		try (IExtendedIterator<KvinTuple> values = store.fetch(List.of(item, other),
				List.of(newProperty), null, 2, 0, 0, 0, null)) {
			assertFalse(values.hasNext());
		}
		try (KvinIceberg second = new KvinIceberg(directory.toString())) {
			second.put(new KvinTuple(other, newProperty, null, 2, 0, 2));
		}
		try (IExtendedIterator<KvinTuple> values = store.fetch(List.of(item, other),
				List.of(newProperty), null, 2, 0, 0, 0, null)) {
			List<KvinTuple> result = values.toList();
			assertEquals(1, result.size());
			assertEquals(other, result.get(0).item);
			assertEquals(newProperty, result.get(0).property);
			assertEquals(2, result.get(0).value);
		}
	}

	@Test
	public void insufficientAndWildcardSeriesStillReturnAllAvailableSeries() {
		URI secondProperty = URIs.createURI("urn:test:second");
		store.put(new KvinTuple(item, property, null, 2, 0, 2),
				new KvinTuple(item, property, null, 1, 0, 1),
				new KvinTuple(other, secondProperty, null, 1, 0, 1));
		for (List<URI> properties : List.of(List.<URI>of(), List.of(property, secondProperty))) {
			try (IExtendedIterator<KvinTuple> values = store.fetch(List.of(item, other),
					properties, null, 2, 1, 2, 0, null)) {
				assertEquals(List.of(item, item, other),
						values.toList().stream().map(tuple -> tuple.item).toList());
			}
		}
	}

	@Test
	public void batchedLookupsRejectConflictingMappings() throws Exception {
		store.put(new KvinTuple(item, property, null, 1, 0, 1));
		Object items = idTable(0);
		var append = items.getClass().getDeclaredMethod("append", Map.class);
		append.setAccessible(true);
		append.invoke(items, Map.of(item, 999L));
		store.close();
		store = new KvinIceberg(directory.toString());
		IllegalStateException failure = assertThrows(IllegalStateException.class,
				() -> store.fetch(List.of(item, other), List.of(property), null, 1, 0, 0, 0, null));
		assertTrue(failure.getMessage().contains("Duplicate Iceberg URI mapping"));
	}

	@Test
	public void batchedReverseLookupsRejectConflictingMappings() throws Exception {
		store.put(new KvinTuple(item, property, null, 1, 0, 1));
		Object properties = idTable(2);
		var append = properties.getClass().getDeclaredMethod("append", Map.class);
		append.setAccessible(true);
		append.invoke(properties, Map.of(URIs.createURI("urn:test:conflicting"), 1L));
		store.close();
		store = new KvinIceberg(directory.toString());
		try (IExtendedIterator<KvinTuple> values = store.fetch(item, null, null, 0)) {
			IllegalStateException failure = assertThrows(IllegalStateException.class, values::hasNext);
			assertTrue(failure.getMessage().contains("Duplicate Iceberg ID mapping"));
		}
	}

	@Test
	public void aggregationLimitsApplyToCompleteIntervalsAcrossSeries() {
		URI secondProperty = URIs.createURI("urn:test:second");
		store.put(new KvinTuple(item, property, null, 29, 0, 4),
				new KvinTuple(item, property, null, 21, 0, 2),
				new KvinTuple(item, property, null, 19, 0, 99),
				new KvinTuple(item, secondProperty, null, 29, 0, 8),
				new KvinTuple(item, secondProperty, null, 21, 0, 2));
		store.put(new KvinTuple(item, property, null, 29, 0, 4));
		try (IExtendedIterator<KvinTuple> values = store.fetch(List.of(item),
				List.of(property, secondProperty), null, 29, 0, 1, 10, "sum")) {
			List<KvinTuple> result = values.toList();
			assertEquals(List.of(6, 10), result.stream().map(tuple -> tuple.value).toList());
			assertEquals(List.of(20L, 20L), result.stream().map(tuple -> tuple.time).toList());
			assertEquals(List.of(property, secondProperty),
					result.stream().map(tuple -> tuple.property).toList());
		}
		try (IExtendedIterator<KvinTuple> values = store.fetch(item, property, null, 29, 0, 0, 10, "sum")) {
			assertEquals(List.of(6, 99), values.toList().stream().map(tuple -> tuple.value).toList());
		}
	}

	@Test
	public void deferredReadersKeepTheirSnapshotAcrossDeletes() {
		store.put(new KvinTuple(item, property, null, 3, 0, 3),
				new KvinTuple(item, property, null, 2, 0, 2),
				new KvinTuple(item, property, null, 1, 0, 1));
		try (IExtendedIterator<KvinTuple> snapshot = store.fetch(item, property, null, 0)) {
			assertEquals(1, store.delete(item, property, null, 2, 2));
			assertEquals(List.of(3, 2, 1), snapshot.toList().stream().map(tuple -> tuple.value).toList());
		}
		try (IExtendedIterator<KvinTuple> current = store.fetch(item, property, null, 0)) {
			assertEquals(List.of(3, 1), current.toList().stream().map(tuple -> tuple.value).toList());
		}
		try (IExtendedIterator<KvinTuple> limited = store.fetch(item, property, null, 1)) {
			assertEquals(List.of(3), limited.toList().stream().map(tuple -> tuple.value).toList());
		}
	}

	@Test
	public void cachesManifestsForAllTablesOnCreationAndReopening() throws Exception {
		store.put(new KvinTuple(item, property, null, 1, 0, "value"));
		for (int round = 0; round < 2; round++) {
			List<Table> tables = storeTables();
			for (Table table : tables) {
				assertEquals("true", table.io().properties().get(CatalogProperties.IO_MANIFEST_CACHE_ENABLED));
				ManifestFiles.dropCache(table.io());
				try (var files = table.newScan().planFiles()) {
					assertTrue(files.iterator().hasNext());
				}
				var first = ManifestFiles.contentCacheStats(table.io());
				assertTrue(first.missCount() > 0);
				try (var files = table.newScan().planFiles()) {
					assertTrue(files.iterator().hasNext());
				}
				var second = ManifestFiles.contentCacheStats(table.io());
				assertTrue(second.hitCount() > first.hitCount());
				assertEquals(first.missCount(), second.missCount());
			}
			store.close();
			for (Table table : tables) {
				var stats = ManifestFiles.contentCacheStats(table.io());
				assertEquals(0, stats.hitCount());
				assertEquals(0, stats.missCount());
				ManifestFiles.dropCache(table.io());
			}
			store = new KvinIceberg(directory.toString());
		}
	}

	@Test
	public void cachedManifestsDoNotHideWritesFromOtherInstances() {
		store.put(new KvinTuple(item, property, null, 1, 0, "old"));
		try (IExtendedIterator<KvinTuple> values = store.fetch(item, property, null, 0)) {
			assertEquals(List.of("old"), values.toList().stream().map(tuple -> tuple.value).toList());
		}
		try (KvinIceberg second = new KvinIceberg(directory.toString())) {
			second.put(
					new KvinTuple(item, property, null, 2, 0, "new"),
					new KvinTuple(other, property, null, 3, 0, "other"));
			try (IExtendedIterator<KvinTuple> values = store.fetch(item, property, null, 0)) {
				assertEquals(List.of("new", "old"), values.toList().stream().map(tuple -> tuple.value).toList());
			}
			try (IExtendedIterator<KvinTuple> values = store.fetch(other, property, null, 0)) {
				assertEquals(List.of("other"), values.toList().stream().map(tuple -> tuple.value).toList());
			}
		}
	}

	private void assertValueSortOrder(Table table) {
		SortOrder expected = SortOrder.builderFor(table.schema())
				.asc("itemId").asc("contextId").asc("propertyId").desc("time").desc("seqNr").build();
		assertTrue(table.sortOrder().sameOrder(expected));
		assertTrue(table.sortOrder().orderId() > 0);
	}

	@Test
	public void recordsSortOrderInTableAndFileMetadata() throws Exception {
		assertValueSortOrder(valueTable());
		store.put(new KvinTuple(item, property, null, 1, 0, "value"));
		store.close();
		store = new KvinIceberg(directory.toString());
		Table table = valueTable();
		assertValueSortOrder(table);
		try (var files = table.newScan().planFiles()) {
			int count = 0;
			for (var task : files) {
				assertEquals(Integer.valueOf(table.sortOrder().orderId()), task.file().sortOrderId());
				count++;
			}
			assertEquals(1, count);
		}
		for (String name : List.of("items", "contexts", "properties")) {
			Table ids = new HadoopTables(new Configuration()).load(directory.toPath().resolve("iceberg-ids").resolve(name).toString());
			assertTrue(ids.sortOrder().isUnsorted());
			try (var files = ids.newScan().planFiles()) {
				for (var task : files) {
					assertEquals(Integer.valueOf(0), task.file().sortOrderId());
				}
			}
		}
	}

	@Test
	public void upgradesExistingSortOrderWithoutRewritingFiles() throws Exception {
		store.put(new KvinTuple(item, property, null, 1, 0, "old"));
		store.close();
		Table table = valueTable();
		long snapshotId = table.currentSnapshot().snapshotId();
		List<String> oldPaths = new ArrayList<>();
		List<Integer> oldOrders = new ArrayList<>();
		try (var files = table.newScan().planFiles()) {
			for (var task : files) {
				oldPaths.add(task.file().location());
				oldOrders.add(task.file().sortOrderId());
			}
		}
		table.replaceSortOrder().commit();
		assertTrue(table.sortOrder().isUnsorted());
		store = new KvinIceberg(directory.toString());
		table.refresh();
		assertValueSortOrder(table);
		assertEquals(snapshotId, table.currentSnapshot().snapshotId());
		try (var files = table.newScan().planFiles()) {
			int count = 0;
			for (var task : files) {
				assertEquals(oldPaths.get(count), task.file().location());
				assertEquals(oldOrders.get(count), task.file().sortOrderId());
				count++;
			}
			assertEquals(oldPaths.size(), count);
		}
		store.put(new KvinTuple(item, property, null, 2, 0, "new"));
		table.refresh();
		try (var files = table.newScan().planFiles()) {
			for (var task : files) {
				if (!oldPaths.contains(task.file().location())) {
					assertEquals(Integer.valueOf(table.sortOrder().orderId()), task.file().sortOrderId());
				}
			}
		}
		try (IExtendedIterator<KvinTuple> values = store.fetch(item, property, null, 0)) {
			assertEquals(List.of("new", "old"), values.toList().stream().map(tuple -> tuple.value).toList());
		}
	}

	@Test
	public void mergesSequenceNumbersDescendingAcrossFilesAndDeduplicates() {
		store.put(
				new KvinTuple(item, property, null, 100, 1, "one"),
				new KvinTuple(item, property, null, 100, 3, "three"),
				new KvinTuple(item, property, null, 90, 5, "older"));
		store.put(
				new KvinTuple(item, property, null, 100, 2, "two"),
				new KvinTuple(item, property, null, 100, 3, "three"),
				new KvinTuple(item, property, null, 100, 0, "zero"));
		try (IExtendedIterator<KvinTuple> values = store.fetch(item, property, null, 0)) {
			List<KvinTuple> tuples = values.toList();
			assertEquals(List.of(3, 2, 1, 0, 5), tuples.stream().map(tuple -> tuple.seqNr).toList());
			assertEquals(List.of(100L, 100L, 100L, 100L, 90L), tuples.stream().map(tuple -> tuple.time).toList());
		}
		try (IExtendedIterator<KvinTuple> values = store.fetch(item, property, null, 2)) {
			assertEquals(List.of(3, 2), values.toList().stream().map(tuple -> tuple.seqNr).toList());
		}
	}

	@Test
	public void typedValuesAndSnapshotReadsSurviveReopening() {
		Record object = new Record(URIs.createURI("urn:test:field"), true);
		store.put(
				new KvinTuple(item, property, Kvin.DEFAULT_CONTEXT, 100, 0, 10),
				new KvinTuple(item, property, Kvin.DEFAULT_CONTEXT, 200, 0, 20L),
				new KvinTuple(item, property, Kvin.DEFAULT_CONTEXT, 300, 0, "hello"),
				new KvinTuple(item, property, Kvin.DEFAULT_CONTEXT, 400, 0, object),
				new KvinTuple(other, property, Kvin.DEFAULT_CONTEXT, 500, 0, 2.5));
		store.put(new KvinTuple(item, property, Kvin.DEFAULT_CONTEXT, 200, 0, 20L));

		store.close();
		store = new KvinIceberg(directory.toString());
		try (IExtendedIterator<KvinTuple> values = store.fetch(item, property, null, 0)) {
			List<KvinTuple> tuples = values.toList();
			assertEquals(4, tuples.size());
			assertEquals(object, tuples.get(0).value);
			assertEquals("hello", tuples.get(1).value);
			assertEquals(20L, tuples.get(2).value);
			assertEquals(10, tuples.get(3).value);
		}
		try (IExtendedIterator<KvinTuple> values = store.fetch(item, property, null, 350, 150, 1, 0, null)) {
			assertEquals(300, values.next().time);
			assertFalse(values.hasNext());
		}
		try (IExtendedIterator<URI> properties = store.properties(item, null)) {
			assertEquals(List.of(property), properties.toList());
		}
		try (IExtendedIterator<KvinTuple> values = store.fetch(other, property, null, 0)) {
			assertEquals(2.5, (Double) values.next().value, 0);
			assertFalse(values.hasNext());
		}
		var table = new HadoopTables(new Configuration()).load(directory.toPath().resolve("iceberg").toString());
		int fileCount = 0;
		try (var files = table.newScan().planFiles()) {
			for (var task : files) {
				fileCount++;
				var path = new org.apache.hadoop.fs.Path(task.file().location());
				var input = HadoopInputFile.fromPath(path, new Configuration());
				var options = HadoopReadOptions.builder(new Configuration(), path).build();
				var metadata = ParquetFileReader.readFooter(input, options, input.newStream());
				assertEquals(CompressionCodecName.ZSTD, metadata.getBlocks().get(0).getColumns().get(0).getCodec());
			}
		} catch (Exception e) {
			throw new AssertionError(e);
		}
		assertEquals(2, fileCount);
		for (String name : List.of("valueInt", "valueLong", "valueFloat", "valueDouble",
				"valueString", "valueBool", "valueObject")) {
			assertNotNull(table.schema().findField(name));
		}
		store.createBranch("archive");
		table.refresh();
		assertNotNull(table.refs().get("archive"));
	}

	@Test
	public void readsDifferentContextsAndPropertiesWithPerPropertyLimits() {
		URI secondProperty = URIs.createURI("urn:test:second");
		URI context = URIs.createURI("urn:test:context");
		store.put(
				new KvinTuple(item, property, Kvin.DEFAULT_CONTEXT, 10, 0, true),
				new KvinTuple(item, property, Kvin.DEFAULT_CONTEXT, 20, 0, false),
				new KvinTuple(item, secondProperty, Kvin.DEFAULT_CONTEXT, 30, 0, 1.5f),
				new KvinTuple(item, property, context, 40, 0, 7));
		try (IExtendedIterator<KvinTuple> values = store.fetch(item, null, null, 1)) {
			assertEquals(2, values.toList().size());
		}
		try (IExtendedIterator<KvinTuple> values = store.fetch(item, property, context, 0)) {
			assertEquals(7, values.next().value);
			assertFalse(values.hasNext());
		}
		try (IExtendedIterator<KvinTuple> values = store.fetch(item, secondProperty, null, 0)) {
			assertEquals(1.5f, (Float) values.next().value, 0);
			assertFalse(values.hasNext());
		}
	}

	@Test
	public void readsOptionalValuesWithoutCarryingValuesBetweenRows() {
		store.put(
				new KvinTuple(item, property, null, 1, 0, 42),
				new KvinTuple(item, property, null, 2, 1, null),
				new KvinTuple(item, property, null, 2, 0, true),
				new KvinTuple(item, property, null, 3, 0, 1.5f),
				new KvinTuple(item, property, null, 4, 0, 2.5),
				new KvinTuple(item, property, null, 5, 0, "text"),
				new KvinTuple(item, property, null, 6, 123L));
		try (IExtendedIterator<KvinTuple> values = store.fetch(item, property, null, 0)) {
			List<KvinTuple> rows = values.toList();
			assertEquals(7, rows.size());
			assertEquals(123L, rows.get(0).value);
			assertEquals("text", rows.get(1).value);
			assertEquals(2.5, (Double) rows.get(2).value, 0);
			assertEquals(1.5f, (Float) rows.get(3).value, 0);
			assertEquals(2, rows.get(4).time);
			assertEquals(1, rows.get(4).seqNr);
			assertNull(rows.get(4).value);
			assertEquals(0, rows.get(5).seqNr);
			assertEquals(true, rows.get(5).value);
			assertEquals(42, rows.get(6).value);
		}
	}

	@Test
	public void readsBinaryValuesAlongsideNullAndPrimitiveValues() {
		Record first = new Record(URIs.createURI("urn:test:field"), "first");
		Record second = new Record(URIs.createURI("urn:test:field"), "second");
		store.put(
				new KvinTuple(item, property, null, 5, 0, first),
				new KvinTuple(item, property, null, 4, 0, second),
				new KvinTuple(item, property, null, 3, 0, null),
				new KvinTuple(item, property, null, 2, 0, false),
				new KvinTuple(item, property, null, 1, 0, 0));
		try (IExtendedIterator<KvinTuple> values = store.fetch(item, property, null, 0)) {
			List<KvinTuple> rows = values.toList();
			assertEquals(5, rows.size());
			assertEquals(first, rows.get(0).value);
			assertEquals(second, rows.get(1).value);
			assertNull(rows.get(2).value);
			assertEquals(false, rows.get(3).value);
			assertEquals(0, rows.get(4).value);
		}
	}

	@Test
	public void mergesSortedBatchesWithBoundedWriteMemory() {
		List<KvinTuple> tuples = new ArrayList<>();
		for (int i = 0; i < 8193; i++) {
			tuples.add(new KvinTuple(item, property, Kvin.DEFAULT_CONTEXT, i, 0, i));
		}
		store.put(tuples);
		try (IExtendedIterator<KvinTuple> values = store.fetch(item, property, null, 2)) {
			assertEquals(8192, values.next().time);
			assertEquals(8191, values.next().time);
			assertFalse(values.hasNext());
		}
	}

	@Test
	public void separateInstancesRefreshUriIds() {
		try (KvinIceberg second = new KvinIceberg(directory.toString())) {
			store.put(new KvinTuple(item, property, null, 1, 0, 1));
			second.put(new KvinTuple(other, property, null, 2, 0, 2));
			store.put(new KvinTuple(item, property, null, 3, 0, 3));
			try (IExtendedIterator<KvinTuple> values = store.fetch(other, property, null, 0)) {
				assertEquals(other, values.next().item);
				assertFalse(values.hasNext());
			}
			try (IExtendedIterator<KvinTuple> values = second.fetch(item, property, null, 0)) {
				assertEquals(2, values.toList().size());
			}
		}
	}

	@Test
	public void idMappingsAreIcebergTablesAndSurviveReopening() {
		URI context = URIs.createURI("urn:test:context");
		store.put(new KvinTuple(item, property, context, 1, 0, "value"));
		assertFalse(Files.exists(directory.toPath().resolve("iceberg-ids.properties")));
		HadoopTables tables = new HadoopTables(new Configuration());
		for (String name : List.of("items", "properties", "contexts")) {
			var table = tables.load(directory.toPath().resolve("iceberg-ids").resolve(name).toString());
			assertNotNull(table.schema().findField("id"));
			assertNotNull(table.schema().findField("value"));
			assertNotNull(table.currentSnapshot());
		}
		store.close();
		store = new KvinIceberg(directory.toString());
		try (IExtendedIterator<KvinTuple> values = store.fetch(item, property, context, 0)) {
			assertEquals("value", values.next().value);
			assertFalse(values.hasNext());
		}
	}

	@Test
	public void refusesExistingDataWithoutIdTables() throws Exception {
		store.put(new KvinTuple(item, property, null, 1, 0, "value"));
		store.close();
		FileUtils.deleteDirectory(directory.toPath().resolve("iceberg-ids").toFile());
		assertThrows(UncheckedIOException.class, () -> new KvinIceberg(directory.toString()));
	}
}
