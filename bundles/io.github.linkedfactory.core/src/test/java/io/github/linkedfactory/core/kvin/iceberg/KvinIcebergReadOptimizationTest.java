package io.github.linkedfactory.core.kvin.iceberg;

import io.github.linkedfactory.core.kvin.KvinTuple;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class KvinIcebergReadOptimizationTest {
	private File directory;
	private KvinIceberg store;
	private final URI item = URIs.createURI("urn:test:item");
	private final URI property = URIs.createURI("urn:test:property");
	private final URI secondProperty = URIs.createURI("urn:test:second");

	@Before
	public void setUp() throws Exception {
		directory = Files.createTempDirectory("kvin-iceberg-read").toFile();
		store = new KvinIceberg(directory.toString());
	}

	@After
	public void tearDown() throws Exception {
		store.close();
		FileUtils.deleteDirectory(directory);
	}

	private Table table() {
		return new HadoopTables(new Configuration()).load(directory.toPath().resolve("iceberg").toString());
	}

	@Test
	public void propertiesUseMarkersWithoutDecodingValuesAndKeepIdOrder() {
		URI context = URIs.createURI("urn:test:context");
		store.put(new KvinTuple(item, property, null, 2, 0, new byte[]{(byte) 255}),
				new KvinTuple(item, property, null, 1, 0, 1),
				new KvinTuple(item, secondProperty, null, 1, 0, null),
				new KvinTuple(item, URIs.createURI("urn:test:excluded"), context, 1, 0, 1));
		store.put(new KvinTuple(item, property, null, 3, 0, 3));
		store.close();
		store = new KvinIceberg(directory.toString());
		try (var properties = store.properties(item, null)) {
			assertEquals(List.of(property, secondProperty), properties.toList());
		}
		assertEquals(2, store.delete(item, property, null, 3, 2));
		try (var properties = store.properties(item, null)) {
			assertEquals(List.of(property, secondProperty), properties.toList());
		}
		assertEquals(1, store.delete(item, property, null, 1, 1));
		try (var properties = store.properties(item, null)) {
			assertEquals(List.of(secondProperty), properties.toList());
		}
		try (var properties = store.properties(URIs.createURI("urn:test:missing"), null)) {
			assertFalse(properties.hasNext());
		}
	}

	@Test
	public void completedSeriesSkipValueDecodingWithinRowGroup() throws Exception {
		List<KvinTuple> tuples = new ArrayList<>();
		for (int time = 10; time > 0; time--) {
			tuples.add(new KvinTuple(item, property, null, time, 0, "old"));
		}
		tuples.add(new KvinTuple(item, secondProperty, null, 1, 0, "next"));
		store.put(tuples);
		Table table = table();
		try (var tasks = table.newScan().planFiles();
			 var reader = new SeriesSkippingReader(table.schema(), tasks.iterator().next(), Expressions.alwaysTrue())) {
			var first = reader.next();
			var field = SeriesSkippingReader.class.getDeclaredField("columns");
			field.setAccessible(true);
			ColumnReader[] columns = (ColumnReader[]) field.get(reader);
			columns[10] = spy(columns[10]);
			reader.skipThrough((Long) first.get(0), (Long) first.get(1), (Long) first.get(2));
			assertEquals("next", reader.next().get(10));
			assertFalse(reader.hasNext());
			verify(columns[10], times(9)).skip();
			verify(columns[10], times(1)).getBinary();
		}
		try (var values = store.fetch(item, null, null, 1)) {
			assertEquals(List.of(10L, 1L), values.toList().stream().map(tuple -> tuple.time).toList());
		}
	}

	@Test
	public void completedSeriesPruneWholeRowGroups() throws Exception {
		Table table = table();
		table.updateProperties().set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "4096")
				.set("write.parquet.row-group-check-min-record-count", "100")
				.set("write.parquet.row-group-check-max-record-count", "100").commit();
		List<KvinTuple> tuples = new ArrayList<>();
		for (int time = 2000; time > 0; time--) {
			tuples.add(new KvinTuple(item, property, null, time, 0, "value-" + time + "-".repeat(100)));
		}
		tuples.add(new KvinTuple(item, secondProperty, null, 1, 0, "next"));
		// Reopen so the writer sees the updated table properties.
		store.close();
		store = new KvinIceberg(directory.toString());
		store.put(tuples);
		table.refresh();
		try (var tasks = table.newScan().planFiles();
			 var reader = new SeriesSkippingReader(table.schema(), tasks.iterator().next(), Expressions.alwaysTrue())) {
			var first = reader.next();
			var field = SeriesSkippingReader.class.getDeclaredField("reader");
			field.setAccessible(true);
			ParquetFileReader file = spy((ParquetFileReader) field.get(reader));
			field.set(reader, file);
			reader.skipThrough((Long) first.get(0), (Long) first.get(1), (Long) first.get(2));
			assertEquals("next", reader.next().get(10));
			assertFalse(reader.hasNext());
			verify(file, atLeastOnce()).skipNextRowGroup();
		}
	}

	@Test
	public void limitedReaderPreservesTypedValuesNullsBoundsAndSequenceNumbers() {
		List<Object> expected = java.util.Arrays.asList(42, 43L, 1.5f, 2.5, "text", true, null,
				new io.github.linkedfactory.core.kvin.Record(URIs.createURI("urn:test:field"), "object"));
		List<KvinTuple> tuples = new ArrayList<>();
		for (int i = 0; i < expected.size(); i++) {
			tuples.add(new KvinTuple(item, property, null, 10, expected.size() - i, expected.get(i)));
		}
		tuples.add(new KvinTuple(item, property, null, 9, 0, "excluded"));
		tuples.add(new KvinTuple(item, secondProperty, null, 10, 0, "second"));
		store.put(tuples);
		try (var values = store.fetch(List.of(item), List.of(property, secondProperty), null, 10, 10, expected.size(), 0, null)) {
			List<KvinTuple> rows = values.toList();
			assertEquals(expected, rows.subList(0, expected.size()).stream().map(tuple -> tuple.value).toList());
			assertEquals("second", rows.get(expected.size()).value);
		}
	}
}
