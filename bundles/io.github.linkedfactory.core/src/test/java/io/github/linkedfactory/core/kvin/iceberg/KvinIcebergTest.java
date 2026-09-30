package io.github.linkedfactory.core.kvin.iceberg;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.Record;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopTables;
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

import static org.junit.Assert.*;

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
