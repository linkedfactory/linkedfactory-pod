package io.github.linkedfactory.core.kvin.parquet;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.Record;
import io.github.linkedfactory.core.kvin.util.KvinTupleGenerator;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.commons.io.FileUtils;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static org.junit.Assert.*;

public class KvinParquetTest {
	KvinParquet kvinParquet;
	File tempDir;
	KvinTupleGenerator tupleGenerator;
	final long startTime = 1696197600000L;
	final long startTimeSecondWeek = 1697407200000L;

	@Before
	public void setup() throws IOException {
		tempDir = Files.createTempDirectory("archive").toFile();
		tupleGenerator = new KvinTupleGenerator();
		kvinParquet = new KvinParquet(tempDir.toString());
		// 02.10.2023 0:00
		kvinParquet.put(tupleGenerator.setStartTime(startTime)
				.setItems(500)
				.setPropertiesPerItem(10)
				.setValuesPerProperty(10)
				.setItemPattern("http://localhost:8080/linkedfactory/demofactory/{}")
				.setPropertyPattern("http://example.org/{}")
				.generate());
		assertTrue(tempDir.listFiles().length > 0);
		nonSequentialPut();
	}

	@After
	public void cleanup() throws IOException {
		FileUtils.deleteDirectory(tempDir);
	}

	public void nonSequentialPut() {
		// inserting as a new week
		// 16.10.2023 0:00
		kvinParquet.put(tupleGenerator.setStartTime(startTimeSecondWeek)
				.setItems(10)
				.setPropertiesPerItem(10)
				.setValuesPerProperty(10)
				.setItemPattern("http://localhost:8080/linkedfactory/demofactory/new-week/{}")
				.setPropertyPattern("http://example.org/{}")
				.generate());

		// inserting as existing week
		// 02.10.2023 0:00
		kvinParquet.put(tupleGenerator.setStartTime(1696197600000L)
				.setItems(10)
				.setPropertiesPerItem(10)
				.setValuesPerProperty(10)
				.setItemPattern("http://localhost:8080/linkedfactory/demofactory/existing-week/{}")
				.setPropertyPattern("http://example.org/{}")
				.generate());
	}

	private File getNonSeqInsertFolder() {
		return new File(new File(tempDir, "2023"), "40");
	}

	@Test
	public void testNonSeqPut() {
		File nonSeqFolder = getNonSeqInsertFolder();
		assertEquals(2, nonSeqFolder.listFiles().length);
	}

	@Test
	public void shouldDoFetch() {
		URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 10);
		URI property = URIs.createURI("http://example.org/" + 1);
		long limit = 0;

		IExtendedIterator<KvinTuple> tuples = kvinParquet.fetch(item, property, Kvin.DEFAULT_CONTEXT, limit);
		assertNotNull(tuples);
		assertTrue(tuples.toList().size() > 0);
		tuples.close();
	}

	@Test
	public void shouldFetchWithTimeRange() {
		// fetches data from first week
		URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 1);
		URI property = URIs.createURI("http://example.org/" + 1);

		IExtendedIterator<KvinTuple> tuples = kvinParquet.fetch(item, property, Kvin.DEFAULT_CONTEXT,
				startTime + 100, startTime + 50, 0, 0, null);
		assertNotNull(tuples);
		assertEquals(5, tuples.toList().size());
		tuples.close();

		// fetches data from second week
		item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/new-week/" + 1);
		tuples = kvinParquet.fetch(item, property, Kvin.DEFAULT_CONTEXT,
				startTimeSecondWeek + 100, startTimeSecondWeek + 50, 0, 0, null);
		assertNotNull(tuples);
		assertEquals(5, tuples.toList().size());
		tuples.close();
	}

	@Test
	public void shouldFetchAllProperties() {
		URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/1");
		long limit = 0;

		IExtendedIterator<KvinTuple> tuples = kvinParquet.fetch(item, null, Kvin.DEFAULT_CONTEXT, limit);
		assertNotNull(tuples);
		List<KvinTuple> list = tuples.toList();
		list.forEach(System.out::println);
		assertEquals(100, list.size());
		tuples.close();
	}

	@Test
	public void shouldDoFetchWithLimit() {
		URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/3");
		URI property = URIs.createURI("http://example.org/1");
		long limit = 5;

		IExtendedIterator<KvinTuple> tuples = kvinParquet.fetch(item, property, Kvin.DEFAULT_CONTEXT, limit);
		assertNotNull(tuples);
		assertEquals(5, tuples.toList().size());
		tuples.close();
	}

	@Test
	public void shouldDoFetchForNonSeqEntry() {
		URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/new-week/1");
		URI property = URIs.createURI("http://example.org/1");
		long limit = 0;

		IExtendedIterator<KvinTuple> tuples = kvinParquet.fetch(item, property, Kvin.DEFAULT_CONTEXT, limit);
		assertNotNull(tuples);
		assertTrue(tuples.toList().size() > 0);
		tuples.close();
	}

	@Test
	public void mappingFileCompactionTest() throws IOException, InterruptedException {
		new Compactor(kvinParquet, 1, 1).execute();

		File[] metadataFiles = new File(kvinParquet.archiveLocation, "metadata")
				.listFiles((file, s) -> s.endsWith(".parquet"));
		assertEquals(3, metadataFiles.length);

		File nonSeqFolder = getNonSeqInsertFolder();
		File[] dataFiles = nonSeqFolder.listFiles((file, s) -> s.endsWith(".parquet"));
		assertEquals(1, dataFiles.length);
	}

	@Test
	public void shouldFetchProperties() {
		URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/1");
		IExtendedIterator<URI> properties = kvinParquet.properties(item, null);
		assertNotNull(properties);
		assertEquals(10, properties.toList().size());
		properties.close();
	}

	@Test
	public void shouldFetchRecord() {
		URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/1");
		URI property = URIs.createURI("some:property");

		var record = new Record(URIs.createURI("property:p1"), true)
				.append(new Record(URIs.createURI("property:p2"), "value2"));
		kvinParquet.put(new KvinTuple(item, property, Kvin.DEFAULT_CONTEXT, startTime, record));

		IExtendedIterator<KvinTuple> tuples = kvinParquet.fetch(item, property, Kvin.DEFAULT_CONTEXT, 1);
		List<KvinTuple> list = tuples.toList();
		assertEquals(1, list.size());
		assertEquals(record, list.get(0).value);
	}

}
