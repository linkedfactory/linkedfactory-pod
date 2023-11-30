package io.github.linkedfactory.kvin.parquet;

import io.github.linkedfactory.kvin.Kvin;
import io.github.linkedfactory.kvin.KvinTuple;
import io.github.linkedfactory.kvin.util.KvinTupleGenerator;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.commons.io.FileUtils;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.Assert.*;

public class KvinParquetTest {
	KvinParquet kvinParquet;
	File tempDir;
	KvinTupleGenerator tupleGenerator;

	@Before
	public void setup() throws IOException {
		tempDir = Files.createTempDirectory("archive").toFile();
		tupleGenerator = new KvinTupleGenerator();
		kvinParquet = new KvinParquet(tempDir.toString());
		// 02.10.2023 0:00
		kvinParquet.put(tupleGenerator.generate(1696197600000L, 500, 10, 10,
				"http://localhost:8080/linkedfactory/demofactory/{}",
				"http://example.org/{}"));
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
		kvinParquet.put(tupleGenerator.generate(1697407200000L, 10, 10, 10,
				"http://localhost:8080/linkedfactory/demofactory/new-week/{}",
				"http://example.org/{}"));

		// inserting as existing week
		// 02.10.2023 0:00
		kvinParquet.put(tupleGenerator.generate(1696197600000L, 10, 10, 10,
				"http://localhost:8080/linkedfactory/demofactory/existing-week/{}",
				"http://example.org/{}"));
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
	public void shouldFetchAllProperties() {
		URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/1");
		long limit = 0;

		IExtendedIterator<KvinTuple> tuples = kvinParquet.fetch(item, null, Kvin.DEFAULT_CONTEXT, limit);
		assertNotNull(tuples);
		//tuples.toList().forEach(System.out::println);
		assertEquals(100, tuples.toList().size());
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
	public void mappingFileCompactionTest() throws IOException {
		new Compactor(kvinParquet.archiveLocation, kvinParquet).execute();

		File[] metadataFiles = new File(kvinParquet.archiveLocation, "metadata")
				.listFiles((file, s) -> s.endsWith(".parquet"));
		assertEquals(3, metadataFiles.length);

		File nonSeqFolder = getNonSeqInsertFolder();
		File[] dataFiles = nonSeqFolder.listFiles((file, s) -> s.endsWith(".parquet"));
		assertEquals(1, dataFiles.length);
	}

	@Test
	public void shouldFetchProperties() {
		try {
			URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/1");
			IExtendedIterator<URI> properties = kvinParquet.properties(item);
			assertNotNull(properties);
			assertEquals(10, properties.toList().size());
			properties.close();
		} catch (Exception e) {
			fail("Something went wrong while testing KvinParquet properties() method");
		}
	}
}
