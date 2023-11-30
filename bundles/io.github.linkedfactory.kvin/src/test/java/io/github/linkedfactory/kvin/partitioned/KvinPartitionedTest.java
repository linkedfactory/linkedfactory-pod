package io.github.linkedfactory.kvin.partitioned;

import io.github.linkedfactory.kvin.Kvin;
import io.github.linkedfactory.kvin.KvinTuple;
import io.github.linkedfactory.kvin.archive.DatabaseArchiver;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.commons.io.FileUtils;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

@Ignore
public class KvinPartitionedTest extends KvinPartitionedTestBase {
	static KvinPartitioned kvinPartitioned;
	static File tempDir;

	@Before
	public void setup() throws IOException {
		tempDir = Files.createTempDirectory("kvinPartitioned").toFile();
		kvinPartitioned = new KvinPartitioned(tempDir, 2, TimeUnit.SECONDS); // archive at every 2 seconds
	}

	@After
	public void cleanup() throws IOException {
		kvinPartitioned.close();
		FileUtils.deleteDirectory(tempDir);
	}

	@Test
	public void shouldDoPut() {
		// continuing incremental put on kvinPartitioned
		kvinPartitioned.put(generateTestTuples(10, 0, 1672614000L));
		kvinPartitioned.runArchival();
		kvinPartitioned.put(generateTestTuples(10, 5, 1673218800L));
		kvinPartitioned.runArchival();
		kvinPartitioned.put(generateTestTuples(10, 10, 1673823600L));

		NiceIterator<KvinTuple> storeIterator = new DatabaseArchiver(kvinPartitioned.hotStore, null)
				.getDatabaseIterator();
		assertTrue(kvinPartitioned.archiveStorePath.listFiles().length > 0); // main folder
		assertEquals(2, new File(kvinPartitioned.archiveStorePath, "2023").listFiles(f -> f.isDirectory()).length); // folder for year 2023
		int recordCount = 0;
		while (storeIterator.hasNext()) {
			storeIterator.next();
			recordCount++;
		}
		assertEquals(recordCount, 10);
	}

	@Test
	public void shouldDoFetch() {
		kvinPartitioned.put(generateTestTuples(10, 0, 1672614000L));
		kvinPartitioned.runArchival();
		kvinPartitioned.put(generateTestTuples(10, 5, 1673218800L));

		URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 1);
		URI property = URIs.createURI("http://example.org/" + 1 + "/measured-point-1");
		long limit = 0;

		IExtendedIterator<KvinTuple> tuples = kvinPartitioned.fetch(item, property, Kvin.DEFAULT_CONTEXT, limit);

		assertNotNull(tuples);
		assertTrue(tuples.toList().size() > 0);
	}

	@Test
	public void shouldFetchProperties() {
		kvinPartitioned.put(generateTestTuples(10, 0, 1672614000L));
		kvinPartitioned.runArchival();
		kvinPartitioned.put(generateTestTuples(10, 5, 1673218800L));

		URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 5);
		IExtendedIterator<URI> properties = kvinPartitioned.properties(item);

		assertNotNull(properties);
		assertTrue(properties.toList().size() > 0);
	}
}
