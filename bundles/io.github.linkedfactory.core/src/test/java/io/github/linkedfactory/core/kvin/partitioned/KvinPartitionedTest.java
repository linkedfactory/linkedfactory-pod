package io.github.linkedfactory.core.kvin.partitioned;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.leveldb.KvinLevelDbArchiver;
import io.github.linkedfactory.core.kvin.util.KvinTupleGenerator;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

@Ignore
public class KvinPartitionedTest {
	static String itemTemplate = "http://localhost:8080/linkedfactory/demofactory/{}";
	static String propertyTemplate = "http://example.org/{}";
	KvinTupleGenerator tupleGenerator;
	KvinPartitioned kvinPartitioned;
	File tempDir;

	@Before
	public void setup() throws IOException {
		tempDir = Files.createTempDirectory("kvinPartitioned").toFile();
		tupleGenerator = new KvinTupleGenerator()
				.setItems(10)
				.setPropertiesPerItem(10)
				.setValuesPerProperty(10)
				.setItemPattern(itemTemplate)
				.setPropertyPattern(propertyTemplate);
		kvinPartitioned = new KvinPartitioned(tempDir);
	}

	@After
	public void cleanup() throws IOException {
		kvinPartitioned.close();
		FileUtils.deleteDirectory(tempDir);
	}

	@Test
	public void shouldDoPut() throws ExecutionException, InterruptedException {
		// continuing incremental put on kvinPartitioned
		kvinPartitioned.put(tupleGenerator.setStartTime(1672614000000L).generate());
		kvinPartitioned.put(tupleGenerator.setStartTime(1673218800000L).generate());
		IExtendedIterator<KvinTuple> storeIterator = kvinPartitioned.hotStore.fetchAll();
		int recordCount = 0;
		while (storeIterator.hasNext()) {
			storeIterator.next();
			recordCount++;
		}
		assertEquals(2000, recordCount);

		kvinPartitioned.runArchival();
		kvinPartitioned.put(tupleGenerator.setStartTime(1673823600000L).generate());

		assertTrue(kvinPartitioned.archiveStorePath.listFiles().length > 0); // main folder
		assertEquals(2, new File(kvinPartitioned.archiveStorePath, "2023").listFiles(f -> f.isDirectory()).length); // folder for year 2023

		storeIterator = kvinPartitioned.hotStore.fetchAll();
		recordCount = 0;
		while (storeIterator.hasNext()) {
			storeIterator.next();
			recordCount++;
		}
		assertEquals(1000, recordCount);
	}

	@Test
	public void shouldDoFetch() throws ExecutionException, InterruptedException {
		kvinPartitioned.put(tupleGenerator.setStartTime(1672614000000L).generate());
		kvinPartitioned.runArchival();
		kvinPartitioned.put(tupleGenerator.setStartTime(1673218800000L).generate());

		URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 1);
		IExtendedIterator<KvinTuple> tuples = kvinPartitioned.fetch(item,
				URIs.createURI("http://example.org/unknown-property"), Kvin.DEFAULT_CONTEXT, 0);
		assertNotNull(tuples);
		assertTrue(tuples.toList().size() == 0);

		tuples = kvinPartitioned.fetch(item,
				URIs.createURI("http://example.org/1"), Kvin.DEFAULT_CONTEXT, 0);
		assertNotNull(tuples);
		assertTrue(tuples.toList().size() > 0);
	}

	@Test
	public void shouldFetchProperties() throws ExecutionException, InterruptedException {
		kvinPartitioned.put(tupleGenerator.setStartTime(1672614000000L).generate());
		kvinPartitioned.runArchival();
		kvinPartitioned.put(tupleGenerator.setStartTime(1673218800000L).generate());

		URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 5);
		IExtendedIterator<URI> properties = kvinPartitioned.properties(item);

		assertNotNull(properties);
		assertTrue(properties.toList().size() > 0);
	}

	@Test
	public void testMultiThreading() throws InterruptedException {
		kvinPartitioned.put(tupleGenerator.setStartTime(1672614000000L).generate());

		URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 1);
		URI item2 = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 2);
		URI item3 = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 3);
		URI property = URIs.createURI("http://example.org/" + 1);
		IExtendedIterator<KvinTuple> it1 = kvinPartitioned.fetch(item, property, Kvin.DEFAULT_CONTEXT, 2);
		it1.hasNext();

		Thread archiver = new Thread(() -> {
			System.out.println("start archival");
			kvinPartitioned.runArchival();
			System.out.println("finished archival");
		});
		archiver.start();

		for (KvinTuple t1 : it1) {
			System.out.println("t1: " + t1);
			IExtendedIterator<KvinTuple> it2 = kvinPartitioned.fetch(item2, property, Kvin.DEFAULT_CONTEXT, 1);
			while (it2.hasNext()) {
				System.out.println("t2: " + it2.next());
			}
		}

		Thread.sleep(10);

		for (KvinTuple t3 : kvinPartitioned.fetch(item3, property, Kvin.DEFAULT_CONTEXT, 2)) {
			System.out.println("t3: " + t3);
		}

		archiver.join();
	}
}
