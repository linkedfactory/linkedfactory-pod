package io.github.linkedfactory.core.kvin.scylladb;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.util.KvinTupleGenerator;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KvinScyllaDbTest {

	final static String keyspace = "test";
	static CqlSession testSession;
	static KvinScyllaDb kvinScyllaDb;
	static boolean isConnectedToDB = false;
	static KvinTupleGenerator tupleGenerator;

	@BeforeClass
	public static void setup() {
		try {
			testSession = CqlSession.builder().build();
			testSession.execute("DROP KEYSPACE IF EXISTS " + keyspace + " ;");
			kvinScyllaDb = new KvinScyllaDb(keyspace);
			tupleGenerator = new KvinTupleGenerator();
			kvinScyllaDb.put(tupleGenerator.generate(1696197600000L, 500, 10, 10,
					"http://localhost:8080/linkedfactory/demofactory/{}",
					"http://example.org/{}"));
			isConnectedToDB = true;
		} catch (AllNodesFailedException exception) {
			exception.printStackTrace();
			System.err.println("ScyllaDB local server instance connection failed...skipping tests.");
		}
	}

	@AfterClass
	public static void tearDown() {
		if (!isConnectedToDB) return;
		testSession.execute("DROP KEYSPACE IF EXISTS " + keyspace + " ;");
		testSession.close();
		kvinScyllaDb.close();
	}

	@Test
	public void doSimplePut() {
		if (!isConnectedToDB) return;
		Iterator<Row> kvinDataResult = testSession.execute("SELECT * FROM " + keyspace + ".kvinData;").iterator();
		int kvinDataRowCount = 0;
		while (kvinDataResult.hasNext()) {
			kvinDataResult.next();
			kvinDataRowCount++;
		}
		assertEquals(500 * 10 * 10, kvinDataRowCount);

		Iterator<Row> kvinMetadataResult = testSession.execute("SELECT * FROM " + keyspace + ".kvinMetadata;").iterator();
		int kvinMetadataRowCount = 0;
		while (kvinMetadataResult.hasNext()) {
			kvinMetadataResult.next();
			kvinMetadataRowCount++;
		}
		assertTrue(kvinMetadataRowCount > 0);
	}

	@Test
	public void doSimpleFetch() {
		if (!isConnectedToDB) return;
		URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 1);
		URI property = URIs.createURI("http://example.org/" + 1);
		URI context = Kvin.DEFAULT_CONTEXT;

		IExtendedIterator<KvinTuple> valuesSingleProperty = kvinScyllaDb.fetch(item, property, context, 0);
		assertEquals(10, valuesSingleProperty.toList().size());

		IExtendedIterator<KvinTuple> valuesMultipleProperties = kvinScyllaDb.fetch(item, null, context, 0);
		assertEquals(100, valuesMultipleProperties.toList().size());
	}

	@Test
	public void doFetchWithLimit() {
		if (!isConnectedToDB) return;
		URI context = Kvin.DEFAULT_CONTEXT;
		URI property = URIs.createURI("http://example.org/" + 1);

		URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 400);
		IExtendedIterator<KvinTuple> limitRead = kvinScyllaDb.fetch(item, null, context, 2);
		assertEquals(2 * 10, limitRead.toList().size());

		IExtendedIterator<KvinTuple> singleLimitRead = kvinScyllaDb.fetch(item, property, context, 3);
		assertEquals(3, singleLimitRead.toList().size());
	}

	@Test
	public void doFetchWithOp() {
		if (!isConnectedToDB) return;
		URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 400);
		URI context = Kvin.DEFAULT_CONTEXT;

		IExtendedIterator<KvinTuple> opRead = kvinScyllaDb.fetch(item, null, context, 1688151537L, 1688115537L, 2, 1000, "avg");
		int count = 0;
		while (opRead.hasNext()) {
			KvinTuple tuple = opRead.next();
			switch (count) {
				case 2:
					assertEquals(0.5867009162902832, tuple.value);
					break;
				case 3:
					assertEquals(0.7427548170089722, tuple.value);
					break;
			}
			count++;
		}
		assertEquals(4, count);
	}

	@Test
	public void doListProperties() {
		if (!isConnectedToDB) return;
		URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/" + 1);
		IExtendedIterator<URI> properties = kvinScyllaDb.properties(item);
		assertEquals(10, properties.toList().size());
	}

}
