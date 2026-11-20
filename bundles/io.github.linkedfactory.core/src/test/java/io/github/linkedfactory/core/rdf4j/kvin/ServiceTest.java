/*
 * Copyright (c) 2022 Fraunhofer IWU.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.linkedfactory.core.rdf4j.kvin;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.Record;
import io.github.linkedfactory.core.kvin.leveldb.KvinLevelDb;
import io.github.linkedfactory.core.rdf4j.common.Conversions;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import net.enilink.vocab.rdf.RDF;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.AbstractFederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ServiceTest {
	static final int SEED = 200;
	static final int START_TIME = 1000;
	final URI valueProperty = URIs.createURI("property:value");

	File storeDirectory;
	Kvin store;
	Repository repository;

	/**
	 * Groups the tuples by item and time.
	 */
	static Map<URI, Map<Long, List<KvinTuple>>> byItemAndTime(List<KvinTuple> data) {
		return data.stream().collect(Collectors.groupingBy(t -> t.item, Collectors.groupingBy(t -> t.time)));
	}

	static Object firstValue(Map<URI, Map<Long, List<KvinTuple>>> data, URI item, long time) {
		return data.get(item).get(time).get(0).value;
	}

	static double doubleValue(BindingSet bs, String name) {
		return ((Literal) bs.getValue(name)).doubleValue();
	}

	static long longValue(BindingSet bs, String name) {
		return ((Literal) bs.getValue(name)).longValue();
	}

	@Test
	public void basicTest() {
		List<KvinTuple> data = addData(10, 10);

		RepositoryConnection conn = repository.getConnection();
		try {
			int time = START_TIME + 20;

			String values = "values ?item { <item-1> <item-2> }";
			String queryStr = """
					select * where { %s service <kvin:> {
					?item <property:value> ?v . ?v <kvin:to> %d ; <kvin:limit> 1 .
					?v <kvin:value> ?value ; <kvin:time> ?time } }""".formatted(values, time);
			TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr, "http://example.org/");

			Map<URI, Map<Long, List<KvinTuple>>> dataByItemAndTime = byItemAndTime(
					data.stream().filter(t -> t.time <= time).toList());

			TupleQueryResult r = query.evaluate();
			while (r.hasNext()) {
				BindingSet bs = r.next();
				URI item = URIs.createURI(bs.getValue("item").toString());
				Object itemValue = firstValue(dataByItemAndTime, item, longValue(bs, "time"));

				Assert.assertEquals((Double) itemValue, doubleValue(bs, "value"), 0.001);
			}
			r.close();
		} finally {
			conn.close();
		}
	}

	@Test
	public void recordTest() {
		List<KvinTuple> data = addRecords(2, 10);

		RepositoryConnection conn = repository.getConnection();
		try {
			int time = START_TIME + 20;

			String values = "values ?item { <item-1> <item-2> }";
			String queryStr = """
					select * where { %s service <kvin:> {
					?item <property:value> ?v . ?v <kvin:to> %d ; <kvin:limit> 1 .
					?v <kvin:value> ?record ; <kvin:time> ?time .
					?record <p:3> [ <p:nested> ?value ]
					} }""".formatted(values, time);
			TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr, "http://example.org/");

			Map<URI, Map<Long, List<KvinTuple>>> dataByItemAndTime = byItemAndTime(
					data.stream().filter(t -> t.time <= time).toList());

			TupleQueryResult r = query.evaluate();
			while (r.hasNext()) {
				BindingSet bs = r.next();
				URI item = URIs.createURI(bs.getValue("item").toString());
				Record itemValue = (Record) firstValue(dataByItemAndTime, item, longValue(bs, "time"));

				Record nested = (Record) itemValue.first(URIs.createURI("p:3")).getValue();
				Assert.assertEquals((Double) nested.first(URIs.createURI("p:nested")).getValue(),
						doubleValue(bs, "value"), 0.001);
			}
			r.close();
		} finally {
			conn.close();
		}
	}

	@Test
	public void jsonTest() {
		List<KvinTuple> data = addRecords(2, 10);

		RepositoryConnection conn = repository.getConnection();
		ValueFactory vf = repository.getValueFactory();
		try {
			int time = START_TIME + 20;

			String values = "values ?item { <item-1> <item-2> }";
			String queryStr = """
					select * where { %s service <kvin:> {
					?item <property:value> ?v . ?v <kvin:to> %d ; <kvin:limit> 1 .
					?v <kvin:valueJson> ?record ; <kvin:time> ?time .
					} }""".formatted(values, time);
			TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr, "http://example.org/");

			Map<URI, Map<Long, List<KvinTuple>>> dataByItemAndTime = byItemAndTime(
					data.stream().filter(t -> t.time <= time).toList());

			TupleQueryResult r = query.evaluate();
			while (r.hasNext()) {
				BindingSet bs = r.next();
				URI item = URIs.createURI(bs.getValue("item").toString());
				Object itemValue = firstValue(dataByItemAndTime, item, longValue(bs, "time"));

				Value expected = Conversions.toJsonRdfValue(itemValue, vf);
				Assert.assertEquals(expected, bs.getValue("record"));
			}
			r.close();
		} finally {
			conn.close();
		}
	}

	@Test
	public void arrayTest() {
		List<KvinTuple> data = addArrays(2, 10);

		RepositoryConnection conn = repository.getConnection();
		try {
			int time = START_TIME + 20;

			String values = "values ?item { <item-1> <item-2> }";
			String queryStr = """
					prefix rdf: <%s>
					select * where { %s service <kvin:> {
					?item <property:value> ?v . ?v <kvin:to> %d ; <kvin:limit> 1 .
					?v <kvin:value> ?array ; <kvin:time> ?time .
					?array rdf:_3 [ <p:nested> ?v3 ] ; rdf:_1 ?v1
					} }""".formatted(RDF.NAMESPACE, values, time);
			TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr, "http://example.org/");

			Map<URI, Map<Long, List<KvinTuple>>> dataByItemAndTime = byItemAndTime(
					data.stream().filter(t -> t.time <= time).toList());

			TupleQueryResult r = query.evaluate();
			Assert.assertTrue(r.hasNext());
			while (r.hasNext()) {
				BindingSet bs = r.next();
				URI item = URIs.createURI(bs.getValue("item").toString());
				Object[] array = (Object[]) firstValue(dataByItemAndTime, item, longValue(bs, "time"));

				Assert.assertEquals(array[0].toString(), ((Literal) bs.getValue("v1")).getLabel());
				Assert.assertEquals((Double) ((Record) array[2]).first(URIs.createURI("p:nested")).getValue(),
						doubleValue(bs, "v3"), 0.001);
			}
			r.close();
		} finally {
			conn.close();
		}
	}

	@Test
	public void testLimit() {
		addData(10, 10);

		RepositoryConnection conn = repository.getConnection();
		ValueFactory vf = repository.getValueFactory();
		try {
			int maxLimit = 5;
			// ensure that at least maxLimit values are returned
			int time = START_TIME + (maxLimit + 1) * 10;

			String baseUri = "http://example.org/";
			int itemCount = 2;
			String values = "values ?item { " + IntStream.rangeClosed(1, itemCount)
					.mapToObj(i -> "<item-" + i + ">").collect(Collectors.joining(" ")) + " }";
			String queryStr = """
					select distinct * where { %s service <kvin:> {
					?item <property:value> ?v1 .
					?v1 <kvin:value> ?value ; <kvin:time> ?time ;
					<kvin:to> %d ; <kvin:limit> ?limit }
					}""".formatted(values, time);
			TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr, baseUri);

			for (int limit = 1; limit <= maxLimit; limit++) {
				query.setBinding("limit", vf.createLiteral(limit));
				TupleQueryResult r = query.evaluate();
				try {
					for (int i = 0; i < itemCount * limit; i++) {
						Assert.assertTrue(r.hasNext());
						r.next();
					}
					Assert.assertFalse(r.hasNext());
				} finally {
					r.close();
				}
			}
		} finally {
			conn.close();
		}
	}

	@Test
	public void testJoin() {
		List<KvinTuple> data = addData(10, 10);

		RepositoryConnection conn = repository.getConnection();
		try {
			int time = START_TIME + 20;

			String baseUri = "http://example.org/";
			String queryStr = """
					select * where {
					  service <kvin:> {
					    <item-1> <property:value> [ <kvin:time> ?time ; <kvin:to> %d ; <kvin:value> ?v1_value ] .
					    <item-2> <property:value> [ <kvin:time> ?time ; <kvin:value> ?v2_value ] .
					  }
					}""".formatted(time);
			TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr, baseUri);

			Map<URI, Map<Long, List<KvinTuple>>> dataByItemAndTime = byItemAndTime(data);

			TupleQueryResult r = query.evaluate();
			while (r.hasNext()) {
				BindingSet bs = r.next();
				long resultTime = longValue(bs, "time");

				Object item1Value = firstValue(dataByItemAndTime, itemUri(1), resultTime);
				Object item2Value = firstValue(dataByItemAndTime, itemUri(2), resultTime);

				Assert.assertEquals((Double) item1Value, doubleValue(bs, "v1_value"), 0.001);
				Assert.assertEquals((Double) item2Value, doubleValue(bs, "v2_value"), 0.001);
			}
		} finally {
			conn.close();
		}
	}

	@Test
	public void testJoinPreviousValue() {
		List<KvinTuple> data = addData(10, 10);

		RepositoryConnection conn = repository.getConnection();
		try {
			int time = START_TIME + 50;

			String baseUri = "http://example.org/";
			String queryStr = """
					select distinct * where {
					  service <kvin:> {
					    <item-1> <property:value> [ <kvin:value> ?v1_value ; <kvin:time> ?time ; <kvin:to> %d ] .
					    <item-2> <property:value> [ <kvin:value> ?v2_value ; <kvin:limit> 1 ; <kvin:to> ?time ] .
					  }
					}""".formatted(time);
			TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr, baseUri);

			Map<URI, Map<Long, List<KvinTuple>>> dataByItemAndTime = byItemAndTime(data);

			Map<Long, List<KvinTuple>> expectedItem1Values = new HashMap<>(dataByItemAndTime.get(itemUri(1)));
			expectedItem1Values.keySet().removeIf(t -> t > time);

			TupleQueryResult r = query.evaluate();
			while (r.hasNext()) {
				BindingSet bs = r.next();
				long resultTime = longValue(bs, "time");

				Object item1Value = expectedItem1Values.remove(resultTime).get(0).value;
				Object item2Value = firstValue(dataByItemAndTime, itemUri(2), resultTime);

				Assert.assertEquals((Double) item1Value, doubleValue(bs, "v1_value"), 0.001);
				Assert.assertEquals((Double) item2Value, doubleValue(bs, "v2_value"), 0.001);
			}

			Assert.assertTrue("Expected values should all be returned by query",
					expectedItem1Values.isEmpty());
		} finally {
			conn.close();
		}
	}

	URI itemUri(int nr) {
		return URIs.createURI("http://example.org/item-" + nr);
	}

	/**
	 * Adds <code>values</code> tuples for each of the items 1 to <code>lastItem</code>.
	 */
	List<KvinTuple> addTuples(int lastItem, int values, Function<Random, Object> valueFactory) {
		Random rand = new Random(SEED);
		List<KvinTuple> result = new ArrayList<>();
		for (int nr = 1; nr <= lastItem; nr++) {
			long time = START_TIME;
			URI uri = itemUri(nr);
			for (int i = 1; i <= values; i++) {
				int seqNr = i % 1000;
				KvinTuple tuple = new KvinTuple(uri, valueProperty, Kvin.DEFAULT_CONTEXT, time, seqNr,
						valueFactory.apply(rand));
				store.put(tuple);
				time += 10;
				result.add(tuple);
			}
		}
		return result;
	}

	List<KvinTuple> addData(int items, int values) {
		return addTuples(items, values, rand -> rand.nextDouble() * rand.nextInt(100));
	}

	List<KvinTuple> addRecords(int items, int values) {
		// records use the value index i as property, hence they are created per value
		Random rand = new Random(SEED);
		List<KvinTuple> result = new ArrayList<>();
		for (int nr = 1; nr < items; nr++) {
			long time = START_TIME;
			URI uri = itemUri(nr);
			for (int i = 1; i <= values; i++) {
				Record value = new Record(URIs.createURI("p:" + i),
						new Record(URIs.createURI("p:nested"), rand.nextDouble() * rand.nextInt(100)));
				int seqNr = i % 1000;
				KvinTuple tuple = new KvinTuple(uri, valueProperty, Kvin.DEFAULT_CONTEXT, time, seqNr, value);
				store.put(tuple);
				time += 10;
				result.add(tuple);
			}
		}
		return result;
	}

	List<KvinTuple> addArrays(int items, int values) {
		return addTuples(items - 1, values, rand -> new Object[]{1, "2",
				new Record(URIs.createURI("p:nested"), rand.nextDouble() * rand.nextInt(100))});
	}

	@Before
	public void init() {
		createStore();
		createRepository();
	}

	void createStore() {
		storeDirectory = new File("/tmp/leveldb-test-" + System.currentTimeMillis() + "-" + new Random().nextInt(1000) + "/");
		storeDirectory.deleteOnExit();
		store = new KvinLevelDb(storeDirectory);
	}

	void createRepository() {
		MemoryStore memoryStore = new MemoryStore();
		SailRepository sailRepository = new SailRepository(memoryStore);

		sailRepository.setFederatedServiceResolver(new AbstractFederatedServiceResolver() {
			final ExecutorService executorService = Executors.newCachedThreadPool();

			@Override
			protected FederatedService createService(String url) {
				return new KvinFederatedService(store, () -> executorService, null, false);
			}

			@Override
			public void shutDown() {
				super.shutDown();
				executorService.shutdownNow();
			}
		});

		sailRepository.init();
		this.repository = sailRepository;
	}

	@After
	public void closeRepository() {
		repository.shutDown();
	}

	@After
	public void closeStore() throws IOException {
		store.close();
		store = null;
		deleteDirectory(storeDirectory.toPath());
	}

	void deleteDirectory(Path dir) throws IOException {
		// delete store directory
		Files.walkFileTree(dir, new SimpleFileVisitor<>() {
			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				Files.delete(file);
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult postVisitDirectory(Path dir, IOException ex) throws IOException {
				Files.delete(dir);
				return FileVisitResult.CONTINUE;
			}
		});
	}
}
