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
package io.github.linkedfactory.core.kvin;

import io.github.linkedfactory.core.kvin.leveldb.KvinLevelDb;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertArrayEquals;
import static org.iq80.leveldb.impl.Iq80DBFactory.factory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the LevelDB-based time series store.
 */
public class KvinLevelDbTest extends KvinTestBase {
	@Before
	public void createStore() throws IOException {
		storeDirectory = Files.createTempDirectory("leveldb-test-").toFile();
		store = new KvinLevelDb(storeDirectory);
	}

	protected void recreateStore() {
		store.close();
		store = new KvinLevelDb(storeDirectory);
	}

	@After
	public void closeStore() throws IOException {
		store.close();
		store = null;
		deleteDirectory(storeDirectory.toPath());
	}

	private static Record recordValue(int nr, int pointNr, long time) {
		return new Record(URIs.createURI("prop:itemNr"), nr)
				.append(new Record(URIs.createURI("prop:pointNr"), pointNr))
				.append(new Record(URIs.createURI("prop:value"), URIs.createURI("some:value#" + time)));
	}

	@Test
	public void testRecordsParallel() {
		URI ctx = null;

		int points = 100;
		long pointDistance = 100;
		long startTime = pointDistance * points;
		int items = 50;
		// insert items in parallel
		IntStream.rangeClosed(1, items).parallel().forEach(nr -> {
			URI uri = itemUri(nr);
			for (int i = 0; i < points; i++) {
				long time = startTime - i * pointDistance;
				store.put(new KvinTuple(uri, valueProperty, ctx, time, recordValue(nr, i, time)));
			}
		});

		// close and re-open store
		recreateStore();

		int queryPoints = points / 2;
		long interval = (queryPoints - 1) * pointDistance;
		for (int nr = 1; nr <= items; nr++) {
			assertEquals(queryPoints, store.fetch(itemUri(nr), valueProperty, ctx,
					startTime, startTime - interval, 0, 0, null).toList().size());

			for (KvinTuple tuple : store.fetch(itemUri(nr), valueProperty, ctx, startTime, startTime - interval, 0, 0, null)) {
				Record record = (Record) tuple.value;
				assertEquals(URIs.createURI("some:value#" + tuple.time),
						record.first(URIs.createURI("prop:value")).getValue());
			}
		}
	}

	@Test
	public void testIds() {
		int points = 100;
		long pointDistance = 100;
		long startTime = pointDistance * points;
		int items = 50;
		for (int nr = 1; nr <= items; nr++) {
			URI uri = itemUri(nr);
			for (int i = 0; i < points; i++) {
				long time = startTime - i * pointDistance;
				store.put(new KvinTuple(uri, valueProperty, Kvin.DEFAULT_CONTEXT, time, recordValue(nr, i, time)));
			}
		}

		List<Long> nextIds = Arrays.stream(((KvinLevelDb) store).nextIds).map(AtomicLong::get).toList();

		// close and re-open store
		recreateStore();

		List<Long> nextIdsLoaded = Arrays.stream(((KvinLevelDb) store).nextIds).map(AtomicLong::get).toList();
		assertEquals(nextIds, nextIdsLoaded);
	}

	@Test
	public void testIdsAreKeptAfterDelete() {
		KvinLevelDb db = (KvinLevelDb) store;
		URI item = itemUri(1);
		URI otherItem = itemUri(2);
		URI ctx = URIs.createURI("ctx:1");
		store.put(new KvinTuple(item, valueProperty, ctx, 100, 1),
				new KvinTuple(item, valueProperty, ctx, 200, 2),
				new KvinTuple(otherItem, valueProperty, ctx, 100, 3));
		byte[] valueId = db.toId(item, valueProperty, ctx, false, null);

		// delete all values of the item and property
		assertEquals(2, store.delete(item, valueProperty, ctx, Long.MAX_VALUE, 0));
		assertEquals(List.of(), fetched(item, valueProperty, ctx));
		assertEquals(List.of(otherItem), store.descendants(URIs.createURI("http://example.org/"), ctx).toList());

		// delete all values of the item in the context
		store.put(new KvinTuple(item, valueProperty, ctx, 300, 4));
		assertTrue(store.delete(item, ctx));
		assertEquals(List.of(), fetched(item, valueProperty, ctx));
		assertEquals(List.of(otherItem), store.descendants(URIs.createURI("http://example.org/"), ctx).toList());

		// the IDs of item, context and property are kept and reused, also after re-opening the store
		recreateStore();
		db = (KvinLevelDb) store;
		assertArrayEquals(valueId, db.toId(item, valueProperty, ctx, false, null));

		store.put(new KvinTuple(item, valueProperty, ctx, 400, 5));
		assertEquals(List.of(new KvinTuple(item, valueProperty, ctx, 400, 5)), fetched(item, valueProperty, ctx));
		assertArrayEquals(valueId, db.toId(item, valueProperty, ctx, false, null));
	}

	private List<KvinTuple> fetched(URI item, URI property, URI context) {
		return store.fetch(item, property, context, 0).toList();
	}

	@Test
	public void testBatchWriteSwitchesAndReusesPrefixesAcrossFlush() {
		URI item = itemUri(101);
		URI otherItem = itemUri(102);
		URI property = valueProperty;
		URI otherProperty = propertyUri(2);
		URI context = URIs.createURI("http://example.org/context/one");
		URI otherContext = URIs.createURI("http://example.org/context/two");
		String largeValue = "x".repeat(2200);

		// The first and last runs deliberately use the same series.  The middle
		// run changes every part of the series identity and the write crosses the
		// one-megabyte value-batch threshold while the cache is in use.
		List<KvinTuple> tuples = new ArrayList<>();
		for (int i = 0; i < 260; i++) {
			tuples.add(new KvinTuple(item, property, context, i, i, largeValue));
		}
		tuples.add(new KvinTuple(otherItem, otherProperty, otherContext, 10000L, 0, largeValue));
		for (int i = 0; i < 260; i++) {
			tuples.add(new KvinTuple(item, property, context, 20000L + i, i, largeValue));
		}

		store.put(tuples);

		List<KvinTuple> firstSeries = fetched(item, property, context);
		assertEquals(520, firstSeries.size());
		List<Long> expectedTimes = Stream.concat(LongStream.range(0, 260).boxed(),
				LongStream.range(20000, 20260).boxed()).toList();
		assertEquals(expectedTimes, firstSeries.stream().map(t -> t.time).sorted().toList());
		assertEquals(1, fetched(otherItem, otherProperty, otherContext).size());
		assertEquals(largeValue, firstSeries.get(0).value);

		recreateStore();

		assertEquals(520, fetched(item, property, context).size());
		assertEquals(1, fetched(otherItem, otherProperty, otherContext).size());
	}

	@Test
	public void testBatchWriteRecoversAfterIteratorFailure() {
		URI item = itemUri(103);
		KvinTuple tuple = new KvinTuple(item, valueProperty, Kvin.DEFAULT_CONTEXT, 1L, 0, "before failure");
		Iterable<KvinTuple> failingEntries = () -> new Iterator<>() {
			private boolean returned = false;

			@Override
			public boolean hasNext() {
				if (!returned) {
					return true;
				}
				throw new IllegalStateException("iterator failure");
			}

			@Override
			public KvinTuple next() {
				returned = true;
				return tuple;
			}
		};

		try {
			store.put(failingEntries);
			fail("Expected the source iterator to fail");
		} catch (IllegalStateException e) {
			// expected
		}

		KvinTuple afterFailure = new KvinTuple(item, valueProperty, Kvin.DEFAULT_CONTEXT, 2L, 0, "after failure");
		store.put(afterFailure);

		List<KvinTuple> values = store.fetch(item, valueProperty, Kvin.DEFAULT_CONTEXT, 0).toList();
		assertEquals(1, values.size());
		assertEquals(afterFailure.time, values.get(0).time);
		assertEquals(afterFailure.value, values.get(0).value);
	}

	@Test
	public void testValuesAreGroupedByContext() {
		URI item1 = URIs.createURI("http://example.org/item1");
		URI item2 = URIs.createURI("http://example.org/item2");
		URI ctxA = URIs.createURI("ctx:a");
		URI ctxB = URIs.createURI("ctx:b");
		store.put(new KvinTuple(item1, valueProperty, ctxA, 100, 1),
				new KvinTuple(item2, valueProperty, ctxB, 100, 2),
				new KvinTuple(item2, valueProperty, ctxA, 100, 3),
				new KvinTuple(item1, valueProperty, ctxB, 100, 4),
				new KvinTuple(item1, propertyUri(1), ctxA, 200, 5));

		// all values of a context are adjacent in key order
		List<URI> contexts = ((KvinLevelDb) store).fetchAll().mapWith(t -> t.context).toList();
		assertEquals(5, contexts.size());
		Set<URI> finished = new HashSet<>();
		URI last = null;
		for (URI context : contexts) {
			if (!context.equals(last)) {
				assertTrue("Context " + context + " is not contiguous", finished.add(context));
				last = context;
			}
		}

		assertEquals(Set.of(valueProperty, propertyUri(1)), new HashSet<>(store.properties(item1, ctxA).toList()));
		assertEquals(List.of(valueProperty), store.properties(item1, ctxB).toList());
		assertEquals(Set.of(item1, item2),
				new HashSet<>(store.descendants(URIs.createURI("http://example.org/"), ctxB).toList()));

		// deleting an item in one context keeps its values in other contexts
		assertTrue(store.delete(item1, ctxA));
		assertEquals(List.of(), fetched(item1, valueProperty, ctxA));
		assertEquals(List.of(), store.properties(item1, ctxA).toList());
		assertEquals(List.of(new KvinTuple(item1, valueProperty, ctxB, 100, 4)), fetched(item1, valueProperty, ctxB));
		assertEquals(List.of(new KvinTuple(item2, valueProperty, ctxA, 100, 3)), fetched(item2, valueProperty, ctxA));
		assertEquals(List.of(item2),
				store.descendants(URIs.createURI("http://example.org/"), ctxA).toList());
	}

	@Test
	public void testReopenStoreWithFormatVersion() {
		URI item = URIs.createURI("http://example.org/item");
		store.put(new KvinTuple(item, valueProperty, null, 100, 1));
		recreateStore();
		assertEquals(List.of(new KvinTuple(item, valueProperty, Kvin.DEFAULT_CONTEXT, 100, 1)),
				fetched(item, valueProperty, null));
	}

	@Test
	public void testOpenStoreWithoutFormatVersionFails() throws IOException {
		File dir = Files.createTempDirectory("leveldb-legacy-").toFile();
		try {
			// a store of an earlier version: values but no format marker
			try (DB values = factory.open(new File(dir, "values"), new Options().createIfMissing(true))) {
				values.put(new byte[]{1, 1, 1, 0}, new byte[]{0});
			}
			assertThrows(KvinException.class, () -> new KvinLevelDb(dir));
			// the failed open must release the database locks
			assertThrows(KvinException.class, () -> new KvinLevelDb(dir));
		} finally {
			deleteDirectory(dir.toPath());
		}
	}

	@Test
	public void testOpenStoreWithOtherFormatVersionFails() throws IOException {
		store.close();
		try (DB ids = factory.open(new File(storeDirectory, "ids"), new Options())) {
			byte[] version = ByteBuffer.allocate(Integer.BYTES).putInt(KvinLevelDb.FORMAT_VERSION + 1).array();
			ids.put("\0formatVersion".getBytes(StandardCharsets.UTF_8), version);
		}
		assertThrows(KvinException.class, () -> new KvinLevelDb(storeDirectory));
		// re-open with the expected version for the cleanup
		try (DB ids = factory.open(new File(storeDirectory, "ids"), new Options())) {
			byte[] version = ByteBuffer.allocate(Integer.BYTES).putInt(KvinLevelDb.FORMAT_VERSION).array();
			ids.put("\0formatVersion".getBytes(StandardCharsets.UTF_8), version);
		}
		store = new KvinLevelDb(storeDirectory);
	}
}
