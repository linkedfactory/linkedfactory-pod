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

import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the LevelDB-based time series store.
 */
public abstract class KvinTestBase {
	protected final URI valueProperty = URIs.createURI("property:value");
	protected final int seed = 200;
	protected File storeDirectory;
	protected Kvin store;

	protected void deleteDirectory(Path dir) throws IOException {
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

	@Test
	public void testWriteMultiple() {
		Random random = new Random();
		int[] nrs = IntStream.range(0, 10).map(i -> random.nextInt(Integer.MAX_VALUE)).toArray();
		int count = 1000;
		Random rand = new Random(seed);
		for (int i = 0; i <= count; i++) {
			int randomNr = nrs[rand.nextInt(nrs.length)];
			URI uri = URIs.createURI("http://example.org/item-" + randomNr + "/subitem/subsubitem-" + randomNr + "/measured-point-1");

			int time = Math.abs(rand.nextInt());
			int value = rand.nextInt();
			store.put(new KvinTuple(uri, valueProperty, null, time, value));
		}
		rand = new Random(seed);
		for (int i = 0; i <= count; i++) {
			int randomNr = nrs[rand.nextInt(nrs.length)];
			URI uri = URIs.createURI("http://example.org/item-" + randomNr + "/subitem/subsubitem-" + randomNr + "/measured-point-1");

			int time = Math.abs(rand.nextInt());
			int expected = rand.nextInt();
			List<KvinTuple> r = store.fetch(uri, valueProperty, null, time, time, 1, 0, null).toList();

			assertEquals(Integer.valueOf(expected), r.get(0).value);
		}
	}

	protected URI itemUri(int nr) {
		return URIs.createURI("http://example.org/l1/l2/item-" + nr + "/measure");
	}

	protected URI propertyUri(int nr) {
		return URIs.createURI("http://example.org/p" + nr);
	}

	protected List<KvinTuple> addData(int items, int values) {
		Random rand = new Random(seed);
		List<KvinTuple> result = new ArrayList<>();
		for (int nr = 1; nr <= items; nr++) {
			URI uri = itemUri(nr);
			for (int i = 1; i <= values; i++) {
				int time = Math.abs(rand.nextInt());
				int value = rand.nextInt();
				KvinTuple tuple = new KvinTuple(uri, valueProperty, Kvin.DEFAULT_CONTEXT, time, value);
				store.put(tuple);
				result.add(tuple);
			}
		}
		return result;
	}

	@Test
	public void testProperties() {
		Random rand = new Random(seed);
		for (int itemNr = 1; itemNr <= 10; itemNr++) {
			URI uri = itemUri(itemNr);
			for (int propertyNr = 1; propertyNr <= 5; propertyNr++) {
				URI pUri = propertyUri(propertyNr);
				for (int i = 1; i <= 10; i++) {
					int time = Math.abs(rand.nextInt());
					int value = rand.nextInt();
					store.put(new KvinTuple(uri, pUri, null, time, value));
				}
			}
		}

		URI item4 = itemUri(4);
		Set<URI> properties = IntStream.rangeClosed(1, 5).mapToObj(this::propertyUri).collect(Collectors.toSet());
		assertEquals(properties, new HashSet<>(store.properties(item4, null).toList()));
	}

	@Test
	public void testLimit() {
		addData(3, 50);
		int limit = 5;
		assertEquals(limit, store.fetch(itemUri(1), valueProperty, null, limit).toList().size());
		limit = 1;
		assertEquals(limit, store.fetch(itemUri(1), valueProperty, null, limit).toList().size());
	}

	@Test
	public void testAllProperties() {
		List<KvinTuple> data = addData(3, 10);
		Comparator<KvinTuple> byTime = Comparator.comparingLong(t -> t.time);
		List<KvinTuple> expected = data.stream().filter(t -> t.item.equals(itemUri(1))).sorted(byTime).toList();
		List<KvinTuple> actual = new ArrayList<>(store.fetch(itemUri(1), null, null, 0).toList());
		actual.sort(byTime);
		assertEquals(expected, actual);
	}

	// @Test
	public void testDelete() {
		int valueCount = 5;
		addData(3, valueCount);

		// exactly one item should exist
		assertEquals(1, store.descendants(itemUri(1), null).toList().size());
		// item should have values
		assertEquals(valueCount, store.fetch(itemUri(1), valueProperty, null, 0).toList().size());

		store.delete(itemUri(1), null);

		// the number of values should be 0
		assertEquals(0, store.fetch(itemUri(1), valueProperty, null, 5).toList().size());
		// the item should not exist
		assertEquals(0, store.descendants(itemUri(1), null).toList().size());
	}

	@Test
	public void testInterval() {
		Random rand = new Random(seed);
		int points = 100;
		long pointDistance = 100;
		long startTime = pointDistance * points;
		for (int nr = 1; nr <= 2; nr++) {
			URI uri = itemUri(nr);
			for (int i = 0; i < points; i++) {
				long time = startTime - i * pointDistance;
				int value = rand.nextInt();
				store.put(new KvinTuple(uri, valueProperty, null, time, value));
			}
		}

		long interval = 2 * pointDistance;
		assertEquals(2, store.fetch(itemUri(1), valueProperty, null, startTime, startTime - interval, 0, interval, null).toList().size());

		List<KvinTuple> entries = store.fetch(itemUri(1), valueProperty, null, startTime, startTime - 2 * interval, 0, interval, null).toList();
		assertEquals(3, entries.size());
		assertEquals(startTime, entries.get(0).time);
		assertEquals(startTime - interval, entries.get(1).time);
		assertEquals(startTime - 2 * interval, entries.get(2).time);
	}
}
