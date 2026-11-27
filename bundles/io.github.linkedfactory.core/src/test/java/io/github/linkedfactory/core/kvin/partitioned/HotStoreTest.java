package io.github.linkedfactory.core.kvin.partitioned;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * Tests the archival methods of both types of hot stores.
 */
@RunWith(Parameterized.class)
public class HotStoreTest {
	static final URI CONTEXT = URIs.createURI("ctx:a");
	static final URI ITEM = URIs.createURI("http://example.org/item");
	static final URI OTHER = URIs.createURI("http://example.org/other");
	static final URI PROPERTY = URIs.createURI("http://example.org/property");
	static final URI OTHER_PROPERTY = URIs.createURI("http://example.org/another");

	@Parameterized.Parameters(name = "{0}")
	public static List<Object[]> factories() {
		return List.of(new Object[]{"LevelDB", HotStore.LEVELDB}, new Object[]{"MemTable", HotStore.MEMTABLE});
	}

	final HotStore.Factory factory;
	File directory;
	HotStore store;

	public HotStoreTest(String name, HotStore.Factory factory) {
		this.factory = factory;
	}

	@Before
	public void setup() throws IOException {
		directory = Files.createTempDirectory("hotStore").toFile();
		store = factory.open(directory);
	}

	@After
	public void cleanup() throws IOException {
		store.close();
		FileUtils.deleteDirectory(directory);
	}

	@Test
	public void fetchesAllValuesOneSeriesAfterAnother() {
		store.put(new KvinTuple(CONTEXT, ITEM, PROPERTY, 1, 1),
				new KvinTuple(CONTEXT, OTHER, PROPERTY, 1, 2),
				new KvinTuple(CONTEXT, ITEM, PROPERTY, 3, 3),
				new KvinTuple(Kvin.DEFAULT_CONTEXT, ITEM, OTHER_PROPERTY, 2, 4),
				new KvinTuple(CONTEXT, ITEM, PROPERTY, 2, 5));

		List<KvinTuple> all = store.fetchAll().toList();
		assertEquals(5, all.size());
		// the values of one series follow one another, newest first
		List<KvinTuple> series = all.stream().filter(t -> t.item.equals(ITEM) && t.property.equals(PROPERTY)).toList();
		assertEquals(List.of(3L, 2L, 1L), series.stream().map(t -> t.time).toList());
		int first = all.indexOf(series.get(0));
		assertEquals(series, all.subList(first, first + 3));
	}

	@Test
	public void fetchesTheNewestValueOfEachSeries() {
		store.put(new KvinTuple(CONTEXT, ITEM, PROPERTY, 1, 1),
				new KvinTuple(CONTEXT, ITEM, PROPERTY, 3, 3),
				new KvinTuple(CONTEXT, OTHER, PROPERTY, 1, 2),
				new KvinTuple(Kvin.DEFAULT_CONTEXT, ITEM, PROPERTY, 2, 4),
				new KvinTuple(CONTEXT, ITEM, OTHER_PROPERTY, 5, 5));

		List<KvinTuple> latest = store.fetchLatest().toList();
		Set<String> expected = Set.of(CONTEXT + " " + ITEM + " " + PROPERTY + " 3",
				CONTEXT + " " + OTHER + " " + PROPERTY + " 1",
				Kvin.DEFAULT_CONTEXT + " " + ITEM + " " + PROPERTY + " 2",
				CONTEXT + " " + ITEM + " " + OTHER_PROPERTY + " 5");
		assertEquals(expected.size(), latest.size());
		assertEquals(expected, latest.stream().map(t -> t.context + " " + t.item + " " + t.property + " " + t.time)
				.collect(Collectors.toSet()));
	}
}
