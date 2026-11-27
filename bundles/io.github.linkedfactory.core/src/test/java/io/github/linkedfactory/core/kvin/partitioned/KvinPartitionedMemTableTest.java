package io.github.linkedfactory.core.kvin.partitioned;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.iceberg.KvinIcebergOptions;
import io.github.linkedfactory.core.kvin.memtable.KvinMemTable;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests KvinPartitioned with KvinMemTable hot stores.
 */
public class KvinPartitionedMemTableTest {
	static final URI ITEM = URIs.createURI("http://example.org/machine");
	static final URI CHILD = URIs.createURI("http://example.org/machine/axis");
	static final URI PROPERTY = URIs.createURI("http://example.org/temperature");
	static final URI OTHER_PROPERTY = URIs.createURI("http://example.org/pressure");

	File tempDir;
	KvinPartitioned store;

	@Before
	public void setup() throws IOException {
		tempDir = Files.createTempDirectory("kvinPartitionedMemTable").toFile();
		store = open();
	}

	@After
	public void cleanup() throws IOException {
		store.close();
		FileUtils.deleteDirectory(tempDir);
	}

	KvinPartitioned open() throws IOException {
		return new KvinPartitioned(tempDir, null,
				KvinIcebergOptions.builder(new File(tempDir, "archive").toString()).build(), HotStore.MEMTABLE);
	}

	static List<Long> times(List<KvinTuple> tuples) {
		return tuples.stream().map(t -> t.time).toList();
	}

	@Test
	public void usesMemTableHotStores() {
		assertTrue(store.hotStore instanceof KvinMemTable);
	}

	@Test
	public void archivesTheHotStore() {
		store.put(new KvinTuple(Kvin.DEFAULT_CONTEXT, ITEM, PROPERTY, 1, 1.0),
				new KvinTuple(Kvin.DEFAULT_CONTEXT, ITEM, PROPERTY, 2, 2.0),
				new KvinTuple(Kvin.DEFAULT_CONTEXT, ITEM, OTHER_PROPERTY, 1, 3.0));
		store.runArchival();

		assertEquals(0, store.hotStore.fetchAll().toList().size());
		assertNull(store.hotStoreArchive);
		assertFalse(store.currentStoreArchivePath.exists());
		assertEquals(List.of(2L, 1L), times(store.archiveStore.fetch(ITEM, PROPERTY, Kvin.DEFAULT_CONTEXT, 0).toList()));

		// archived and hot values are merged
		store.put(new KvinTuple(Kvin.DEFAULT_CONTEXT, ITEM, PROPERTY, 3, 4.0));
		List<KvinTuple> tuples = store.fetch(ITEM, PROPERTY, Kvin.DEFAULT_CONTEXT, 0).toList();
		assertEquals(List.of(3L, 2L, 1L), times(tuples));
		assertEquals(4.0, tuples.get(0).value);
		assertEquals(2, store.properties(ITEM, Kvin.DEFAULT_CONTEXT).toList().size());
	}

	@Test
	public void readsAndDeletesHotValues() {
		store.put(new KvinTuple(Kvin.DEFAULT_CONTEXT, ITEM, PROPERTY, 1, 1.0),
				new KvinTuple(Kvin.DEFAULT_CONTEXT, CHILD, PROPERTY, 1, 2.0));

		assertEquals(List.of(ITEM, CHILD), store.descendants(ITEM, Kvin.DEFAULT_CONTEXT).toList());
		assertEquals(1, store.delete(ITEM, PROPERTY, Kvin.DEFAULT_CONTEXT, 1, 1));
		assertEquals(List.of(), store.fetch(ITEM, PROPERTY, Kvin.DEFAULT_CONTEXT, 0).toList());
		assertTrue(store.delete(CHILD, Kvin.DEFAULT_CONTEXT));
		assertEquals(List.of(), store.descendants(ITEM, Kvin.DEFAULT_CONTEXT).toList());
	}

	@Test
	public void keepsHotValuesAfterReopening() throws IOException {
		store.put(new KvinTuple(Kvin.DEFAULT_CONTEXT, ITEM, PROPERTY, 1, 1.0));
		store.close();

		store = open();
		assertEquals(List.of(1L), times(store.fetch(ITEM, PROPERTY, Kvin.DEFAULT_CONTEXT, 0).toList()));
	}

	@Test
	public void archivesAnInterruptedArchivalAfterReopening() throws IOException {
		store.put(new KvinTuple(Kvin.DEFAULT_CONTEXT, ITEM, PROPERTY, 1, 1.0));
		// archival stops after the hot store was moved
		store.createNewHotDataStore();
		store.put(new KvinTuple(Kvin.DEFAULT_CONTEXT, ITEM, PROPERTY, 2, 2.0));
		store.close();

		store = open();
		assertNotNull(store.hotStoreArchive);
		assertEquals(List.of(2L, 1L), times(store.fetch(ITEM, PROPERTY, Kvin.DEFAULT_CONTEXT, 0).toList()));

		store.runArchival();
		assertNull(store.hotStoreArchive);
		assertEquals(List.of(1L), times(store.archiveStore.fetch(ITEM, PROPERTY, Kvin.DEFAULT_CONTEXT, 0).toList()));
		assertEquals(List.of(2L, 1L), times(store.fetch(ITEM, PROPERTY, Kvin.DEFAULT_CONTEXT, 0).toList()));
	}

	@Test
	public void unmapsTheArchivedHotStore() throws Exception {
		org.junit.Assume.assumeTrue(java.nio.file.Files.isReadable(java.nio.file.Path.of("/proc/self/maps")));
		store.put(new KvinTuple(Kvin.DEFAULT_CONTEXT, ITEM, PROPERTY, 1, 1.0));
		String current = store.currentStorePath.toPath().toRealPath().toString();
		assertTrue(mappedFiles().stream().anyMatch(file -> file.startsWith(current + "/")));

		store.runArchival();
		// the directory of the archived hot store is removed, so its parent is resolved
		String archived = tempDir.toPath().toRealPath().resolve("current-archive").toString();
		assertEquals(List.of(), mappedFiles().stream().filter(file -> file.startsWith(archived)).toList());
	}

	static List<String> mappedFiles() throws IOException {
		try (var lines = java.nio.file.Files.lines(java.nio.file.Path.of("/proc/self/maps"))) {
			return lines.map(line -> line.indexOf('/') < 0 ? "" : line.substring(line.indexOf('/'))).toList();
		}
	}

	@Test
	public void keepsListenersOfTheHotStoreAfterArchival() {
		List<KvinTuple> values = new java.util.concurrent.CopyOnWriteArrayList<>();
		List<URI> items = new java.util.concurrent.CopyOnWriteArrayList<>();
		store.addListener(new io.github.linkedfactory.core.kvin.KvinListener() {
			@Override
			public void entityCreated(URI item) {
				items.add(item);
			}

			@Override
			public void valueAdded(URI item, URI property, URI context, long time, long seqNr, Object value) {
				values.add(new KvinTuple(context, item, property, time, (int) seqNr, value));
			}
		});
		store.put(new KvinTuple(Kvin.DEFAULT_CONTEXT, ITEM, PROPERTY, 1, 1.0));
		store.runArchival();
		store.put(new KvinTuple(Kvin.DEFAULT_CONTEXT, ITEM, PROPERTY, 2, 2.0));

		assertEquals(List.of(1L, 2L), times(values));
		// each hot store has its own IDs, so a new hot store reports its items again
		assertEquals(List.of(ITEM, ITEM), items);
	}
}
