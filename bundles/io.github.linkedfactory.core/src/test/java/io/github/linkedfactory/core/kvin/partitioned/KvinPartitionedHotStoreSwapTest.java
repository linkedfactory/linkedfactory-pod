package io.github.linkedfactory.core.kvin.partitioned;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.iceberg.KvinIcebergOptions;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Tests how KvinPartitioned starts new hot stores and opens the hot store directories.
 */
public class KvinPartitionedHotStoreSwapTest {
	static final URI ITEM = URIs.createURI("http://example.org/machine");
	static final URI PROPERTY = URIs.createURI("http://example.org/temperature");

	File tempDir;
	KvinPartitioned store;

	@Before
	public void setup() throws IOException {
		tempDir = Files.createTempDirectory("kvinPartitionedHotStoreSwap").toFile();
	}

	@After
	public void cleanup() throws IOException {
		if (store != null) {
			store.close();
		}
		FileUtils.deleteDirectory(tempDir);
	}

	KvinPartitioned open(HotStore.Factory factory) throws IOException {
		return new KvinPartitioned(tempDir, null,
				KvinIcebergOptions.builder(new File(tempDir, "archive").toString()).build(), factory);
	}

	static KvinTuple value(long time) {
		return new KvinTuple(Kvin.DEFAULT_CONTEXT, ITEM, PROPERTY, time, (double) time);
	}

	List<Long> times(Kvin kvin) {
		return kvin.fetch(ITEM, PROPERTY, Kvin.DEFAULT_CONTEXT, 0).toList().stream().map(t -> t.time).toList();
	}

	List<String> hotStoreDirectories() {
		File[] files = tempDir.listFiles(file -> file.isDirectory() && !file.getName().equals("archive"));
		return java.util.Arrays.stream(files).map(File::getName).sorted().toList();
	}

	@Test
	public void writesWhileTheNewHotStoreIsOpened() throws Exception {
		CountDownLatch opening = new CountDownLatch(1);
		CountDownLatch proceed = new CountDownLatch(1);
		AtomicInteger opens = new AtomicInteger();
		store = open(directory -> {
			if (opens.incrementAndGet() == 2) {
				opening.countDown();
				try {
					proceed.await();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
			return HotStore.MEMTABLE.open(directory);
		});
		HotStore previous = store.hotStore;
		store.put(value(1));
		CompletableFuture<Void> archival = CompletableFuture.runAsync(store::runArchival);
		try {
			assertTrue(opening.await(10, TimeUnit.SECONDS));
			// the write lock is not held while the new hot store is opened
			CompletableFuture.runAsync(() -> store.put(value(2))).get(10, TimeUnit.SECONDS);
		} finally {
			proceed.countDown();
		}
		archival.get(30, TimeUnit.SECONDS);

		// the previous hot store is archived without opening it again
		assertEquals(2, opens.get());
		assertNull(store.hotStoreArchive);
		// the value was written before the swap, so it is archived with the previous hot store
		assertEquals(List.of(2L, 1L), times(store.archiveStore));
		assertEquals(List.of(), store.hotStore.fetchAll().toList());
		assertEquals(List.of("hot-2"), hotStoreDirectories());
		assertFalse(store.hotStore == previous);
	}

	@Test
	public void keepsThePreviousHotStoreAsArchive() throws IOException {
		store = open(HotStore.MEMTABLE);
		HotStore previous = store.hotStore;
		File previousPath = store.hotStorePath;
		store.put(value(1));
		store.createNewHotDataStore();

		assertSame(previous, store.hotStoreArchive);
		assertEquals(previousPath, store.hotStoreArchivePath);
		assertEquals(new File(tempDir, "hot-2"), store.hotStorePath);
		assertEquals(List.of(1L), times(store));
		assertThrows(IllegalStateException.class, store::createNewHotDataStore);
	}

	@Test
	public void keepsWritingToTheHotStoreIfANewOneCannotBeOpened() throws Exception {
		AtomicInteger opens = new AtomicInteger();
		store = open(directory -> {
			if (opens.incrementAndGet() == 2) {
				throw new IOException("Cannot open " + directory);
			}
			return HotStore.MEMTABLE.open(directory);
		});
		HotStore hotStore = store.hotStore;
		store.put(value(1));
		store.runArchival();

		assertSame(hotStore, store.hotStore);
		assertNull(store.hotStoreArchive);
		assertEquals(List.of("hot-1"), hotStoreDirectories());
		store.put(value(2));
		assertEquals(List.of(2L, 1L), times(store));

		// the next archival uses a new directory
		store.runArchival();
		assertEquals(List.of("hot-3"), hotStoreDirectories());
		assertEquals(List.of(2L, 1L), times(store.archiveStore));
	}

	@Test
	public void archivesTheHotStoresOfEarlierVersions() throws IOException {
		HotStore archive = HotStore.MEMTABLE.open(new File(tempDir, "current-archive"));
		archive.put(value(1));
		archive.close();
		HotStore current = HotStore.MEMTABLE.open(new File(tempDir, "current"));
		current.put(value(2));
		current.close();

		store = open(HotStore.MEMTABLE);
		assertEquals(new File(tempDir, "current"), store.hotStorePath);
		assertEquals(new File(tempDir, "current-archive"), store.hotStoreArchivePath);
		assertEquals(List.of(2L, 1L), times(store));

		store.runArchival();
		assertEquals(List.of("current"), hotStoreDirectories());
		store.runArchival();
		assertEquals(List.of("hot-1"), hotStoreDirectories());
		assertEquals(List.of(2L, 1L), times(store.archiveStore));
	}

	@Test
	public void reopensAnInterruptedArchival() throws IOException {
		store = open(HotStore.MEMTABLE);
		store.put(value(1));
		store.createNewHotDataStore();
		store.put(value(2));
		store.close();

		store = open(HotStore.MEMTABLE);
		assertEquals(new File(tempDir, "hot-2"), store.hotStorePath);
		assertNotNull(store.hotStoreArchive);
		store.runArchival();
		assertEquals(List.of("hot-2"), hotStoreDirectories());
		assertEquals(List.of(1L), times(store.archiveStore));

		// the next hot store gets a new number
		store.runArchival();
		assertEquals(List.of("hot-3"), hotStoreDirectories());
	}

	@Test
	public void rejectsMoreThanTwoHotStores() throws IOException {
		for (String name : List.of("hot-1", "hot-2", "hot-3")) {
			Files.createDirectories(new File(tempDir, name).toPath());
		}
		IOException e = assertThrows(IOException.class, () -> open(HotStore.MEMTABLE));
		assertTrue(e.getMessage(), e.getMessage().contains("hot-3"));
	}
}
