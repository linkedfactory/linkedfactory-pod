package io.github.linkedfactory.core.kvin.partitioned;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.iceberg.KvinIcebergOptions;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests archiving hot stores when they reach their maximum size.
 */
public class KvinPartitionedRotationTest {
	static final URI ITEM = URIs.createURI("http://example.org/item");
	static final URI PROPERTY = URIs.createURI("http://example.org/property");
	// a few hundred values fill a hot store of this size
	static final long MAX_SIZE = 4096;

	// set before a store is created, as createArchiveStore runs in the super constructor
	static Kvin archive;

	File tempDir;
	KvinPartitioned store;
	ExecutorService executor;
	long nextTime = 1;

	@Before
	public void setup() throws IOException {
		tempDir = Files.createTempDirectory("kvinPartitionedRotation").toFile();
		executor = Executors.newCachedThreadPool();
	}

	@After
	public void cleanup() throws IOException {
		executor.shutdownNow();
		if (store != null) {
			store.close();
		}
		archive = null;
		FileUtils.deleteDirectory(tempDir);
	}

	KvinIcebergOptions archiveOptions() {
		return KvinIcebergOptions.builder(new File(tempDir, "archive").toString()).build();
	}

	KvinPartitioned openWithMockedArchive(HotStore.Factory factory) throws IOException {
		return new KvinPartitioned(tempDir, null, archiveOptions(), factory, MAX_SIZE) {
			@Override
			protected Kvin createArchiveStore(KvinIcebergOptions archiveOptions) {
				return archive;
			}
		};
	}

	/**
	 * Values whose size exceeds the maximum size of the hot store.
	 */
	List<KvinTuple> fullStore() {
		List<KvinTuple> tuples = new ArrayList<>();
		for (int i = 0; i < 500; i++) {
			tuples.add(new KvinTuple(Kvin.DEFAULT_CONTEXT, ITEM, PROPERTY, nextTime++, (double) i));
		}
		return tuples;
	}

	static void await(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
		while (!condition.getAsBoolean()) {
			if (System.nanoTime() > deadline) {
				fail("Condition not met within 30 seconds");
			}
			Thread.sleep(10);
		}
	}

	/**
	 * An archive whose writes wait for the returned latch.
	 */
	CountDownLatch blockingArchive(CountDownLatch started) {
		CountDownLatch release = new CountDownLatch(1);
		archive = mockArchive();
		doAnswer(invocation -> {
			started.countDown();
			assertTrue(release.await(30, TimeUnit.SECONDS));
			return null;
		}).when(archive).put(any(Iterable.class));
		return release;
	}

	/**
	 * An archive without values.
	 */
	static Kvin mockArchive() {
		Kvin kvin = mock(Kvin.class);
		when(kvin.fetch(anyList(), anyList(), any(), anyLong(), anyLong(), anyLong(), anyLong(), any()))
				.thenAnswer(invocation -> NiceIterator.emptyIterator());
		return kvin;
	}

	@Test
	public void archivesAFullHotStore() throws Exception {
		store = new KvinPartitioned(tempDir, null, archiveOptions(), HotStore.MEMTABLE, MAX_SIZE);
		List<KvinTuple> tuples = fullStore();
		store.put(tuples);

		await(() -> store.archiveStore.fetch(ITEM, PROPERTY, Kvin.DEFAULT_CONTEXT, 0).toList().size() == tuples.size());
		await(() -> store.hotStoreArchive == null);
		assertEquals(0, store.hotStore.sizeInBytes());
		assertEquals(tuples.size(), store.fetch(ITEM, PROPERTY, Kvin.DEFAULT_CONTEXT, 0).toList().size());
	}

	@Test
	public void keepsAHotStoreBelowTheMaximumSize() throws Exception {
		archive = mockArchive();
		store = openWithMockedArchive(HotStore.MEMTABLE);
		store.put(new KvinTuple(Kvin.DEFAULT_CONTEXT, ITEM, PROPERTY, 1, 1.0));
		Thread.sleep(200);
		verify(archive, never()).put(any(Iterable.class));
		assertNull(store.hotStoreArchive);
	}

	@Test
	public void ignoresTheMaximumSizeOfHotStoresOfUnknownSize() throws Exception {
		archive = mockArchive();
		store = openWithMockedArchive(HotStore.LEVELDB);
		store.put(fullStore());
		Thread.sleep(200);
		verify(archive, never()).put(any(Iterable.class));
	}

	@Test
	public void stallsWritesWhileTheFullHotStoreWaitsForTheArchival() throws Exception {
		CountDownLatch started = new CountDownLatch(1);
		CountDownLatch release = blockingArchive(started);
		store = openWithMockedArchive(HotStore.MEMTABLE);

		// the first full store is archived, new values go to the next hot store
		store.put(fullStore());
		assertTrue(started.await(30, TimeUnit.SECONDS));
		store.put(fullStore());
		assertTrue(store.hotStore.sizeInBytes() >= MAX_SIZE);

		// the next hot store is full as well, so writes wait for the archival
		Future<?> stalled = executor.submit(() -> store.put(fullStore()));
		assertThrows(TimeoutException.class, () -> stalled.get(300, TimeUnit.MILLISECONDS));
		// reads do not wait
		assertEquals(1000, store.fetch(ITEM, PROPERTY, Kvin.DEFAULT_CONTEXT, 0).toList().size());

		release.countDown();
		stalled.get(30, TimeUnit.SECONDS);
		await(() -> store.hotStoreArchive == null && store.hotStore.sizeInBytes() == 0);
	}

	@Test
	public void continuesWritesAfterAFailedArchival() throws Exception {
		AtomicInteger attempts = new AtomicInteger();
		archive = mockArchive();
		doAnswer(invocation -> {
			if (attempts.incrementAndGet() == 1) {
				throw new IllegalStateException("Archive unavailable");
			}
			return null;
		}).when(archive).put(any(Iterable.class));
		store = openWithMockedArchive(HotStore.MEMTABLE);

		store.put(fullStore());
		await(() -> attempts.get() == 1 && !store.isArchivalRunning());
		// the frozen hot store is kept for the next archival
		assertTrue(store.hotStoreArchive != null);

		// within the retry delay, a full hot store starts no archival and writes do not wait
		store.put(fullStore());
		store.put(fullStore());
		Thread.sleep(200);
		assertEquals(1, attempts.get());

		// the retry archives the frozen hot store
		store.archivalRetryDelayNanos = 0;
		store.put(fullStore());
		await(() -> attempts.get() == 2 && store.hotStoreArchive == null && !store.isArchivalRunning());
		assertTrue(store.hotStore.sizeInBytes() >= MAX_SIZE);

		// the next write to the full hot store archives it
		store.put(fullStore());
		await(() -> attempts.get() == 3 && store.hotStoreArchive == null && !store.isArchivalRunning());
		assertEquals(0, store.hotStore.sizeInBytes());
	}

	@Test
	public void closeWaitsForTheRunningArchival() throws Exception {
		CountDownLatch started = new CountDownLatch(1);
		CountDownLatch release = blockingArchive(started);
		store = openWithMockedArchive(HotStore.MEMTABLE);
		store.put(fullStore());
		assertTrue(started.await(30, TimeUnit.SECONDS));

		KvinPartitioned closing = store;
		store = null;
		Future<?> closed = executor.submit(closing::close);
		assertThrows(TimeoutException.class, () -> closed.get(300, TimeUnit.MILLISECONDS));
		release.countDown();
		closed.get(30, TimeUnit.SECONDS);
		verify(archive).close();

		// an archival after closing does nothing
		closing.runArchival();
		verify(archive).put(any(Iterable.class));
	}

	@Test
	public void rejectsANegativeMaximumSize() {
		assertThrows(IllegalArgumentException.class,
				() -> new KvinPartitioned(tempDir, null, archiveOptions(), HotStore.MEMTABLE, -1));
	}
}
