package io.github.linkedfactory.core.kvin.memtable;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.Record;
import io.github.linkedfactory.core.kvin.records.KvinRecord;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class KvinMemTableTest {
	private final URI item = URIs.createURI("http://example.org/item");
	private final URI other = URIs.createURI("http://example.org/other");
	private final URI property = URIs.createURI("http://example.org/property");
	private final URI contextA = URIs.createURI("ctx:a");
	private final URI contextB = URIs.createURI("ctx:b");

	private Path directory;
	private KvinMemTable store;

	@Before
	public void setup() throws IOException {
		directory = Files.createTempDirectory("memtable-test");
		store = new KvinMemTable(directory);
	}

	@After
	public void cleanup() throws IOException {
		store.close();
		FileUtils.deleteDirectory(directory.toFile());
	}

	private long id(URI uri, KvinMemTable.EntryType type) {
		return store.toId(uri, type, false, null);
	}

	private int count(URI item, URI context) {
		return store.tableFiles.fetch(id(item, KvinMemTable.EntryType.SubjectToId),
				id(property, KvinMemTable.EntryType.PropertyToId),
				id(context, KvinMemTable.EntryType.ContextToId), 100).toList().size();
	}

	@Test
	public void deletesContext() {
		store.put(new KvinTuple(contextA, item, property, 1, 0, 1),
				new KvinTuple(contextA, item, property, 2, 0, 2),
				new KvinTuple(contextA, other, property, 1, 0, 3),
				new KvinTuple(contextB, item, property, 1, 0, 4),
				new KvinTuple(Kvin.DEFAULT_CONTEXT, item, property, 1, 0, 5));

		assertEquals(3, store.delete(contextA));
		assertEquals(0, count(item, contextA));
		assertEquals(0, count(other, contextA));
		assertEquals(1, count(item, contextB));
		assertEquals(1, count(item, Kvin.DEFAULT_CONTEXT));

		assertEquals(0, store.delete(contextA));
		assertEquals(0, store.delete(URIs.createURI("ctx:unknown")));

		store.put(new KvinTuple(contextA, other, property, 3, 0, 6));
		assertEquals(1, count(other, contextA));

		// null selects the default context
		assertEquals(1, store.delete((URI) null));
		assertEquals(0, count(item, Kvin.DEFAULT_CONTEXT));
		assertEquals(List.of(), store.tableFiles.properties(id(item, KvinMemTable.EntryType.SubjectToId),
				id(Kvin.DEFAULT_CONTEXT, KvinMemTable.EntryType.ContextToId)).toList());
	}

	@Test
	public void deletesNothingBeforeTheFirstWrite() {
		assertEquals(0, store.delete(contextA));
	}

	@Test
	public void keepsContextDeleteAfterReopening() throws IOException {
		store.put(new KvinTuple(contextA, item, property, 1, 0, 1),
				new KvinTuple(contextB, item, property, 1, 0, 2));
		assertEquals(1, store.delete(contextA));
		store.close();

		store = new KvinMemTable(directory);
		store.put(new KvinTuple(contextB, other, property, 2, 0, 3));
		assertEquals(0, count(item, contextA));
		assertEquals(1, count(item, contextB));
		assertEquals(1, count(other, contextB));
	}

	/**
	 * Fails each put of a record while {@link #failing} is set.
	 */
	static class FailingTableFileSet extends TableFileSet {
		volatile boolean failing = true;

		FailingTableFileSet(Path directory) throws IOException {
			super(directory);
		}

		@Override
		synchronized void put(KvinRecord record) throws IOException {
			if (failing) {
				throw new IOException("Disk full");
			}
			super.put(record);
		}
	}

	private FailingTableFileSet reopenFailing() throws IOException {
		store.close();
		FailingTableFileSet[] tableFiles = new FailingTableFileSet[1];
		store = new KvinMemTable(directory, path -> tableFiles[0] = new FailingTableFileSet(path));
		return tableFiles[0];
	}

	private List<KvinTuple> tuples(URI context, int count) {
		List<KvinTuple> tuples = new ArrayList<>();
		for (int i = 0; i < count; i++) {
			tuples.add(new KvinTuple(context, URIs.createURI("http://example.org/item" + i), property, 1, 0, i));
		}
		return tuples;
	}

	// more tuples than fit into the queue of the writer, so that a stopped writer would block the put
	@Test(timeout = 30_000)
	public void throwsFailureOfTheWriter() throws IOException {
		FailingTableFileSet tableFiles = reopenFailing();
		var e = assertThrows(UncheckedIOException.class, () -> store.put(tuples(contextA, 50_000)));
		assertEquals("Disk full", e.getCause().getMessage());

		tableFiles.failing = false;
		store.put(new KvinTuple(contextB, item, property, 1, 0, 1));
		assertEquals(1, count(item, contextB));
		// the records left in the queue by the failed put are not written by the next put
		assertEquals(0, count(URIs.createURI("http://example.org/item1"), contextA));
		assertTrue(store.pendingIds.isEmpty());
	}

	@Test
	public void throwsFailureOfASynchronousWrite() throws IOException {
		reopenFailing();
		store.async = false;
		var e = assertThrows(UncheckedIOException.class, () -> store.put(tuples(contextA, 10)));
		assertEquals("Disk full", e.getCause().getMessage());
		assertTrue(store.pendingIds.isEmpty());
	}

	@Test
	public void writesTheIdsOfAFailedPut() throws IOException {
		reopenFailing();
		assertThrows(UncheckedIOException.class, () -> store.put(new KvinTuple(contextA, item, property, 1, 0, 1)));
		store.close();

		store = new KvinMemTable(directory);
		assertNotEquals(-1, id(item, KvinMemTable.EntryType.SubjectToId));
	}

	@Test
	public void keepsOnlyUnwrittenIdsPending() throws IOException {
		int count = 3 * KvinMemTable.MAX_PENDING_IDS;
		List<KvinTuple> tuples = new ArrayList<>(tuples(contextA, count));
		// the same item in a later ID batch of the put reuses the written ID
		tuples.add(new KvinTuple(contextA, URIs.createURI("http://example.org/item0"), property, 2, 0, 1));
		store.put(tuples);

		assertTrue(store.pendingIds.isEmpty());
		assertTrue(store.uriToIdCache.size() <= 20_000);
		assertEquals(2, count(URIs.createURI("http://example.org/item0"), contextA));
		store.close();

		store = new KvinMemTable(directory);
		for (int i = 0; i < count; i++) {
			assertNotEquals(-1, id(URIs.createURI("http://example.org/item" + i), KvinMemTable.EntryType.SubjectToId));
		}
	}

	private final URI otherProperty = URIs.createURI("http://example.org/another");

	private List<Long> times(List<KvinTuple> tuples) {
		return tuples.stream().map(t -> t.time).toList();
	}

	@Test
	public void fetchesValuesNewestFirst() {
		store.put(new KvinTuple(contextA, item, property, 1, 0, 1.0),
				new KvinTuple(contextA, item, property, 3, 0, 3.0),
				new KvinTuple(contextA, item, property, 2, 0, 2.0),
				new KvinTuple(contextA, item, property, 2, 1, 2.5),
				new KvinTuple(contextB, item, property, 4, 0, 4.0));

		List<KvinTuple> tuples = store.fetch(item, property, contextA, 0).toList();
		assertEquals(List.of(3L, 2L, 2L, 1L), times(tuples));
		assertEquals(List.of(3.0, 2.5, 2.0, 1.0), tuples.stream().map(t -> t.value).toList());
		assertEquals(1, tuples.get(1).seqNr);
		assertEquals(contextA, tuples.get(0).context);
		assertEquals(item, tuples.get(0).item);
		assertEquals(property, tuples.get(0).property);

		assertEquals(List.of(3L, 2L), times(store.fetch(item, property, contextA, 2).toList()));
		assertEquals(List.of(2L, 2L), times(store.fetch(item, property, contextA, 2, 2, 0, 0, null).toList()));
		assertEquals(List.of(4L), times(store.fetch(item, property, contextB, 0).toList()));
	}

	@Test
	public void fetchesNothingForUnknownUris() {
		store.put(new KvinTuple(contextA, item, property, 1, 0, 1));
		assertEquals(List.of(), store.fetch(other, property, contextA, 0).toList());
		assertEquals(List.of(), store.fetch(item, otherProperty, contextA, 0).toList());
		assertEquals(List.of(), store.fetch(item, property, URIs.createURI("ctx:unknown"), 0).toList());
		assertEquals(List.of(), store.fetch(item, null, URIs.createURI("ctx:unknown"), 0).toList());
	}

	@Test
	public void fetchesAllPropertiesWithALimitForEach() {
		store.put(new KvinTuple(contextA, item, property, 1, 0, 1),
				new KvinTuple(contextA, item, property, 2, 0, 2),
				new KvinTuple(contextA, item, otherProperty, 1, 0, 3),
				new KvinTuple(contextA, item, otherProperty, 2, 0, 4));

		List<KvinTuple> tuples = store.fetch(item, null, contextA, 1).toList();
		// ordered by property URI
		assertEquals(List.of(otherProperty, property), tuples.stream().map(t -> t.property).toList());
		assertEquals(List.of(2L, 2L), times(tuples));
		assertEquals(4, store.fetch(item, null, contextA, 0).toList().size());
	}

	@Test
	public void fetchesTheDefaultContextForNull() {
		store.put(new KvinTuple(Kvin.DEFAULT_CONTEXT, item, property, 1, 0, 1));
		List<KvinTuple> tuples = store.fetch(item, property, null, 0).toList();
		assertEquals(1, tuples.size());
		assertEquals(Kvin.DEFAULT_CONTEXT, tuples.get(0).context);
		assertEquals(List.of(property), store.properties(item, null).toList());
	}

	@Test
	public void aggregatesValues() {
		store.put(new KvinTuple(contextA, item, property, 1, 0, 1),
				new KvinTuple(contextA, item, property, 2, 0, 5),
				new KvinTuple(contextA, item, property, 3, 0, 3));
		List<KvinTuple> tuples = store.fetch(item, property, contextA, 10, 0, 0, 10, "max").toList();
		assertEquals(1, tuples.size());
		assertEquals(5, ((Number) tuples.get(0).value).intValue());
	}

	@Test
	public void decodesStoredValues() {
		URI longProperty = URIs.createURI("http://example.org/" + "p".repeat(300));
		Record record = new Record(longProperty, 1.5).append(new Record(property, "text"));
		Object[] array = {1, "two", item};
		store.put(new KvinTuple(contextA, item, property, 1, 0, "text"),
				new KvinTuple(contextA, item, property, 2, 0, other),
				new KvinTuple(contextA, item, property, 3, 0, record),
				new KvinTuple(contextA, item, property, 4, 0, array),
				new KvinTuple(contextA, item, property, 5, 0, 42L));

		List<Object> values = store.fetch(item, property, contextA, 0).toList().stream().map(t -> t.value).toList();
		assertEquals(42L, values.get(0));
		assertArrayEquals(array, (Object[]) values.get(1));
		assertEquals(record, values.get(2));
		assertEquals(other, values.get(3));
		assertEquals("text", values.get(4));
	}

	@Test
	public void fetchesValuesAfterReopening() throws IOException {
		store.put(new KvinTuple(contextA, item, property, 1, 0, 1),
				new KvinTuple(contextA, item, otherProperty, 2, 0, 2));
		store.close();

		store = new KvinMemTable(directory);
		assertEquals(List.of(1L), times(store.fetch(item, property, contextA, 0).toList()));
		assertEquals(List.of(otherProperty, property), store.properties(item, contextA).toList());
	}

	@Test
	public void listsPropertiesWithValues() {
		store.put(new KvinTuple(contextA, item, property, 1, 0, 1),
				new KvinTuple(contextA, item, otherProperty, 1, 0, 2),
				new KvinTuple(contextB, other, property, 1, 0, 3));

		assertEquals(List.of(otherProperty, property), store.properties(item, contextA).toList());
		assertEquals(List.of(), store.properties(item, contextB).toList());
		assertEquals(List.of(), store.properties(URIs.createURI("http://example.org/unknown"), contextA).toList());

		store.delete(contextA);
		assertEquals(List.of(), store.properties(item, contextA).toList());
	}

	@Test
	public void listsDescendantsWithValuesInTheContext() {
		URI parent = URIs.createURI("http://example.org/machine");
		URI child = URIs.createURI("http://example.org/machine/axis");
		URI otherChild = URIs.createURI("http://example.org/machine/spindle");
		URI sibling = URIs.createURI("http://example.org/robot");
		store.put(new KvinTuple(contextA, parent, property, 1, 0, 1),
				new KvinTuple(contextA, child, property, 1, 0, 2),
				new KvinTuple(contextB, otherChild, property, 1, 0, 3),
				new KvinTuple(contextA, sibling, property, 1, 0, 4));

		assertEquals(List.of(parent, child), store.descendants(parent, contextA).toList());
		assertEquals(List.of(otherChild), store.descendants(parent, contextB).toList());
		assertEquals(List.of(parent), store.descendants(parent, contextA, 1).toList());
		assertEquals(List.of(), store.descendants(parent, contextA, 0).toList());
		assertEquals(List.of(), store.descendants(parent, URIs.createURI("ctx:unknown")).toList());

		store.delete(contextA);
		assertEquals(List.of(), store.descendants(parent, contextA).toList());
	}

	@Test(timeout = 60_000)
	public void storesConcurrentPuts() throws Exception {
		int threads = 4;
		int count = 20_000;
		ExecutorService executor = Executors.newFixedThreadPool(threads);
		try {
			List<Future<?>> futures = new ArrayList<>();
			for (int t = 0; t < threads; t++) {
				URI threadItem = URIs.createURI("http://example.org/thread" + t);
				futures.add(executor.submit(() -> {
					List<KvinTuple> tuples = new ArrayList<>();
					for (int i = 1; i <= count; i++) {
						tuples.add(new KvinTuple(contextA, threadItem, property, i, 0, i));
						if (tuples.size() == 1000) {
							store.put(tuples);
							tuples.clear();
						}
					}
				}));
			}
			for (Future<?> future : futures) {
				future.get();
			}
		} finally {
			executor.shutdown();
		}
		for (int t = 0; t < threads; t++) {
			URI threadItem = URIs.createURI("http://example.org/thread" + t);
			assertEquals(count, store.fetch(threadItem, property, contextA, 0).toList().size());
		}
	}

	@Test
	public void deletesTimeRanges() throws IOException {
		store.put(new KvinTuple(contextA, item, property, 1, 0, 1),
				new KvinTuple(contextA, item, property, 2, 0, 2),
				new KvinTuple(contextA, item, property, 3, 0, 3),
				new KvinTuple(contextA, item, otherProperty, 2, 0, 4),
				new KvinTuple(contextB, item, property, 2, 0, 5));

		assertEquals(1, store.delete(item, property, contextA, 2, 2));
		assertEquals(List.of(3L, 1L), times(store.fetch(item, property, contextA, 0).toList()));
		assertEquals(1, store.fetch(item, otherProperty, contextA, 0).toList().size());
		assertEquals(1, store.fetch(item, property, contextB, 0).toList().size());

		// all properties
		assertEquals(3, store.delete(item, null, contextA, 3, 1));
		assertEquals(List.of(), store.fetch(item, null, contextA, 0).toList());

		assertEquals(0, store.delete(other, property, contextA, 3, 1));
		assertEquals(0, store.delete(item, URIs.createURI("http://example.org/unknown"), contextB, 3, 1));
		store.close();

		store = new KvinMemTable(directory);
		assertEquals(List.of(), store.fetch(item, null, contextA, 0).toList());
		assertEquals(1, store.fetch(item, property, contextB, 0).toList().size());
	}

	@Test
	public void deletesItems() throws IOException {
		store.put(new KvinTuple(contextA, item, property, 1, 0, 1),
				new KvinTuple(contextA, item, otherProperty, 1, 0, 2),
				new KvinTuple(contextA, other, property, 1, 0, 3),
				new KvinTuple(contextB, item, property, 1, 0, 4));

		assertTrue(store.delete(item, contextA));
		assertEquals(List.of(), store.fetch(item, null, contextA, 0).toList());
		assertEquals(List.of(), store.properties(item, contextA).toList());
		assertEquals(1, store.fetch(other, property, contextA, 0).toList().size());
		assertEquals(1, store.fetch(item, property, contextB, 0).toList().size());

		assertEquals(false, store.delete(item, contextA));
		assertEquals(false, store.delete(URIs.createURI("http://example.org/unknown"), contextA));

		// the item can be used again
		store.put(new KvinTuple(contextA, item, property, 2, 0, 5));
		assertEquals(List.of(2L), times(store.fetch(item, property, contextA, 0).toList()));
		store.close();

		store = new KvinMemTable(directory);
		assertEquals(List.of(2L), times(store.fetch(item, property, contextA, 0).toList()));
	}

	@Test
	public void fetchesAllValues() {
		Record record = new Record(property, "text");
		store.put(new KvinTuple(contextA, item, property, 1, 0, 1.5),
				new KvinTuple(contextA, item, property, 2, 0, record),
				new KvinTuple(contextB, other, otherProperty, 1, 0, item));
		store.delete(other, contextB);
		store.put(new KvinTuple(Kvin.DEFAULT_CONTEXT, other, property, 3, 0, "x"));

		List<KvinTuple> tuples = store.fetchAll().toList();
		assertEquals(List.of(
				new KvinTuple(contextA, item, property, 2, 0, record),
				new KvinTuple(contextA, item, property, 1, 0, 1.5),
				new KvinTuple(Kvin.DEFAULT_CONTEXT, other, property, 3, 0, "x")), tuples);
	}

	/**
	 * Records the notifications of a store.
	 */
	static class RecordingListener implements io.github.linkedfactory.core.kvin.KvinListener {
		final List<URI> items = new java.util.concurrent.CopyOnWriteArrayList<>();
		final List<KvinTuple> values = new java.util.concurrent.CopyOnWriteArrayList<>();

		@Override
		public void entityCreated(URI item) {
			items.add(item);
		}

		@Override
		public void valueAdded(URI item, URI property, URI context, long time, long seqNr, Object value) {
			values.add(new KvinTuple(context, item, property, time, (int) seqNr, value));
		}
	}

	@Test
	public void notifiesListenersOfNewItemsAndValues() throws IOException {
		RecordingListener listener = new RecordingListener();
		assertTrue(store.addListener(listener));
		assertFalse(store.addListener(listener));

		List<KvinTuple> tuples = List.of(new KvinTuple(contextA, item, property, 1, 0, 1.0),
				new KvinTuple(contextA, item, property, 2, 1, 2.0),
				new KvinTuple(contextB, other, otherProperty, 3, 0, "text"));
		store.put(tuples);
		assertEquals(List.of(item, other), listener.items);
		assertEquals(tuples, listener.values);

		// known items are not created again, also after reopening
		store.close();
		store = new KvinMemTable(directory);
		store.addListener(listener);
		store.put(new KvinTuple(contextB, item, property, 4, 0, 4.0));
		assertEquals(List.of(item, other), listener.items);
		assertEquals(4, listener.values.size());

		assertTrue(store.removeListener(listener));
		assertFalse(store.removeListener(listener));
		store.put(new KvinTuple(contextA, URIs.createURI("http://example.org/new"), property, 5, 0, 5.0));
		assertEquals(2, listener.items.size());
		assertEquals(4, listener.values.size());
	}

	@Test
	public void notifiesListenersOfValuesThatCanOnlyBeIteratedOnce() {
		RecordingListener listener = new RecordingListener();
		store.addListener(listener);
		List<KvinTuple> tuples = tuples(contextA, 3);
		store.put(net.enilink.commons.iterator.WrappedIterator.create(tuples.iterator()));
		assertEquals(tuples, listener.values);
		assertEquals(3, store.fetchAll().toList().size());
	}

	@Test
	public void notifiesNoValuesOfAFailedPut() throws IOException {
		reopenFailing();
		RecordingListener listener = new RecordingListener();
		store.addListener(listener);
		assertThrows(UncheckedIOException.class, () -> store.put(new KvinTuple(contextA, item, property, 1, 0, 1)));
		// the ID of the item is stored
		assertEquals(List.of(item), listener.items);
		assertEquals(List.of(), listener.values);
	}

	@Test
	public void storesValuesIfAListenerFails() {
		RecordingListener listener = new RecordingListener();
		store.addListener(new io.github.linkedfactory.core.kvin.KvinListener() {
			@Override
			public void entityCreated(URI item) {
				throw new IllegalStateException("Listener failed");
			}

			@Override
			public void valueAdded(URI item, URI property, URI context, long time, long seqNr, Object value) {
				throw new IllegalStateException("Listener failed");
			}
		});
		store.addListener(listener);

		store.put(new KvinTuple(contextA, item, property, 1, 0, 1));
		assertEquals(List.of(item), listener.items);
		assertEquals(1, listener.values.size());
		assertEquals(1, count(item, contextA));
	}

	@Test(timeout = 30_000)
	public void allowsListenersToUseTheStore() throws Exception {
		URI derived = URIs.createURI("http://example.org/derived");
		List<Integer> fetched = new java.util.concurrent.CopyOnWriteArrayList<>();
		store.addListener(new io.github.linkedfactory.core.kvin.KvinListener() {
			@Override
			public void entityCreated(URI item) {
			}

			@Override
			public void valueAdded(URI item, URI property, URI context, long time, long seqNr, Object value) {
				if (item.equals(KvinMemTableTest.this.item)) {
					fetched.add(store.fetch(item, property, context, 0).toList().size());
					// from another thread, which waits for the lock of a running put
					var executor = Executors.newSingleThreadExecutor();
					try {
						executor.submit(() -> store.put(new KvinTuple(context, derived, property, time, 0, value))).get();
					} catch (Exception e) {
						throw new IllegalStateException(e);
					} finally {
						executor.shutdown();
					}
				}
			}
		});
		store.put(new KvinTuple(contextA, item, property, 1, 0, 1));
		assertEquals(List.of(1), fetched);
		assertEquals(1, count(derived, contextA));
	}

	private int unsyncedBytes() {
		return store.tableFiles.getFiles().stream().mapToInt(TableFile::unsyncedBytes).sum();
	}

	@Test
	public void syncsEachWriteIfRequested() throws IOException {
		store.put(new KvinTuple(contextA, item, property, 1, 0, 1));
		assertTrue("Without syncing, the operating system persists writes", unsyncedBytes() > 0);
		store.close();

		store = new KvinMemTable(directory, true);
		store.put(new KvinTuple(contextA, item, property, 2, 0, 2));
		assertEquals(0, unsyncedBytes());
		assertEquals(1, store.delete(item, property, contextA, 1, 1));
		assertEquals(0, unsyncedBytes());
		assertTrue(store.delete(item, contextA));
		assertEquals(0, unsyncedBytes());
		store.put(new KvinTuple(contextB, item, property, 3, 0, 3));
		assertEquals(1, store.delete(contextB));
		assertEquals(0, unsyncedBytes());
	}

	@Test
	public void closesTheGroupOfEachPut() throws IOException {
		store.put(tuples(contextA, 100));
		assertEquals(0, store.tableFiles.getFiles().stream().mapToInt(TableFile::uncommittedBytes).sum());

		// also after a failed put, so that the stored records are kept
		FailingTableFileSet tableFiles = reopenFailing();
		tableFiles.failing = false;
		store.put(new KvinTuple(contextA, item, property, 1, 0, 1));
		tableFiles.failing = true;
		assertThrows(UncheckedIOException.class, () -> store.put(new KvinTuple(contextA, item, property, 2, 0, 2)));
		assertEquals(0, store.tableFiles.getFiles().stream().mapToInt(TableFile::uncommittedBytes).sum());
	}

	@Test
	public void storesValuesOfTheNullContextInTheDefaultContext() {
		store.put(new KvinTuple(null, item, property, 1, 0, 1));
		assertEquals(1, count(item, Kvin.DEFAULT_CONTEXT));
		assertEquals(1, store.fetch(item, property, null, 0).toList().size());
		assertEquals(Kvin.DEFAULT_CONTEXT, store.fetchAll().toList().get(0).context);
	}
}
