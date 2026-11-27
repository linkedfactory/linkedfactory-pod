package io.github.linkedfactory.core.kvin.partitioned;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinException;
import io.github.linkedfactory.core.kvin.KvinListener;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.iceberg.KvinIceberg;
import io.github.linkedfactory.core.kvin.iceberg.KvinIcebergOptions;
import io.github.linkedfactory.core.kvin.util.AggregatingIterator;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.commons.iterator.WrappedIterator;
import net.enilink.commons.util.Pair;
import net.enilink.komma.core.URI;
import org.apache.commons.io.FileUtils;
import org.eclipse.rdf4j.common.concurrent.locks.Lock;
import org.eclipse.rdf4j.common.concurrent.locks.ReadPrefReadWriteLockManager;
import org.eclipse.rdf4j.common.concurrent.locks.ReadWriteLockManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class KvinPartitioned implements Kvin {
	static final Logger log = LoggerFactory.getLogger(KvinPartitioned.class);
	// starts the cyclic archivals of all stores; a daemon thread, as it is never shut down
	private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, task -> {
		Thread thread = new Thread(task, "kvin-partitioned-scheduler");
		thread.setDaemon(true);
		return thread;
	});
	protected List<KvinListener> listeners = new ArrayList<>();
	protected File path;
	protected Duration archiveInterval;
	protected File currentStorePath, currentStoreArchivePath;
	// location of the Iceberg archive, a path or a URI
	protected String archiveLocation;
	protected final HotStore.Factory hotStoreFactory;
	protected volatile HotStore hotStore, hotStoreArchive;
	protected Kvin archiveStore;
	// the size in bytes at which the hot store is archived, 0 for no limit
	protected final long maxHotStoreSize;
	// time after a failed archival before a full hot store starts the next one
	long archivalRetryDelayNanos = TimeUnit.MINUTES.toNanos(1);

	// held by each archival run, so that runs do not overlap
	private final ReentrantLock archivalLock = new ReentrantLock();
	// signalled when an archival run ends, to resume stalled writes
	private final Object archivalEnded = new Object();
	// runs the archivals that full hot stores request
	private final ExecutorService archiver = Executors.newSingleThreadExecutor(task -> {
		Thread thread = new Thread(task, "kvin-partitioned-archival");
		thread.setDaemon(true);
		return thread;
	});
	private final AtomicBoolean archivalRequested = new AtomicBoolean();
	private volatile long lastFailedArchival;
	private volatile boolean closed;
	private ScheduledFuture<?> cyclicArchival;

	ReadWriteLockManager lockManager = new ReadPrefReadWriteLockManager(true, 5000);

	public KvinPartitioned(File path) throws IOException {
		this(path, null, (Duration) null);
	}

	public KvinPartitioned(File path, Duration archiveInterval, Duration retentionPeriod) throws IOException {
		this(path, archiveInterval, KvinIcebergOptions.builder(new File(path, "archive").toString())
				.retentionPeriod(retentionPeriod).build());
	}

	/**
	 * Creates a store whose Iceberg archive uses the given settings. The location of the
	 * settings is the location of the archive: by default the directory {@code archive} in
	 * {@code path}, but it may also be another directory or a remote location such as
	 * {@code s3a://bucket/archive}. The cleanup interval is ignored: the archive is cleaned
	 * up after each archival run.
	 */
	public KvinPartitioned(File path, Duration archiveInterval, KvinIcebergOptions archiveOptions) throws IOException {
		this(path, archiveInterval, archiveOptions, HotStore.LEVELDB);
	}

	/**
	 * Creates a store whose Iceberg archive uses the given settings, and whose hot stores
	 * the factory opens, see {@link HotStore#LEVELDB} and {@link HotStore#MEMTABLE}. The hot
	 * stores are the directories {@code current} and {@code current-archive} in {@code path},
	 * so the same factory must be used each time the store is opened.
	 */
	public KvinPartitioned(File path, Duration archiveInterval, KvinIcebergOptions archiveOptions,
	                       HotStore.Factory hotStoreFactory) throws IOException {
		this(path, archiveInterval, archiveOptions, hotStoreFactory, 0);
	}

	/**
	 * Creates a store that also archives its hot store when it reaches the given size in
	 * bytes, see {@link HotStore#sizeInBytes()}; 0 for no limit. While the values of a full
	 * hot store are archived, new values go to an empty hot store. If that store also
	 * reaches the size before the archival ends, writes wait for the end of the archival, so
	 * the hot stores hold at most about twice the size.
	 */
	public KvinPartitioned(File path, Duration archiveInterval, KvinIcebergOptions archiveOptions,
	                       HotStore.Factory hotStoreFactory, long maxHotStoreSize) throws IOException {
		if (maxHotStoreSize < 0) {
			throw new IllegalArgumentException("The maximum hot store size must not be negative: " + maxHotStoreSize);
		}
		this.path = path;
		this.hotStoreFactory = hotStoreFactory;
		this.maxHotStoreSize = maxHotStoreSize;
		this.archiveInterval = archiveInterval;
		this.currentStorePath = new File(path, "current");
		this.currentStoreArchivePath = new File(path, "current-archive");
		this.archiveLocation = archiveOptions.location();
		Files.createDirectories(this.currentStorePath.toPath());
		hotStore = hotStoreFactory.open(this.currentStorePath);
		if (Files.exists(this.currentStoreArchivePath.toPath())) {
			try {
				hotStoreArchive = hotStoreFactory.open(this.currentStoreArchivePath);
			} catch (IOException | RuntimeException e) {
				hotStore.close();
				throw e;
			}
		}
		try {
			archiveStore = createArchiveStore(archiveOptions.toBuilder().cleanUpInterval(null).build());
		} catch (RuntimeException e) {
			// releases the LevelDB locks, so that the store can be opened again
			hotStore.close();
			if (hotStoreArchive != null) {
				hotStoreArchive.close();
			}
			archiver.shutdown();
			throw e;
		}
		scheduleCyclicArchival();
	}

	/**
	 * Creates the store that receives the archived values. Called from the constructor,
	 * so overriding methods must not use instance fields of the subclass.
	 */
	protected Kvin createArchiveStore(KvinIcebergOptions archiveOptions) {
		return new KvinIceberg(archiveOptions);
	}

	Lock writeLock() {
		try {
			return lockManager.getWriteLock();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new KvinException("Interrupted while waiting for lock", e);
		}
	}

	Lock readLock() {
		try {
			return lockManager.getReadLock();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new KvinException("Interrupted while waiting for lock", e);
		}
	}

	/**
	 * Archives the values of the hot store: starts a new hot store, copies the values of the
	 * previous one into the archive store, and then removes it. Waits for a running archival
	 * first. Does nothing after the store is closed.
	 */
	public void runArchival() {
		archivalLock.lock();
		try {
			if (closed) {
				return;
			}
			if (archive()) {
				lastFailedArchival = 0;
			} else {
				lastFailedArchival = Math.max(System.nanoTime(), 1);
			}
		} finally {
			archivalLock.unlock();
			synchronized (archivalEnded) {
				archivalEnded.notifyAll();
			}
		}
	}

	/**
	 * Runs one archival.
	 *
	 * @return true if the values of the previous hot store were archived and the store removed
	 */
	private boolean archive() {
		log.debug("Run archival");
		boolean archived = false;
		Lock writeLock = null;
		try {
			writeLock = writeLock();
			if (this.hotStoreArchive == null) {
				// the hot store archive might exist if a previous archival was interrupted
				createNewHotDataStore();
			}
		} catch (IOException e) {
			log.error("Creating archive failed", e);
		} finally {
			if (writeLock != null) {
				writeLock.release();
			}
		}

		try {
			archive(hotStoreArchive);
			try {
				writeLock = writeLock();
				this.hotStoreArchive.close();
				this.hotStoreArchive = null;
				FileUtils.deleteDirectory(this.currentStoreArchivePath);
				archived = true;
			} catch (IOException e) {
				log.error("Deleting hot store archive failed", e);
			} finally {
				if (writeLock != null) {
					writeLock.release();
				}
			}
		} catch (Exception e) {
			log.error("Archiving data to archive store failed", e);
		}

		if (archiveStore instanceof KvinIceberg icebergStore) {
			// after the hot store archive is removed, so that a failure does not archive values twice
			try {
				icebergStore.cleanUp();
			} catch (Exception e) {
				log.error("Cleaning up archive store failed", e);
			}
		}
		return archived;
	}

	/**
	 * True while an archival runs, for tests.
	 */
	boolean isArchivalRunning() {
		return archivalLock.isLocked();
	}

	private boolean isFull(HotStore store) {
		return maxHotStoreSize > 0 && store.sizeInBytes() >= maxHotStoreSize;
	}

	/**
	 * Starts an archival in the background unless one is already requested, or an archival
	 * failed less than the retry delay ago.
	 */
	private void requestArchival() {
		long failed = lastFailedArchival;
		if (closed || failed != 0 && System.nanoTime() - failed < archivalRetryDelayNanos) {
			return;
		}
		if (archivalRequested.compareAndSet(false, true)) {
			try {
				archiver.execute(() -> {
					archivalRequested.set(false);
					runArchival();
				});
			} catch (RejectedExecutionException e) {
				// the store is closing
				archivalRequested.set(false);
			}
		}
	}

	/**
	 * Waits while the hot store is full and the previous hot store is still archived, as the
	 * full hot store can only be archived after that.
	 */
	private void awaitHotStoreCapacity() {
		if (maxHotStoreSize == 0) {
			return;
		}
		synchronized (archivalEnded) {
			boolean logged = false;
			while (!closed && archivalLock.isLocked() && hotStoreArchive != null && isFull(hotStore)) {
				if (!logged) {
					log.warn("Hot store is full, writes wait until the running archival ends");
					logged = true;
				}
				try {
					// also checks the condition again if the notification is missed
					archivalEnded.wait(100);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new KvinException("Interrupted while waiting for the archival", e);
				}
			}
		}
	}

	/**
	 * Copies all values of the hot store into the archive store. An Iceberg archive fetches
	 * the values one series at a time in the order of its IDs, so that it writes sorted data
	 * files without sorting the values in memory.
	 */
	private void archive(HotStore store) {
		if (archiveStore instanceof KvinIceberg icebergStore) {
			try (IExtendedIterator<KvinTuple> series = store.fetchLatest()) {
				icebergStore.appendSeries(store, series);
			}
		} else {
			try (IExtendedIterator<KvinTuple> tuples = store.fetchAll()) {
				archiveStore.put(tuples);
			}
		}
	}

	private void scheduleCyclicArchival() {
		if (this.archiveInterval != null) {
			cyclicArchival = scheduler.scheduleWithFixedDelay(this::runArchival, 0, archiveInterval.toMillis(),
					TimeUnit.MILLISECONDS);
		}
	}

	@Override
	public boolean addListener(KvinListener listener) {
		listeners.add(listener);
		return hotStore.addListener(listener);
	}

	@Override
	public boolean removeListener(KvinListener listener) {
		listeners.remove(listener);
		return hotStore.removeListener(listener);
	}

	@Override
	public void put(KvinTuple... tuples) {
		this.put(Arrays.asList(tuples));
	}

	/**
	 * Stores the tuples in the hot store. If the hot store has a maximum size, a full hot
	 * store starts an archival, and writes wait while the hot store is full and the previous
	 * one is still archived.
	 */
	@Override
	public void put(Iterable<KvinTuple> tuples) {
		awaitHotStoreCapacity();
		Lock readLock = readLock();
		boolean full;
		try {
			hotStore.put(tuples);
			full = isFull(hotStore);
		} finally {
			readLock.release();
		}
		if (full) {
			requestArchival();
		}
	}

	public void createNewHotDataStore() throws IOException {
		hotStore.close();
		FileUtils.deleteDirectory(this.currentStoreArchivePath);
		FileUtils.moveDirectory(this.currentStorePath, this.currentStoreArchivePath);
		hotStore = hotStoreFactory.open(currentStorePath);
		for (KvinListener listener : listeners) {
			// register listeners on new hot store
			hotStore.addListener(listener);
		}
		hotStoreArchive = hotStoreFactory.open(this.currentStoreArchivePath);
	}

	@Override
	public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long limit) {
		return fetch(item, property, context, KvinTuple.TIME_MAX_VALUE, 0, limit, 0, null);
	}

	@Override
	public IExtendedIterator<KvinTuple> fetch(List<URI> items, List<URI> properties, URI context, long end, long begin, long limit, long interval, String op) {
		IExtendedIterator<KvinTuple> internalResult = fetchInternal(items, properties, context, end, begin, op == null ? limit : 0L);
		if (op != null) {
			internalResult = new AggregatingIterator<>(internalResult, interval, op.trim().toLowerCase(), limit) {
				@Override
				protected KvinTuple createElement(URI item, URI property, URI context, long time, int seqNr, Object value) {
					return new KvinTuple(context, item, property, time, seqNr, value);
				}
			};
		}
		return internalResult;
	}

	@Override
	public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long end, long begin, long limit, long interval, String op) {
		var properties = property == null ? Collections.<URI>emptyList() : List.of(property);
		return fetch(List.of(item),	properties, context, end, begin, limit, interval, op);
	}

	protected IExtendedIterator<KvinTuple> fetchInternal(List<URI> items, List<URI> properties, URI context, long end, long begin, long limit) {
		Lock readLock = readLock();
		return new NiceIterator<>() {
			final PriorityQueue<Pair<KvinTuple, IExtendedIterator<KvinTuple>>> nextTuples = new PriorityQueue<>(
					Comparator.comparing(Pair::getFirst, (a, b) -> {
						int diff = a.property.equals(b.property) ? 0 : a.toString().compareTo(b.toString());
						if (diff != 0) {
							return diff;
						}
						diff = Long.compare(a.time, b.time);
						if (diff != 0) {
							// time is reverse
							return -diff;
						}
						diff = Integer.compare(a.seqNr, b.seqNr);
						if (diff != 0) {
							// seqNr is reverse
							return -diff;
						}
						return 0;
					}));
			long propertyValueCount;
			KvinTuple prevTuple, nextTuple;
			boolean closed;

			@Override
			public boolean hasNext() {
				if (nextTuple != null) {
					return true;
				}
				if (prevTuple == null) {
					// this is the case if the iterator is not yet initialized
					List<Kvin> stores = new ArrayList<>();
					stores.add(hotStore);
					if (hotStoreArchive != null) {
						stores.add(hotStoreArchive);
					}
					stores.add(archiveStore);
					for (Kvin store : stores) {
						IExtendedIterator<KvinTuple> storeTuples = store.fetch(items, properties, context, end, begin,
								limit, 0L, null);
						if (storeTuples.hasNext()) {
							nextTuples.add(new Pair<>(storeTuples.next(), storeTuples));
						} else {
							storeTuples.close();
						}
					}
				}

				while (nextTuple == null && !nextTuples.isEmpty()) {
					var min = nextTuples.poll();
					var candidate = min.getFirst();

					boolean isDuplicate = false;
					if (prevTuple == null ||
							!candidate.property.equals(prevTuple.property) ||
							!candidate.item.equals(prevTuple.item)) {
						propertyValueCount = 1;
					} else {
						// omit duplicates in terms of id, time, and seqNr
						isDuplicate = prevTuple.time == candidate.time && prevTuple.seqNr == candidate.seqNr;
					}

					if (min.getSecond().hasNext()) {
						nextTuples.add(new Pair<>(min.getSecond().next(), min.getSecond()));
					} else {
						min.getSecond().close();
					}

					// omit duplicates
					if (isDuplicate) {
						continue;
					}

					// skip properties if limit is reached
					if (limit != 0 && propertyValueCount > limit) {
						continue;
					}

					prevTuple = nextTuple;
					nextTuple = candidate;
					propertyValueCount++;
				}

				if (nextTuple != null) {
					return true;
				} else {
					close();
					return false;
				}
			}

			@Override
			public KvinTuple next() {
				if (hasNext()) {
					KvinTuple result = nextTuple;
					prevTuple = result;
					nextTuple = null;
					return result;
				}
				throw new NoSuchElementException();
			}

			@Override
			public void close() {
				if (!closed) {
					try {
						while (!nextTuples.isEmpty()) {
							try {
								nextTuples.poll().getSecond().close();
							} catch (Exception e) {
								// TODO log exception
							}
						}
					} finally {
						readLock.release();
						closed = true;
					}
				}
			}
		};
	}

	@Override
	public long delete(URI item, URI property, URI context, long end, long begin) {
		Lock readLock = readLock();
		try {
			return hotStore.delete(item, property, context, end, begin);
		} finally {
			readLock.release();
		}
	}

	@Override
	public boolean delete(URI item, URI context) {
		Lock readLock = readLock();
		try {
			return hotStore.delete(item, context);
		} finally {
			readLock.release();
		}
	}

	/**
	 * Deletes the values of the context in the hot store. Like the other delete methods,
	 * this does not change archived values.
	 */
	@Override
	public long delete(URI context) {
		Lock readLock = readLock();
		try {
			return hotStore.delete(context);
		} finally {
			readLock.release();
		}
	}

	@Override
	public IExtendedIterator<URI> descendants(URI item, URI context) {
		Lock readLock = readLock();
		IExtendedIterator<URI> results = hotStore.descendants(item, context);
		return new NiceIterator<>() {
			boolean closed;

			@Override
			public boolean hasNext() {
				if (results.hasNext()) {
					return true;
				} else {
					close();
					return false;
				}
			}

			@Override
			public URI next() {
				if (hasNext()) {
					return results.next();
				}
				throw new NoSuchElementException();
			}

			@Override
			public void close() {
				if (!closed) {
					try {
						results.close();
					} finally {
						readLock.release();
						closed = true;
					}
				}
			}
		};
	}

	@Override
	public IExtendedIterator<URI> descendants(URI item, URI context, long limit) {
		Lock readLock = readLock();
		IExtendedIterator<URI> results = hotStore.descendants(item, context, limit);
		return new NiceIterator<>() {
			boolean closed;

			@Override
			public boolean hasNext() {
				if (results.hasNext()) {
					return true;
				} else {
					close();
					return false;
				}
			}

			@Override
			public URI next() {
				if (hasNext()) {
					return results.next();
				}
				throw new NoSuchElementException();
			}

			@Override
			public void close() {
				if (!closed) {
					try {
						results.close();
					} finally {
						readLock.release();
						closed = true;
					}
				}
			}
		};
	}

	@Override
	public IExtendedIterator<URI> properties(URI item, URI context) {
		Set<URI> properties = new HashSet<>();
		Lock readLock = readLock();
		try {
			properties.addAll(hotStore.properties(item, context).toList());
			if (hotStoreArchive != null) {
				properties.addAll(hotStoreArchive.properties(item, context).toList());
			}
			properties.addAll(archiveStore.properties(item, context).toList());
		} finally {
			readLock.release();
		}
		return WrappedIterator.create(properties.iterator());
	}

	/**
	 * Closes the store. Cancels further archivals and waits for a running one.
	 */
	@Override
	public void close() {
		if (cyclicArchival != null) {
			cyclicArchival.cancel(false);
		}
		archiver.shutdown();
		archivalLock.lock();
		try {
			closed = true;
		} finally {
			archivalLock.unlock();
		}
		synchronized (archivalEnded) {
			archivalEnded.notifyAll();
		}
		Lock readLock = readLock();
		try {
			try {
				hotStore.close();
				if (hotStoreArchive != null) {
					hotStoreArchive.close();
				}
			} finally {
				archiveStore.close();
			}
		} finally {
			readLock.release();
		}
	}
}
