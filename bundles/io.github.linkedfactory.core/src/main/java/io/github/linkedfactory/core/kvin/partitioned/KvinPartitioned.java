package io.github.linkedfactory.core.kvin.partitioned;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinListener;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.leveldb.KvinLevelDb;
import io.github.linkedfactory.core.kvin.leveldb.KvinLevelDbArchiver;
import io.github.linkedfactory.core.kvin.parquet.Compactor;
import io.github.linkedfactory.core.kvin.parquet.KvinParquet;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class KvinPartitioned implements Kvin {
	static final Logger log = LoggerFactory.getLogger(KvinPartitioned.class);
	private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	protected List<KvinListener> listeners = new ArrayList<>();
	protected File path;
	protected Duration archiveInterval;
	protected File currentStorePath, currentStoreArchivePath, archiveStorePath;
	protected volatile KvinLevelDb hotStore, hotStoreArchive;
	protected KvinParquet archiveStore;

	ReadWriteLockManager lockManager = new ReadPrefReadWriteLockManager(true, 5000);

	public KvinPartitioned(File path) throws IOException {
		this(path, null);
	}

	public KvinPartitioned(File path, Duration archiveInterval) throws IOException {
		this.path = path;
		this.archiveInterval = archiveInterval;
		this.currentStorePath = new File(path, "current");
		this.currentStoreArchivePath = new File(path, "current-archive");
		this.archiveStorePath = new File(path, "archive");
		Files.createDirectories(this.currentStorePath.toPath());
		hotStore = new KvinLevelDb(this.currentStorePath);
		if (Files.exists(this.currentStoreArchivePath.toPath())) {
			hotStoreArchive = new KvinLevelDb(this.currentStoreArchivePath);
		}
		archiveStore = new KvinParquet(archiveStorePath.toString());
		scheduleCyclicArchival();
	}

	Lock writeLock() {
		try {
			return lockManager.getWriteLock();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	Lock readLock() {
		try {
			return lockManager.getReadLock();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public void runArchival() {
		log.info("Run archival");
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
			new KvinLevelDbArchiver(hotStoreArchive, archiveStore).archive();
			try {
				new Compactor(archiveStore).execute();
			} catch (IOException e) {
				log.error("Compacting archive store failed", e);
			}
		} catch (Exception e) {
			log.error("Archiving data to archive store failed", e);
		}

		try {
			writeLock = writeLock();
			this.hotStoreArchive.close();
			this.hotStoreArchive = null;
			FileUtils.deleteDirectory(this.currentStoreArchivePath);
		} catch (IOException e) {
			log.error("Deleting hot store archive failed", e);
		} finally {
			if (writeLock != null) {
				writeLock.release();
			}
		}
	}

	private void scheduleCyclicArchival() {
		if (this.archiveInterval != null) {
			scheduler.scheduleWithFixedDelay(this::runArchival, 0, archiveInterval.toMillis(), TimeUnit.MILLISECONDS);
		}
	}

	@Override
	public boolean addListener(KvinListener listener) {
		try {
			listeners.add(listener);
			return true;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean removeListener(KvinListener listener) {
		try {
			listeners.remove(listener);
			return true;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void put(KvinTuple... tuples) {
		this.put(Arrays.asList(tuples));
	}

	@Override
	public void put(Iterable<KvinTuple> tuples) {
		hotStore.put(tuples);
	}

	public void createNewHotDataStore() throws IOException {
		hotStore.close();
		FileUtils.deleteDirectory(this.currentStoreArchivePath);
		FileUtils.moveDirectory(this.currentStorePath, this.currentStoreArchivePath);
		hotStore = new KvinLevelDb(currentStorePath);
		hotStoreArchive = new KvinLevelDb(this.currentStoreArchivePath);
	}

	@Override
	public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long limit) {
		return fetch(item, property, context, KvinTuple.TIME_MAX_VALUE, 0, limit, 0, null);
	}

	@Override
	public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long end, long begin, long limit, long interval, String op) {
		IExtendedIterator<KvinTuple> internalResult = fetchInternal(item, property, context, end, begin, limit);
		if (op != null) {
			internalResult = new AggregatingIterator<>(internalResult, interval, op.trim().toLowerCase(), limit) {
				@Override
				protected KvinTuple createElement(URI item, URI property, URI context, long time, int seqNr, Object value) {
					return new KvinTuple(item, property, context, time, seqNr, value);
				}
			};
		}
		return internalResult;
	}

	protected IExtendedIterator<KvinTuple> fetchInternal(URI item, URI property, URI context, long end, long begin, long limit) {
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
						IExtendedIterator<KvinTuple> storeTuples = store.fetch(item, property, context, end, begin,
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
						isDuplicate = prevTuple != null
								&& prevTuple.time == candidate.time
								&& prevTuple.seqNr == candidate.seqNr;
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
	public boolean delete(URI item) {
		Lock readLock = readLock();
		try {
			return hotStore.delete(item);
		} finally {
			readLock.release();
		}
	}

	@Override
	public IExtendedIterator<URI> descendants(URI item) {
		Lock readLock = readLock();
		IExtendedIterator<URI> results = hotStore.descendants(item);
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
	public IExtendedIterator<URI> descendants(URI item, long limit) {
		Lock readLock = readLock();
		IExtendedIterator<URI> results = hotStore.descendants(item, limit);
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
	public IExtendedIterator<URI> properties(URI item) {
		Set<URI> properties = new HashSet<>();
		Lock readLock = readLock();
		try {
			properties.addAll(hotStore.properties(item).toList());
			if (hotStoreArchive != null) {
				properties.addAll(hotStoreArchive.properties(item).toList());
			}
			properties.addAll(archiveStore.properties(item).toList());
		} finally {
			readLock.release();
		}
		return WrappedIterator.create(properties.iterator());
	}

	@Override
	public long approximateSize(URI item, URI property, URI context, long end, long begin) {
		return 0;
	}

	@Override
	public void close() {
		Lock readLock = readLock();
		try {
			hotStore.close();
			if (hotStoreArchive != null) {
				hotStoreArchive.close();
			}
		} finally {
			readLock.release();
		}
	}
}
