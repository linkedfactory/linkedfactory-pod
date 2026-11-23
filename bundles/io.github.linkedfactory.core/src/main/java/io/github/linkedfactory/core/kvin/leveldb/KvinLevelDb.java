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
package io.github.linkedfactory.core.kvin.leveldb;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.Striped;
import io.github.linkedfactory.core.kvin.Data;
import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinListener;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.Record;
import io.github.linkedfactory.core.kvin.util.AggregatingIterator;
import io.github.linkedfactory.core.kvin.util.Values;
import io.github.linkedfactory.core.kvin.util.Varint;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.commons.iterator.UniqueExtendedIterator;
import net.enilink.commons.iterator.WrappedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

/**
 * Indirect mapping of (item, property) -> ID and (ID, time, sequence-nr) -> value.
 */
public class KvinLevelDb extends KvinLevelDbBase implements Kvin {
	private static final int DELETE_BATCH_SIZE = 100000;
	private static final int MAX_VALUES_BATCH_SIZE = 1000000;

	/**
	 * Cache key for URI -> ID mappings.
	 */
	protected record UriKey(String uri, int entryType) {
	}

	/**
	 * Cache key for (item, context, property) -> ID mappings.
	 */
	protected record ScpKey(String item, String context, String property) {
	}

	protected final Striped<ReadWriteLock> locks = Striped.readWriteLock(64);

	protected final AtomicInteger activeWrites = new AtomicInteger(0);
	protected final Cache<UriKey, byte[]> uriToIdCacheWrite = CacheBuilder.newBuilder().build();
	protected final Cache<UriKey, byte[]> uriToIdCache = CacheBuilder.newBuilder().maximumSize(20000).build();
	protected final Cache<ScpKey, byte[]> scpToIdCache = CacheBuilder.newBuilder().maximumSize(20000).build();

	protected final DB ids;
	protected final DB values;
	protected final Set<KvinListener> listeners = new CopyOnWriteArraySet<>();

	public final AtomicLong[] nextIds;

	public KvinLevelDb(File path) {
		try {
			ids = factory.open(new File(path, "ids"), createOptions(false));
			values = factory.open(new File(path, "values"), createOptions(true));
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		nextIds = Arrays.stream(readNextIds(List.of(EntryType.SubjectToId, EntryType.PropertyToId,
				EntryType.ContextToId, EntryType.ResourceToId))).mapToObj(AtomicLong::new).toArray(AtomicLong[]::new);
	}

	// open the LevelDB instance
	protected Options createOptions(boolean timeSeries) {
		Options options = new Options();
		options.createIfMissing(true);
		// set compression type
		options.compressionType(CompressionType.SNAPPY);
		// can be increased for write performance
		options.writeBufferSize(options.writeBufferSize() * 4);

		// default is 4096
		options.blockSize(4096 * 2);

		if (timeSeries) {
			// default is 16
			options.blockRestartInterval(64);
			//      options.reverseOrdering(true)
			//      options.timeSeriesMode(true)
		}

		return options;
	}

	@Override
	public boolean addListener(KvinListener listener) {
		return listeners.add(listener);
	}

	@Override
	public boolean removeListener(KvinListener listener) {
		return listeners.remove(listener);
	}

	private long nextId(EntryType entryType) {
		return nextIds[entryType.index()].getAndIncrement();
	}

	protected long[] readNextIds(List<EntryType> entryTypes) {
		long[] result = new long[entryTypes.size()];
		// default id is 1
		Arrays.fill(result, 1);

		DBIterator it = ids.iterator();
		try {
			byte[] idPrefix = new byte[2];
			idPrefix[1] = (byte) 0xFF;
			for (EntryType t : entryTypes) {
				idPrefix[0] = (byte) t.reverse();
				it.seek(idPrefix);
				if (it.hasPrev()) {
					Map.Entry<byte[], byte[]> pref = it.peekPrev();
					if (pref.getKey()[0] == (byte) t.reverse()) {
						result[t.index()] = Varint.readUnsigned(ByteBuffer.wrap(pref.getKey()), 1) + 1L;
					}
				}
			}
		} finally {
			it.close();
		}
		return result;
	}

	public byte[] uriKey(byte prefix, URI uri) {
		byte[] uriBytes = uri.toString().getBytes(StandardCharsets.UTF_8);
		// append 0 after the uri to ensure that it is not a prefix of another string
		byte[] key = new byte[uriBytes.length + 2];
		key[0] = prefix;
		System.arraycopy(uriBytes, 0, key, 1, uriBytes.length);
		return key;
	}

	public byte[] idKey(byte prefix, byte[] id) {
		byte[] key = new byte[id.length + 1];
		key[0] = prefix;
		System.arraycopy(id, 0, key, 1, id.length);
		return key;
	}

	protected byte[] toId(URI uri, EntryType entryType, boolean generate, WriteBatch writeBatch) {
		UriKey cacheKey = new UriKey(uri.toString(), entryType.id());
		byte[] idBytes = uriToIdCache.getIfPresent(cacheKey);
		if (idBytes == null) {
			byte[] key = uriKey((byte) entryType.id(), uri);
			idBytes = ids.get(key);
			if (idBytes == null && generate) {
				ReentrantReadWriteLock lock = lockFor(uri);
				boolean hadReadLock = acquireWriteLock(lock);
				try {
					idBytes = ids.get(key);
					if (idBytes == null) {
						idBytes = uriToIdCacheWrite.getIfPresent(cacheKey);
					}
					if (idBytes == null) {
						long id = nextId(entryType);
						idBytes = new byte[Varint.calcLengthUnsigned(id)];
						Varint.writeUnsigned(idBytes, 0, id);

						WriteBatch batch = writeBatch == null ? ids.createWriteBatch() : writeBatch;

						// add forward mapping
						batch.put(key, idBytes);
						// add reverse mapping
						byte[] idKeyBytes = idKey((byte) entryType.reverse(), idBytes);
						batch.put(idKeyBytes, uri.toString().getBytes(StandardCharsets.UTF_8));

						// Ensure that writes to the id database are always synced to disk.
						// As ids are subject to fewer changes the pages may only
						// be flushed with large delays to disk which may cause data loss.
						if (batch != writeBatch) {
							try {
								ids.write(batch, new WriteOptions().sync(true));
							} finally {
								closeUnchecked(batch);
							}
						} else {
							uriToIdCacheWrite.put(cacheKey, idBytes);
						}

						if (entryType == EntryType.SubjectToId) {
							for (KvinListener l : listeners) {
								l.entityCreated(uri);
							}
						}
					}
				} finally {
					releaseWriteLock(lock, hadReadLock);
				}
			}
			if (idBytes != null) {
				uriToIdCache.put(cacheKey, idBytes);
			}
		}
		return idBytes;
	}

	public URI contextOrDefault(URI context) {
		return context == null ? Kvin.DEFAULT_CONTEXT : context;
	}

	public byte[] toId(URI item, URI property, URI context, boolean generate, WriteBatch writeBatch) {
		ScpKey cacheKey = new ScpKey(item.toString(), contextOrDefault(context).toString(), property.toString());
		byte[] id = scpToIdCache.getIfPresent(cacheKey);
		if (id != null) {
			return id;
		}
		byte[] itemId = toId(item, EntryType.SubjectToId, generate, writeBatch);
		if (itemId == null) {
			return null;
		}
		byte[] contextId = toId(contextOrDefault(context), EntryType.ContextToId, generate, writeBatch);
		if (contextId == null) {
			return null;
		}
		byte[] propertyId = toId(property, EntryType.PropertyToId, generate, writeBatch);
		if (propertyId != null) {
			id = new byte[contextId.length + itemId.length + propertyId.length];

			System.arraycopy(itemId, 0, id, 0, itemId.length);
			System.arraycopy(contextId, 0, id, itemId.length, contextId.length);
			System.arraycopy(propertyId, 0, id, itemId.length + contextId.length, propertyId.length);

			scpToIdCache.put(cacheKey, id);
		}
		return id;
	}

	/**
	 * Returns the URI for the given ID.
	 *
	 * @return the URI or <code>null</code> if the ID is unknown
	 */
	public URI toUri(byte[] id, EntryType entryType) {
		byte[] uriBytes = ids.get(idKey((byte) entryType.reverse(), id));
		return uriBytes == null ? null : URIs.createURI(new String(uriBytes, StandardCharsets.UTF_8));
	}

	@Override
	public boolean delete(URI item, URI context) {
		byte[] contextId = toId(contextOrDefault(context), EntryType.ContextToId, false, null);
		byte[] itemId = toId(item, EntryType.SubjectToId, false, null);
		boolean deletedAny = false;
		if (itemId != null && contextId != null) {
			byte[] prefix = new byte[itemId.length + contextId.length];
			System.arraycopy(itemId, 0, prefix, 0, itemId.length);
			System.arraycopy(contextId, 0, prefix, itemId.length, contextId.length);

			DBIterator it = values.iterator();
			WriteBatch batch = null;
			try {
				long count = 0L;

				it.seek(prefix);
				while (it.hasNext()) {
					Map.Entry<byte[], byte[]> entry = it.next();
					byte[] key = entry.getKey();
					if (!startsWith(key, prefix)) {
						break;
					}
					deletedAny = true;

					if (batch == null) {
						batch = values.createWriteBatch();
					}
					count += 1;
					batch.delete(key);
					if (count % DELETE_BATCH_SIZE == 0) {
						values.write(batch, new WriteOptions().sync(false));
						closeUnchecked(batch);
						batch = values.createWriteBatch();
					}
				}

				if (batch != null && count % DELETE_BATCH_SIZE != 0) {
					values.write(batch, new WriteOptions().sync(false));
				}
			} finally {
				it.close();
				if (batch != null) {
					closeUnchecked(batch);
				}
			}
		}
		return deletedAny;
	}

	@Override
	public long delete(URI item, URI property, URI context, long end, long begin) {
		ReentrantReadWriteLock lock = lockFor(item);
		lock.readLock().lock();
		try {
			byte[] id = toId(item, property, context, false, null);
			if (id == null) {
				return 0L;
			}
			byte[] idTimePrefix = new byte[id.length + Varint.MAX_BYTES];
			ByteBuffer prefixBuffer = ByteBuffer.wrap(idTimePrefix).order(BYTE_ORDER);
			prefixBuffer.put(id);
			writeVarint(prefixBuffer, end);
			WriteBatch batch = null;
			long count = 0L;
			try (DBIterator it = values.iterator()) {
				it.seek(idTimePrefix);
				while (it.hasNext()) {
					Map.Entry<byte[], byte[]> entry = it.next();
					byte[] key = entry.getKey();
					if (!startsWith(key, id)) {
						break;
					}
					ByteBuffer bb = ByteBuffer.wrap(key, id.length, key.length - id.length).order(BYTE_ORDER);
					long time = readVarint(bb);
					if (time > end || time < begin) {
						break;
					}
					if (batch == null) {
						batch = values.createWriteBatch();
					}
					count += 1;
					batch.delete(key);
					if (count % DELETE_BATCH_SIZE == 0) {
						values.write(batch, new WriteOptions().sync(false));
						closeUnchecked(batch);
						batch = values.createWriteBatch();
					}
				}
				if (batch != null && count % DELETE_BATCH_SIZE != 0) {
					values.write(batch, new WriteOptions().sync(false));
				}
			} finally {
				if (batch != null) {
					closeUnchecked(batch);
				}
			}
			return count;
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public IExtendedIterator<URI> descendants(URI uri, URI context) {
		return descendants(uri, context, Long.MAX_VALUE);
	}

	@Override
	public IExtendedIterator<URI> descendants(URI uri, URI context, long limit) {
		return entries(EntryType.SubjectToId, uri, context, limit);
	}

	public IExtendedIterator<URI> entries(EntryType entryType, URI uriPrefix, URI context, long limit) {
		byte[] contextId = toId(contextOrDefault(context), EntryType.ContextToId, false, null);
		if (contextId == null) {
			return NiceIterator.emptyIterator();
		}
		byte[] uriBytes = uriPrefix.toString().getBytes(StandardCharsets.UTF_8);
		byte[] prefix = new byte[uriBytes.length + 1];
		prefix[0] = (byte) entryType.id();
		System.arraycopy(uriBytes, 0, prefix, 1, uriBytes.length);

		byte[] valuesPrefix = new byte[(LONG_BYTES + 1) * 2];

		DBIterator it = ids.iterator();
		it.seek(prefix);

		DBIterator valuesIt = values.iterator();
		return new StoreIterator<URI>(it) {
			final Set<URI> seen = new HashSet<>();
			long count = 0L;

			@Override
			protected URI computeNext() {
				while (it.hasNext()) {
					Map.Entry<byte[], byte[]> entry = it.next();
					byte[] key = entry.getKey();
					if (!startsWith(key, prefix) || count >= limit) {
						break;
					}
					byte[] itemId = entry.getValue();
					System.arraycopy(itemId, 0, valuesPrefix, 0, itemId.length);
					System.arraycopy(contextId, 0, valuesPrefix, itemId.length, contextId.length);
					Arrays.fill(valuesPrefix, itemId.length + contextId.length, valuesPrefix.length, (byte) 0);

					// test if there is an entry with the given context in the values table
					valuesIt.seek(valuesPrefix);
					if (valuesIt.hasNext() &&
							Arrays.compare(valuesIt.next().getKey(), 0, itemId.length + contextId.length,
									valuesPrefix, 0, itemId.length + contextId.length) == 0) {
						// create array of character bytes without prefix and without trailing 0
						byte[] uriBytes = new byte[indexOf(key, (byte) 0) - 1];
						System.arraycopy(key, 1, uriBytes, 0, uriBytes.length);
						URI uri = URIs.createURI(new String(uriBytes, StandardCharsets.UTF_8));
						if (seen.add(uri)) {
							count += 1;
							return uri;
						}
					}
				}
				return null;
			}

			@Override
			public void close() {
				try {
					if (open) {
						valuesIt.close();
					}
				} finally {
					super.close();
				}
			}
		};
	}

	private static int indexOf(byte[] bytes, byte value) {
		for (int i = 0; i < bytes.length; i++) {
			if (bytes[i] == value) {
				return i;
			}
		}
		return -1;
	}

	@Override
	public IExtendedIterator<URI> properties(URI item, URI context) {
		byte[] contextId = toId(contextOrDefault(context), EntryType.ContextToId, false, null);
		byte[] itemId = toId(item, EntryType.SubjectToId, false, null);
		if (contextId == null || itemId == null) {
			return NiceIterator.emptyIterator();
		}
		byte[] itemAndContextId = new byte[contextId.length + itemId.length];
		System.arraycopy(itemId, 0, itemAndContextId, 0, itemId.length);
		System.arraycopy(contextId, 0, itemAndContextId, itemId.length, contextId.length);

		byte[] prefix = new byte[itemId.length + contextId.length + Long.BYTES + 1];
		System.arraycopy(itemId, 0, prefix, 0, itemId.length);
		System.arraycopy(contextId, 0, prefix, itemId.length, contextId.length);

		DBIterator it = values.iterator();
		it.seek(prefix);

		return UniqueExtendedIterator.create(new StoreIterator<URI>(it) {
			@Override
			protected URI computeNext() {
				Map.Entry<byte[], byte[]> entry = it.next();
				byte[] key = entry.getKey();
				if (!startsWith(key, itemAndContextId)) {
					return null;
				}
				int propertyIdLength = varIntLength(key, itemId.length + contextId.length);
				byte[] propertyIdBytes = new byte[propertyIdLength];
				System.arraycopy(key, itemId.length + contextId.length, propertyIdBytes, 0, propertyIdBytes.length);
				long propertyId = Varint.readUnsigned(ByteBuffer.wrap(propertyIdBytes));
				Varint.writeUnsigned(prefix, contextId.length + itemId.length, propertyId + 1);
				// go to next property
				it.seek(prefix);
				return toUri(propertyIdBytes, EntryType.PropertyToId);
			}
		});
	}

	/**
	 * Returns all tuples in this store
	 *
	 * @return iterator with all tuples
	 */
	public IExtendedIterator<KvinTuple> fetchAll() {
		DBIterator it = values.iterator();
		return new StoreIterator<KvinTuple>(it) {
			URI item, property, context;
			long itemId, propertyId, contextId;

			byte[] subArray(byte[] key, int index, int length) {
				byte[] sub = new byte[length];
				System.arraycopy(key, index, sub, 0, length);
				return sub;
			}

			@Override
			protected KvinTuple computeNext() {
				KvinTuple next = null;
				while (next == null && it.hasNext()) {
					Map.Entry<byte[], byte[]> entry = it.next();
					byte[] key = entry.getKey();
					ByteBuffer keyBb = ByteBuffer.wrap(key);

					int itemIdLength = Varint.firstToLength(keyBb.get(keyBb.position()));
					long newItemId = Varint.readUnsigned(keyBb);
					if (newItemId != itemId) {
						item = toUri(subArray(key, 0, itemIdLength), EntryType.SubjectToId);
						itemId = newItemId;
					}
					int contextIdLength = Varint.firstToLength(keyBb.get(keyBb.position()));
					long newContextId = Varint.readUnsigned(keyBb);
					if (newContextId != contextId) {
						context = toUri(subArray(key, itemIdLength, contextIdLength), EntryType.ContextToId);
						contextId = newContextId;
					}
					int propertyIdLength = Varint.firstToLength(keyBb.get(keyBb.position()));
					long newPropertyId = Varint.readUnsigned(keyBb);
					if (newPropertyId != propertyId) {
						property = toUri(subArray(key, itemIdLength + contextIdLength, propertyIdLength),
								EntryType.PropertyToId);
						propertyId = newPropertyId;
					}

					if (item != null && property != null && context != null) {
						long time = readVarint(keyBb);
						int seq = keyBb.hasRemaining() ? (int) readVarint(keyBb) : 0;
						next = new KvinTuple(item, property, context, time, seq, decode(entry.getValue()));
					}
				}
				return next;
			}
		};
	}

	public ReentrantReadWriteLock lockFor(URI uri) {
		return (ReentrantReadWriteLock) locks.get(uri);
	}

	/**
	 * Acquires the write lock. A read lock held by the current thread is released first.
	 *
	 * @return <code>true</code> if a read lock was released and has to be reacquired
	 * by {@link #releaseWriteLock(ReentrantReadWriteLock, boolean)}
	 */
	protected boolean acquireWriteLock(ReentrantReadWriteLock lock) {
		boolean hadReadLock = false;
		if (lock.getReadHoldCount() > 0) {
			hadReadLock = true;
			lock.readLock().unlock();
		}
		lock.writeLock().lock();
		return hadReadLock;
	}

	protected void releaseWriteLock(ReentrantReadWriteLock lock, boolean hadReadLock) {
		lock.writeLock().unlock();
		if (hadReadLock) {
			lock.readLock().lock();
		}
	}

	private byte[] valueKey(byte[] prefix, KvinTuple entry) {
		byte[] key = new byte[prefix.length + Varint.calcLengthUnsigned(entry.time) +
				Varint.calcLengthUnsigned(entry.seqNr)];
		ByteBuffer bb = ByteBuffer.wrap(key).order(BYTE_ORDER);
		bb.put(prefix);
		writeVarint(bb, entry.time);
		writeVarint(bb, entry.seqNr);
		return key;
	}

	private void notifyValueAdded(Iterable<KvinTuple> tuples) {
		for (KvinTuple entry : tuples) {
			for (KvinListener l : listeners) {
				l.valueAdded(entry.item, entry.property, entry.context, entry.time, entry.seqNr, entry.value);
			}
		}
	}

	@Override
	public void put(KvinTuple... entries) {
		if (entries.length > 5) {
			// write in batch
			put(Arrays.asList(entries));
			return;
		}
		// write directly
		for (KvinTuple entry : entries) {
			// encode value first to circumvent problems with locks
			byte[] encodedValue = encode(entry.value);
			ReentrantReadWriteLock lock = lockFor(entry.item);
			lock.readLock().lock();
			try {
				byte[] prefix = toId(entry.item, entry.property, entry.context, true, null);
				values.put(valueKey(prefix, entry), encodedValue);

				// remove timed-out entries
				ttl(entry.item).ifPresent(ttl -> asyncRemoveByTtl(values, prefix, ttl));
			} finally {
				lock.readLock().unlock();
			}
		}
		notifyValueAdded(Arrays.asList(entries));
	}

	@Override
	public void put(Iterable<KvinTuple> entries) {
		List<KvinTuple> notifyTuples = null;
		if (!listeners.isEmpty() && entries instanceof IExtendedIterator<?>) {
			// buffer tuples if entries are given via iterator
			notifyTuples = new ArrayList<>();
		}

		WriteBatch idsBatch = ids.createWriteBatch();
		WriteBatch batch = values.createWriteBatch();
		URI lastItem = null;
		URI lastProperty = null;
		URI lastContext = null;
		byte[] lastPrefix = null;
		ReentrantReadWriteLock lastLock = null;
		activeWrites.incrementAndGet();
		try {
			for (KvinTuple entry : entries) {
				// encode value first to circumvent problems with locks
				byte[] encodedValue = encode(entry.value);
				boolean samePrefix = lastPrefix != null && Objects.equals(entry.item, lastItem) &&
						Objects.equals(entry.property, lastProperty) && Objects.equals(entry.context, lastContext);
				ReentrantReadWriteLock lock = samePrefix ? lastLock : lockFor(entry.item);
				lock.readLock().lock();
				try {
					byte[] prefix;
					if (samePrefix) {
						prefix = lastPrefix;
					} else {
						prefix = toId(entry.item, entry.property, entry.context, true, idsBatch);
						lastItem = entry.item;
						lastProperty = entry.property;
						lastContext = entry.context;
						lastPrefix = prefix;
						lastLock = lock;
					}

					batch.put(valueKey(prefix, entry), encodedValue);
					if (notifyTuples != null) {
						notifyTuples.add(entry);
					}

					boolean flush = batch.size() > MAX_VALUES_BATCH_SIZE;
					if (flush) {
						Future<?> writeIds = writeIdsAsync(idsBatch);

						values.write(batch);
						closeUnchecked(batch);
						batch = values.createWriteBatch();

						if (writeIds != null) {
							await(writeIds);
						}

						// send notifications
						if (notifyTuples != null) {
							notifyValueAdded(notifyTuples);
							notifyTuples.clear();
						}
					}

					// remove timed-out entries
					ttl(entry.item).ifPresent(ttl -> asyncRemoveByTtl(values, prefix, ttl));
				} finally {
					lock.readLock().unlock();
				}
			}

			Future<?> writeIds = writeIdsAsync(idsBatch);
			if (batch.size() > 0) {
				values.write(batch);
			}
			if (writeIds != null) {
				await(writeIds);
			}
		} finally {
			try {
				closeUnchecked(idsBatch);
				closeUnchecked(batch);
			} finally {
				if (activeWrites.decrementAndGet() == 0) {
					uriToIdCacheWrite.invalidateAll();
				}
			}
		}
		notifyValueAdded(notifyTuples != null ? notifyTuples : entries);
	}

	private Future<?> writeIdsAsync(WriteBatch idsBatch) {
		if (idsBatch.size() > 0) {
			return executor.submit(() -> ids.write(idsBatch, new WriteOptions().sync(true)));
		}
		return null;
	}

	private static void await(Future<?> future) {
		try {
			future.get();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		} catch (ExecutionException e) {
			if (e.getCause() instanceof RuntimeException re) {
				throw re;
			} else if (e.getCause() instanceof Error err) {
				throw err;
			}
			throw new RuntimeException(e.getCause());
		}
	}

	@Override
	public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long limit) {
		return fetchInternal(item, property, context, KvinTuple.TIME_MAX_VALUE, 0L, limit, 0L);
	}

	@Override
	public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long end, long begin, long limit,
	                                          long interval, String op) {
		IExtendedIterator<KvinTuple> results = fetchInternal(item, property, context, end, begin,
				op == null ? limit : 0L, op == null ? interval : 0L);
		if (op != null) {
			results = new AggregatingIterator<>(results, interval, op.trim().toLowerCase(), limit) {
				@Override
				protected KvinTuple createElement(URI item, URI property, URI context, long time, int seqNr,
				                                  Object value) {
					return new KvinTuple(item, property, context, time, seqNr, value);
				}
			};
		}
		return results;
	}

	public byte[] encode(Object value) {
		if (value instanceof Data<?> d) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			// marker for an object
			baos.write('O');
			int size = d.size();
			byte[] length = new byte[Varint.calcLengthUnsigned(size)];
			Varint.writeUnsigned(length, 0, size);
			baos.writeBytes(length);
			for (Data<?> element : d) {
				// write the property
				URI p = element.getProperty();
				byte[] pId = toId(p, EntryType.PropertyToId, true, null);
				baos.writeBytes(pId);

				// write the value
				baos.writeBytes(encode(element.getValue()));
			}
			return baos.toByteArray();
		} else if (value != null && value.getClass().isArray()) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			// marker for an array
			baos.write('[');
			int arrayLength = Array.getLength(value);
			byte[] length = new byte[Varint.calcLengthUnsigned(arrayLength)];
			Varint.writeUnsigned(length, 0, arrayLength);
			baos.writeBytes(length);
			for (int i = 0; i < arrayLength; i++) {
				baos.writeBytes(encode(Array.get(value, i)));
			}
			return baos.toByteArray();
		} else if (value instanceof URI ref) {
			byte[] refId = toId(ref, EntryType.ResourceToId, true, null);
			byte[] refData = new byte[1 + refId.length];
			refData[0] = 'R';
			System.arraycopy(refId, 0, refData, 1, refId.length);
			return refData;
		} else {
			return Values.encode(value);
		}
	}

	public Object decode(byte[] data) {
		return decode(ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN));
	}

	public Object decode(ByteBuffer b) {
		int oldPos = b.position();
		switch (b.get()) {
			// this is an object
			case 'O': {
				int length = (int) Varint.readUnsigned(b);
				Record dataObj = Record.NULL;
				for (int i = 0; i < length; i++) {
					byte[] pId = new byte[varIntLength(b)];
					b.get(pId);
					URI pUri = toUri(pId, EntryType.PropertyToId);
					Object value = decode(b);
					if (pUri != null) {
						dataObj = dataObj.append(new Record(pUri, value));
					}
				}
				return dataObj;
			}
			// an array
			case '[': {
				int length = (int) Varint.readUnsigned(b);
				Object[] values = new Object[length];
				for (int i = 0; i < length; i++) {
					values[i] = decode(b);
				}
				return values;
			}
			// a URI reference
			case 'R': {
				byte[] refId = new byte[varIntLength(b)];
				b.get(refId);
				URI uri = toUri(refId, EntryType.ResourceToId);
				return uri != null ? uri : URIs.createURI("urn:invalid:" + Varint.readUnsigned(ByteBuffer.wrap(refId)));
			}
			// a scalar value
			default:
				b.position(oldPos);
				try {
					return Values.decode(b);
				} catch (IOException e) {
					throw new UncheckedIOException(e);
				}
		}
	}

	public int varIntLength(byte[] bytes, int pos) {
		return Varint.firstToLength(bytes[pos]);
	}

	public int varIntLength(ByteBuffer bb) {
		return Varint.firstToLength(bb.get(bb.position()));
	}

	public IExtendedIterator<KvinTuple> fetchInternal(URI item, URI property, URI context, long end, long begin,
	                                                  long limit, long interval) {
		URI currentContext = contextOrDefault(context);
		IExtendedIterator<URI> propertiesIt = property == null ?
				properties(item, context) : WrappedIterator.create(List.of(property).iterator());

		if (!propertiesIt.hasNext()) {
			return NiceIterator.emptyIterator();
		}
		DBIterator it = values.iterator();
		return new StoreIterator<KvinTuple>(it) {
			final byte[] idTimePrefix = new byte[Varint.MAX_BYTES * 4];
			final ByteBuffer prefixBuffer = ByteBuffer.wrap(idTimePrefix).order(BYTE_ORDER);
			URI currentProperty;
			byte[] id;
			int intervalSeq = 0;
			long count = 0;

			{
				// initialize first property
				nextProperty();
			}

			void nextProperty() {
				boolean validProperty = false;
				while (!validProperty && propertiesIt.hasNext()) {
					currentProperty = propertiesIt.next();
					id = toId(item, currentProperty, context, false, null);
					if (id != null) {
						validProperty = true;
						Arrays.fill(idTimePrefix, (byte) 0);
						prefixBuffer.clear();
						prefixBuffer.put(id);
						writeVarint(prefixBuffer, end);
					}
				}
				if (validProperty) {
					count = 0;
					it.seek(idTimePrefix);
				} else {
					close();
				}
			}

			@Override
			protected KvinTuple computeNext() {
				Map.Entry<byte[], byte[]> entry = it.next();
				byte[] key = entry.getKey();
				if (startsWith(key, id)) {
					ByteBuffer bb = ByteBuffer.wrap(key, id.length, key.length - id.length).order(BYTE_ORDER);
					long time = readVarint(bb);
					int seq = bb.hasRemaining() ? (int) readVarint(bb) : 0;
					if (time <= end && time >= begin && (limit == 0 || count < limit)) {
						count += 1;

						// skips time intervals if requested, the upper value is exclusive, the lower value is inclusive
						if (interval > 0) {
							long intervalStart = time - (time % interval);
							// seek to next interval
							Arrays.fill(idTimePrefix, id.length, idTimePrefix.length, (byte) 0);
							writeVarint(prefixBuffer, id.length, intervalStart - 1);
							it.seek(idTimePrefix);

							intervalSeq += 1;
							return new KvinTuple(item, currentProperty, currentContext, intervalStart,
									intervalSeq, decode(entry.getValue()));
						} else {
							return new KvinTuple(item, currentProperty, currentContext, time, seq,
									decode(entry.getValue()));
						}
					}
				}
				nextProperty();
				return open ? computeNext() : null;
			}

			@Override
			public void close() {
				propertiesIt.close();
				super.close();
			}
		};
	}

	@Override
	public void close() {
		IOException error = null;
		try {
			ids.close();
		} catch (IOException e) {
			error = e;
		}
		try {
			values.close();
		} catch (IOException e) {
			if (error != null) {
				e.addSuppressed(error);
			}
			error = e;
		}
		executor.shutdown();
		if (error != null) {
			throw new UncheckedIOException(error);
		}
	}
}
