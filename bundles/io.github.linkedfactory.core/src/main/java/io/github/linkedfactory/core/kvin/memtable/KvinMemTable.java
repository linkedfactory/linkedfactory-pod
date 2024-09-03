package io.github.linkedfactory.core.kvin.memtable;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.Striped;
import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinListener;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.Record;
import io.github.linkedfactory.core.kvin.records.KvinRecord;
import io.github.linkedfactory.core.kvin.util.Values;
import io.github.linkedfactory.core.kvin.util.Varint;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URI;
import org.eclipse.rdf4j.sail.lmdb.config.LmdbStoreConfig;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.impl.Iq80DBFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

public class KvinMemTable implements Kvin {
	enum EntryType {
		SubjectToId(0, 1),
		PropertyToId(1, 3),
		ResourceToId(2, 5),
		ContextToId(3, 7);

		private final int index;
		private final int id;

		EntryType(int index, int id) {
			this.index = index;
			this.id = id;
		}

		public int getIndex() {
			return index;
		}

		public int getId() {
			return id;
		}

		public int getReverse() {
			return id + 1;
		}
	}

	final Striped<ReadWriteLock> locks = Striped.readWriteLock(64);

	final AtomicLong[] nextIds;

	record CacheKey(String uri, int entryTypeId) {
	}

	private final Cache<CacheKey, Long> uriToIdCacheWrite =
			CacheBuilder.newBuilder().build();

	private final Cache<CacheKey, Long> uriToIdCache =
			CacheBuilder.newBuilder().maximumSize(20_000).build();

	final KvinRecord END_MARKER = new KvinRecord();
	final Path path;
	TableFile currentTableFile;
	final ExecutorService executor = Executors.newSingleThreadExecutor();
	int tableFileNr = 0;
	boolean async = true;
	final DB ids;
	final CircularBuffer<KvinRecord> queue = new CircularBuffer<>(10000);

	KvinMemTable(Path path) throws IOException {
		this.path = path;
		Path valueStorePath = path.resolve("ids.dat");
		var config = new LmdbStoreConfig();
		config.setValueIDCacheSize(10000);
		this.ids = Iq80DBFactory.factory.open(new File(valueStorePath.toFile(), "ids"),
				new org.iq80.leveldb.Options()
						.createIfMissing(true)
						.compressionType(CompressionType.SNAPPY));
		this.nextIds = Arrays.stream(readNextIds(List.of(EntryType.values())))
				.mapToObj(AtomicLong::new).toArray(AtomicLong[]::new);
	}

	private long nextId(EntryType entryType) {
		return nextIds[entryType.getIndex()].getAndIncrement();
	}

	private long[] readNextIds(List<EntryType> entryTypes) {
		long[] result = new long[entryTypes.size()];
		Arrays.fill(result, 1L); // default id is 1

		try (var it = ids.iterator()) {
			byte[] idPrefix = new byte[2];
			idPrefix[1] = (byte) 0xFF;

			for (EntryType t : entryTypes) {
				idPrefix[0] = (byte) t.getReverse();
				it.seek(idPrefix);
				if (it.hasPrev()) {
					var prev = it.peekPrev();
					if (prev.getKey()[0] == (byte) t.getReverse()) {
						result[t.getIndex()] = Varint.readUnsigned(ByteBuffer.wrap(prev.getKey()), 1) + 1L;
					}
				}
			}
		}

		return result;
	}

	private ReentrantReadWriteLock lockFor(URI uri) {
		return (ReentrantReadWriteLock) locks.get(uri);
	}

	private <T> T writeLock(ReentrantReadWriteLock lock, Supplier<T> block) {
		boolean readLock = false;
		try {
			if (lock.getReadHoldCount() > 0) {
				readLock = true;
				lock.readLock().unlock();
			}
			lock.writeLock().lock();
			return block.get();
		} finally {
			lock.writeLock().unlock();
			if (readLock) lock.readLock().lock();
		}
	}

	private void writeLock(ReentrantReadWriteLock lock, Runnable block) {
		writeLock(lock, () -> {
			block.run();
			return null;
		});
	}

	private byte[] uriKey(byte prefix, URI uri) {
		byte[] uriBytes = uri.toString().getBytes(StandardCharsets.UTF_8);
		// append 0 after the uri to ensure that it is not a prefix of another string
		byte[] key = new byte[uriBytes.length + 2];
		key[0] = prefix;
		System.arraycopy(uriBytes, 0, key, 1, uriBytes.length);
		return key;
	}

	private byte[] idKey(byte prefix, byte[] id) {
		byte[] key = new byte[id.length + 1];
		key[0] = prefix;
		System.arraycopy(id, 0, key, 1, id.length);
		return key;
	}

	long toId(URI uri, EntryType entryType, boolean generate, WriteBatch writeBatch) {
		var cacheKey = new CacheKey(uri.toString(), entryType.getId());
		Long id = uriToIdCache.getIfPresent(cacheKey);
		if (id == null) {
			byte[] key = uriKey((byte) entryType.getId(), uri);
			byte[] idBytes = ids.get(key);
			if (idBytes == null && generate) {
				ReentrantReadWriteLock lock = lockFor(uri);
				id = writeLock(lock, () -> {
					Long cachedId = uriToIdCacheWrite.getIfPresent(cacheKey);
					if (cachedId != null) {
						return cachedId;
					}

					byte[] idBytesInner = ids.get(key);
					if (idBytesInner == null) {
						long newId = nextId(entryType);
						idBytesInner = new byte[Varint.calcLengthUnsigned(newId)];
						Varint.writeUnsigned(idBytesInner, 0, newId);

						WriteBatch batch = (writeBatch == null) ? ids.createWriteBatch() : writeBatch;

						// add forward mapping
						batch.put(key, idBytesInner);
						// add reverse mapping
						byte[] idKeyBytes = idKey((byte) entryType.getReverse(), idBytesInner);
						batch.put(idKeyBytes, uri.toString().getBytes(StandardCharsets.UTF_8));

						// Ensure that writes to the id database are always synced to disk.
						// As ids are subject to fewer changes the pages may only
						// be flushed with large delays to disk which may cause data loss.
						if (batch != writeBatch) {
							try {
								ids.write(batch, new WriteOptions().sync(true));
							} finally {
								try {
									batch.close();
								} catch (IOException e) {
									throw new UncheckedIOException(e);
								}
							}
						} else {
							uriToIdCacheWrite.put(cacheKey, newId);
						}

						//if (entryType == EntryType.SubjectToId) {
						//  for (var listener : listeners) {
						// 	listener.entityCreated(uri);
						// }
						// }
						return newId;
					}
					Long idInner = Varint.readUnsigned(ByteBuffer.wrap(idBytesInner));
					uriToIdCacheWrite.put(cacheKey, idInner);
					return idInner;
				});
				return id;
			}

			if (idBytes != null) {
				id = Varint.readUnsigned(ByteBuffer.wrap(idBytes));
				uriToIdCache.put(cacheKey, id);
				return id;
			}
		}
		return id != null ? id : -1;
	}

	void nextTableFile() throws IOException {
		if (this.currentTableFile == null || this.currentTableFile.size() > 20 * 1024 * 1024) {
			if (this.currentTableFile != null) {
				this.currentTableFile.close();
			}
			Path tableFilePath = path.resolve("entries_" + (tableFileNr++) + ".dat");
			this.currentTableFile = new TableFile(tableFilePath);
		}
	}

	public static byte[] encodeObject(Object valueObject) throws IOException {
		if (valueObject instanceof Record) {
			Record r = (Record) valueObject;
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			// marker for an object
			baos.write('O');
			int size = r.size();
			byte[] sizeBytes = new byte[Varint.calcLengthUnsigned(size)];
			Varint.writeUnsigned(sizeBytes, 0, size);
			baos.write(sizeBytes);
			for (Record element : r) {
				// write the property
				URI p = element.getProperty();
				byte[] content = p.toString().getBytes(StandardCharsets.UTF_8);
				int lengthBytes = Varint.calcLengthUnsigned(content.length);
				byte[] uriBytes = new byte[lengthBytes + content.length];
				Varint.writeUnsigned(uriBytes, 0, content.length);
				System.arraycopy(content, 0, uriBytes, 1, content.length);
				baos.write(uriBytes);

				// write the value
				baos.write(encodeObject(element.getValue()));
			}
			return baos.toByteArray();
		} else if (valueObject instanceof Object[]) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			baos.write('[');
			byte[] length = new byte[Varint.calcLengthUnsigned(((Object[]) valueObject).length)];
			Varint.writeUnsigned(length, 0, ((Object[]) valueObject).length);
			baos.write(length);
			for (Object v : (Object[]) valueObject) {
				baos.write(encodeObject(v));
			}
			return baos.toByteArray();
		} else if (valueObject instanceof URI) {
			URI uri = (URI) valueObject;
			byte[] content = uri.toString().getBytes(StandardCharsets.UTF_8);
			int lengthBytes = Varint.calcLengthUnsigned(content.length);
			byte[] uriBytes = new byte[1 + lengthBytes + content.length];
			uriBytes[0] = 'R';
			Varint.writeUnsigned(uriBytes, 1, content.length);
			System.arraycopy(content, 0, uriBytes, 1 + lengthBytes, content.length);
			return uriBytes;
		} else {
			return Values.encode(valueObject);
		}
	}

	@Override
	public boolean addListener(KvinListener listener) {
		return false;
	}

	@Override
	public boolean removeListener(KvinListener listener) {
		return false;
	}

	@Override
	public void put(KvinTuple... tuples) {
		put(Arrays.asList(tuples));
	}

	@Override
	public void put(Iterable<KvinTuple> tuples) {
		var future = async ? executor.submit(() -> {
			while (true) {
				KvinRecord data = queue.remove();
				if (data == null) {
					Thread.onSpinWait();
				} else if (data == END_MARKER) {
					break;
				} else {
					nextTableFile();
					currentTableFile.put(data);
				}
			}
			return null;
		}) : null;
		try {
			var writeBatch = ids.createWriteBatch();
			for (KvinTuple tuple : tuples) {
				KvinRecord record = new KvinRecord(
					toId(tuple.item, EntryType.SubjectToId, true, writeBatch),
					toId(tuple.context, EntryType.ContextToId, true, writeBatch),
					toId(tuple.property, EntryType.PropertyToId, true, writeBatch),
					tuple.time,
					tuple.seqNr,
					encodeObject(tuple.value)
				);

				if (async) {
					while (!queue.add(record)) {
						Thread.onSpinWait();
					}
				} else {
					nextTableFile();
					currentTableFile.put(record);
				}

				if (writeBatch.size() >= 10000) {
					ids.write(writeBatch, new WriteOptions().sync(true));
					writeBatch.close();
					writeBatch = ids.createWriteBatch();
				}
			}

			if (writeBatch.size() > 0) {
				ids.write(writeBatch, new WriteOptions().sync(true));
			}
			writeBatch.close();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		} finally {
			if (async) {
				while (!queue.add(END_MARKER)) {
					Thread.onSpinWait();
				}
			}
		}
		if (async) {
			try {
				future.get();
			} catch (InterruptedException e) {
				// ignore
			} catch (ExecutionException e) {
				// ignore
			}
		}
	}

	@Override
	public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long limit) {
		return null;
	}

	@Override
	public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long end, long begin, long limit, long interval, String op) {
		return null;
	}

	@Override
	public long delete(URI item, URI property, URI context, long end, long begin) {
		return 0;
	}

	@Override
	public boolean delete(URI item, URI context) {
		return false;
	}

	@Override
	public IExtendedIterator<URI> descendants(URI item, URI context) {
		return null;
	}

	@Override
	public IExtendedIterator<URI> descendants(URI item, URI context, long limit) {
		return null;
	}

	@Override
	public IExtendedIterator<URI> properties(URI item, URI context) {
		return null;
	}

	@Override
	public void close() {
		try {
			currentTableFile.close();
			ids.close();
			executor.shutdown();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
