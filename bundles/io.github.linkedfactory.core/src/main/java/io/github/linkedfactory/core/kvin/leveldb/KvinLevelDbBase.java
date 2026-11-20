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

import io.github.linkedfactory.core.kvin.util.Varint;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.komma.core.URI;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Common base functions for LevelDB based value stores.
 */
public abstract class KvinLevelDbBase {
	protected static final int LONG_BYTES = Long.BYTES;
	protected static final ByteOrder BYTE_ORDER = ByteOrder.BIG_ENDIAN;

	protected static final Pattern TTL = Pattern.compile("(?:^|&)ttl=([0-9]+)(ns|us|ms|s|m|d)(?=&|$)");

	protected final Set<ByteBuffer> waitingForTTL = Collections.newSetFromMap(new ConcurrentHashMap<>());
	protected final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

	/**
	 * Iterator over a LevelDB iterator that computes its elements lazily and
	 * closes the underlying iterator when it is exhausted.
	 *
	 * @param <T> the element type
	 */
	protected abstract static class StoreIterator<T> extends NiceIterator<T> {
		protected final DBIterator base;
		protected boolean open = true;
		protected T current;

		protected StoreIterator(DBIterator base) {
			this.base = base;
		}

		@Override
		public boolean hasNext() {
			if (current != null) {
				return true;
			} else if (open && base.hasNext()) {
				current = computeNext();
				if (current != null) {
					return true;
				}
				close();
				return false;
			} else {
				if (open) {
					close();
				}
				return false;
			}
		}

		@Override
		public T next() {
			if (hasNext()) {
				T result = current;
				current = null;
				return result;
			}
			throw new NoSuchElementException();
		}

		/**
		 * Computes the next element.
		 *
		 * @return the next element or <code>null</code> if there are no more elements
		 */
		protected abstract T computeNext();

		@Override
		public void close() {
			if (open) {
				try {
					base.close();
				} finally {
					open = false;
				}
			}
		}
	}

	protected static boolean startsWith(byte[] bytes, byte[] prefix) {
		return bytes.length >= prefix.length &&
				Arrays.equals(bytes, 0, prefix.length, prefix, 0, prefix.length);
	}

	protected static void closeUnchecked(Closeable closeable) {
		try {
			closeable.close();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	protected void putLong(DB db, byte[] key, long value) {
		ByteBuffer bb = ByteBuffer.wrap(new byte[LONG_BYTES]).order(BYTE_ORDER);
		bb.putLong(value);
		db.put(key, bb.array());
	}

	protected long readLong(DB db, byte[] key) {
		byte[] longBytes = db.get(key);
		if (longBytes == null) {
			return -1L;
		}
		return ByteBuffer.wrap(longBytes).order(BYTE_ORDER).getLong();
	}

	// TTL support

	/**
	 * Returns the time to live in milliseconds that is encoded in the query part
	 * of the given URI, e.g. <code>?ttl=5s</code> or <code>?a=b&amp;ttl=2d</code>.
	 *
	 * @param uri the URI
	 * @return the time to live or an empty value if the URI has no TTL
	 */
	protected OptionalLong ttl(URI uri) {
		String query = uri.query();
		if (query == null) {
			return OptionalLong.empty();
		}
		Matcher matcher = TTL.matcher(query);
		if (!matcher.find()) {
			return OptionalLong.empty();
		}
		long d = Long.parseLong(matcher.group(1));
		return OptionalLong.of(switch (matcher.group(2)) {
			case "ns" -> d / 1000000L;
			case "us" -> d / 1000L;
			case "ms" -> d;
			case "s" -> d * 1000L;
			case "m" -> d * 60000L;
			case "d" -> d * 86400000L;
			default -> throw new IllegalStateException("Unknown time unit: " + matcher.group(2));
		});
	}

	protected void asyncRemoveByTtl(DB db, byte[] prefix, long ttl) {
		ByteBuffer key = ByteBuffer.wrap(prefix);
		if (waitingForTTL.add(key)) {
			executor.submit(() -> {
				removeByTtl(db, prefix, ttl);
				waitingForTTL.remove(key);
			});
		}
	}

	protected void removeByTtl(DB db, byte[] prefix, long ttl) {
		long now = System.currentTimeMillis();
		long eldest = now - ttl;
		byte[] idTimePrefix = new byte[prefix.length + Varint.MAX_BYTES];
		ByteBuffer bb = ByteBuffer.wrap(idTimePrefix).order(ByteOrder.BIG_ENDIAN);
		bb.put(prefix);
		writeVarint(bb, eldest);
		DBIterator it = db.iterator();
		try {
			it.seek(idTimePrefix);
			while (it.hasNext()) {
				Map.Entry<byte[], byte[]> entry = it.next();
				byte[] key = entry.getKey();
				if (!startsWith(key, prefix)) {
					break;
				}
				db.delete(key);
			}
		} finally {
			it.close();
		}
	}

	public void writeVarint(ByteBuffer byteBuffer, long value) {
		Varint.writeUnsignedInverted(byteBuffer, value);
	}

	public void writeVarint(ByteBuffer byteBuffer, int pos, long value) {
		Varint.writeUnsignedInverted(byteBuffer, pos, value);
	}

	public long readVarint(ByteBuffer byteBuffer) {
		return Varint.readUnsignedInverted(byteBuffer);
	}
}
