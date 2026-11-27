package io.github.linkedfactory.core.kvin.iceberg;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileSystem;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Keeps data files open between fetches, so that reading pages that are not cached does not
 * open the file again.
 *
 * <p>Handles read by position and keep no position of their own, so several threads can read
 * through the same handle at once. A handle is reference counted: the cache holds one
 * reference, and each {@link #acquire(String)} another one until the returned handle is
 * closed. A handle that is evicted or {@link #invalidate(String) invalidated} is closed when
 * its last reader closes it.</p>
 */
final class DataFileHandles implements Closeable {
	static final int DEFAULT_MAX_OPEN_FILES = 256;

	/**
	 * A range of a file to read into an array.
	 */
	record Range(long offset, byte[] target) {
	}

	private final Configuration conf;
	private final Cache<String, Handle> handles;

	DataFileHandles(Configuration conf, int maxOpenFiles) {
		this.conf = conf;
		this.handles = CacheBuilder.newBuilder().maximumSize(maxOpenFiles)
				.removalListener((RemovalListener<String, Handle>) removal -> removal.getValue().release())
				.build();
	}

	/**
	 * Returns an open handle of a file. The caller must close it after reading.
	 */
	Handle acquire(String location) throws IOException {
		while (true) {
			Handle handle;
			try {
				handle = handles.get(location, () -> open(location));
			} catch (ExecutionException e) {
				throw e.getCause() instanceof IOException io ? io : new IOException(e.getCause());
			} catch (com.google.common.util.concurrent.UncheckedExecutionException e) {
				throw e.getCause() instanceof RuntimeException runtime ? runtime : e;
			}
			if (handle.retain()) {
				return handle;
			}
			// closed after the cache dropped it; open a new one
			handles.asMap().remove(location, handle);
		}
	}

	/**
	 * Closes the handle of a file once no reader uses it, for example after it was deleted.
	 */
	void invalidate(String location) {
		handles.invalidate(location);
	}

	int openFiles() {
		return (int) handles.size();
	}

	@Override
	public void close() {
		handles.invalidateAll();
	}

	private Handle open(String location) throws IOException {
		Path local = ArchiveLocation.localPath(location);
		if (local != null) {
			return new LocalHandle(local);
		}
		org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(location);
		FileSystem fs = path.getFileSystem(conf);
		return new RemoteHandle(fs.open(path));
	}

	abstract static class Handle implements Closeable {
		// one reference of the cache, plus one per reader
		private final AtomicInteger references = new AtomicInteger(1);

		private boolean retain() {
			while (true) {
				int current = references.get();
				if (current == 0) {
					return false;
				}
				if (references.compareAndSet(current, current + 1)) {
					return true;
				}
			}
		}

		private void release() {
			if (references.decrementAndGet() == 0) {
				try {
					closeFile();
				} catch (IOException e) {
					throw new UncheckedIOException(e);
				}
			}
		}

		/**
		 * Returns the reference of a reader.
		 */
		@Override
		public void close() {
			release();
		}

		/**
		 * Fills each target array with the bytes of the file at its offset.
		 */
		abstract void read(List<Range> ranges) throws IOException;

		abstract void closeFile() throws IOException;
	}

	private static final class LocalHandle extends Handle {
		private final Path path;
		private volatile FileChannel channel;
		// set once the last reference was returned; a closed handle is never reopened
		private volatile boolean released;

		LocalHandle(Path path) throws IOException {
			this.path = path;
			this.channel = FileChannel.open(path, StandardOpenOption.READ);
		}

		@Override
		void read(List<Range> ranges) throws IOException {
			for (Range range : ranges) {
				read(range.offset(), range.target());
			}
		}

		private void read(long offset, byte[] target) throws IOException {
			FileChannel current = channel;
			try {
				readFully(current, offset, target);
			} catch (ClosedByInterruptException e) {
				// this thread was interrupted; the next reader reopens the channel
				throw e;
			} catch (ClosedChannelException e) {
				// The interrupt of another reading thread closes the channel for all threads.
				readFully(reopen(current), offset, target);
			}
		}

		private synchronized FileChannel reopen(FileChannel closed) throws IOException {
			if (released) {
				throw new ClosedChannelException();
			}
			if (channel == closed) {
				channel = FileChannel.open(path, StandardOpenOption.READ);
			}
			return channel;
		}

		private static void readFully(FileChannel channel, long offset, byte[] target) throws IOException {
			ByteBuffer buffer = ByteBuffer.wrap(target);
			while (buffer.hasRemaining()) {
				int read = channel.read(buffer, offset + buffer.position());
				if (read < 0) {
					throw new EOFException("Unexpected end of file at " + (offset + buffer.position()));
				}
			}
		}

		@Override
		synchronized void closeFile() throws IOException {
			released = true;
			channel.close();
		}
	}

	private static final class RemoteHandle extends Handle {
		private final FSDataInputStream stream;

		RemoteHandle(FSDataInputStream stream) {
			this.stream = stream;
		}

		@Override
		void read(List<Range> ranges) throws IOException {
			if (ranges.size() == 1) {
				Range range = ranges.getFirst();
				stream.readFully(range.offset(), range.target());
				return;
			}
			// requests the ranges in parallel
			List<FileRange> fileRanges = new ArrayList<>(ranges.size());
			for (Range range : ranges) {
				fileRanges.add(FileRange.createFileRange(range.offset(), range.target().length));
			}
			stream.readVectored(fileRanges, ByteBuffer::allocate);
			try {
				for (int i = 0; i < ranges.size(); i++) {
					CompletableFuture<ByteBuffer> data = fileRanges.get(i).getData();
					data.get().get(ranges.get(i).target());
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new java.io.InterruptedIOException("Interrupted while reading " + stream);
			} catch (ExecutionException e) {
				throw e.getCause() instanceof IOException io ? io : new IOException(e.getCause());
			}
		}

		@Override
		void closeFile() throws IOException {
			stream.close();
		}
	}
}
