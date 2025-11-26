package io.github.linkedfactory.core.kvin.parquet;

import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;

public class MemoryMappedSeekableInputStream extends SeekableInputStream {
	static final int BUFFER_SIZE = 500 * 1024 * 1024;
	final MappedByteBuffer[] buffers;
	final FileChannel fileChannel;
	long size;
	long pos;
	MappedByteBuffer buffer;
	long bufferStart;
	long bufferEnd;

	public MemoryMappedSeekableInputStream(Path file) throws IOException {
		this.fileChannel = (FileChannel) Files.newByteChannel(file, EnumSet.of(StandardOpenOption.READ));
		this.size = fileChannel.size();
		this.buffers = new MappedByteBuffer[(int) (this.fileChannel.size() / BUFFER_SIZE) + 1];
	}

	void ensureBuffer() throws IOException {
		if (buffer != null && pos < bufferEnd && pos >= bufferEnd) {
			return;
		}
		int bufferIndex = (int) (pos / BUFFER_SIZE);
		buffer = buffers[bufferIndex];
		int bufferSize = (int) Math.min(BUFFER_SIZE, size - (long) bufferIndex * BUFFER_SIZE);
		bufferStart = (long) bufferIndex * BUFFER_SIZE;
		bufferEnd = bufferStart + bufferSize;
		if (buffer == null) {
			buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, (long) bufferIndex * BUFFER_SIZE, bufferSize);
			buffers[bufferIndex] = buffer;
		}
	}

	@Override
	public long getPos() throws IOException {
		return pos;
	}

	@Override
	public void seek(long newPos) throws IOException {
		pos = newPos;
		ensureBuffer();
		buffer.position((int)(pos % BUFFER_SIZE));
	}

	@Override
	public void readFully(byte[] bytes) throws IOException {
		readFully(ByteBuffer.wrap(bytes));
	}

	@Override
	public void readFully(byte[] bytes, int start, int len) throws IOException {
		readFully(ByteBuffer.wrap(bytes, start, len));
	}

	@Override
	public int read(ByteBuffer bb) throws IOException {
		int readBytes = 0;
		while (bb.remaining() > 0 && pos < size) {
			ensureBuffer();
			int maxReadable = Math.min((int) (bufferEnd - pos), bb.remaining());
			bb.put(bb.position(), buffer, (int) (pos % BUFFER_SIZE), maxReadable);
			bb.position(bb.position() + maxReadable);
			buffer.position(buffer.position() + maxReadable);
			readBytes += maxReadable;
			pos += maxReadable;
		}
		return readBytes;
	}

	@Override
	public void readFully(ByteBuffer bb) throws IOException {
		read(bb);
	}

	@Override
	public int read() throws IOException {
		ensureBuffer();
		pos++;
		return buffer.get() & 255;
	}

	@Override
	public void close() throws IOException {
		fileChannel.close();
		super.close();
	}
}
