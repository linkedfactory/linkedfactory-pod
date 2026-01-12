package io.github.linkedfactory.core.rdf4j.io;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.BufferedOutputStream;
import java.io.IOException;

class OutputFileOutputStream implements OutputFile {
	private final BufferedOutputStream out;

	public OutputFileOutputStream(BufferedOutputStream out) {
		this.out = out;
	}

	@Override
	public PositionOutputStream create(long blockSizeHint) throws IOException {
		return createPositionOutputstream();
	}

	private PositionOutputStream createPositionOutputstream() {
		return new PositionOutputStream() {
			int pos = 0;

			@Override
			public long getPos() {
				return pos;
			}

			@Override
			public void flush() throws IOException {
				out.flush();
			}

			@Override
			public void close() throws IOException {
				out.close();
			}

			@Override
			public void write(int b) throws IOException {
				out.write(b);
				pos++;
			}

			@Override
			public void write(byte[] b, int off, int len) throws IOException {
				out.write(b, off, len);
				pos += len;
			}
		};
	}

	@Override
	public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
		return createPositionOutputstream();
	}

	@Override
	public boolean supportsBlockSize() {
		return false;
	}

	@Override
	public long defaultBlockSize() {
		return 0;
	}
}