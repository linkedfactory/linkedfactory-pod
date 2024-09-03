package io.github.linkedfactory.core.kvin.parquet;

import io.github.linkedfactory.core.kvin.KvinException;
import io.github.linkedfactory.core.kvin.records.KvinRecord;
import net.enilink.commons.iterator.IExtendedIterator;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ParquetHelpersTest {
	static final int RECORDS = 1000;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	File writeDataFile() throws IOException {
		File file = new File(tempFolder.getRoot(), "data__1.parquet");
		try (ParquetWriter<KvinRecord> writer = ParquetHelpers.getKvinRecordWriter(new Path(file.getAbsolutePath()))) {
			for (int i = 0; i < RECORDS; i++) {
				writer.write(new KvinRecord(1, 1, 1, RECORDS - i, 0, "value-" + i));
			}
		}
		return file;
	}

	/**
	 * Overwrites the data pages of the file but keeps its footer intact. The
	 * Hadoop checksum file is removed so that the error occurs while reading.
	 */
	static void corruptData(File file) throws IOException {
		Files.deleteIfExists(new File(file.getParentFile(), "." + file.getName() + ".crc").toPath());
		try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
			byte[] tail = new byte[8];
			raf.seek(raf.length() - tail.length);
			raf.readFully(tail);
			int footerLength = ByteBuffer.wrap(tail, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
			long dataEnd = raf.length() - tail.length - footerLength;
			// skip the magic bytes at the start of the file
			raf.seek(4);
			for (long pos = 4; pos < dataEnd; pos++) {
				raf.write(0xFF);
			}
		}
	}

	@Test
	public void testReadAll() throws IOException {
		File file = writeDataFile();
		IExtendedIterator<KvinRecord> reader = ParquetHelpers.createKvinRecordReader(
				new Path(file.getAbsolutePath()), null);
		try {
			assertEquals(RECORDS, reader.toList().size());
		} finally {
			reader.close();
		}
	}

	@Test
	public void testReadErrorIsThrown() throws IOException {
		File file = writeDataFile();
		corruptData(file);

		IExtendedIterator<KvinRecord> reader = ParquetHelpers.createKvinRecordReader(
				new Path(file.getAbsolutePath()), null);
		KvinException e = assertThrows(KvinException.class, reader::hasNext);
		assertTrue(e.getMessage().contains(file.getName()));
		// the reader is already closed and can be closed again
		reader.close();
	}
}
