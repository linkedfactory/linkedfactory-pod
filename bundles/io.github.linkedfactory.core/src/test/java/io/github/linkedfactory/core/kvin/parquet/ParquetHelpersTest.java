package io.github.linkedfactory.core.kvin.parquet;

import io.github.linkedfactory.core.kvin.KvinException;
import io.github.linkedfactory.core.kvin.parquet.records.KvinParquetWriter;
import io.github.linkedfactory.core.kvin.records.KvinRecord;
import net.enilink.commons.iterator.IExtendedIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.codec.ZstandardCodec;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
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
import java.util.List;

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

	static List<CompressionCodecName> codecs(File file) throws IOException {
		try (ParquetFileReader reader = ParquetFileReader.open(
				HadoopInputFile.fromPath(new Path(file.getAbsolutePath()), new Configuration()))) {
			return reader.getRowGroups().stream().flatMap(block -> block.getColumns().stream())
					.map(ColumnChunkMetaData::getCodec).distinct().toList();
		}
	}

	@Test
	public void testWritersUseZstd() throws IOException {
		assertEquals(List.of(CompressionCodecName.ZSTD), codecs(writeDataFile()));

		File mappingFile = new File(tempFolder.getRoot(), "items__1.parquet");
		try (ParquetWriter<Object> writer = ParquetHelpers.getParquetMappingWriter(new Path(mappingFile.getAbsolutePath()))) {
			SimpleMapping mapping = new SimpleMapping();
			mapping.setId(1);
			mapping.setValue("urn:test:item");
			writer.write(mapping);
		}
		assertEquals(List.of(CompressionCodecName.ZSTD), codecs(mappingFile));
		// Parquet reads the level from this key, it is not stored in the file
		assertEquals(ParquetHelpers.ZSTD_COMPRESSION_LEVEL,
				ParquetHelpers.configuration.getInt(ZstandardCodec.PARQUET_COMPRESS_ZSTD_LEVEL, -1));
	}

	@Test
	public void testSnappyFilesRemainReadable() throws IOException {
		File file = new File(tempFolder.getRoot(), "data__1.parquet");
		try (ParquetWriter<KvinRecord> writer = KvinParquetWriter.builder(
						HadoopOutputFile.fromPath(new Path(file.getAbsolutePath()), ParquetHelpers.configuration))
				.withCompressionCodec(CompressionCodecName.SNAPPY).build()) {
			for (int i = 0; i < RECORDS; i++) {
				writer.write(new KvinRecord(1, 1, 1, RECORDS - i, 0, "value-" + i));
			}
		}
		assertEquals(List.of(CompressionCodecName.SNAPPY), codecs(file));
		try (IExtendedIterator<KvinRecord> reader = ParquetHelpers.createKvinRecordReader(
				new Path(file.getAbsolutePath()), null)) {
			assertEquals(RECORDS, reader.toList().size());
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
