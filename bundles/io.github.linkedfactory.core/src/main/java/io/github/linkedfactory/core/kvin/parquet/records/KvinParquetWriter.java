package io.github.linkedfactory.core.kvin.parquet.records;

import java.io.IOException;
import java.util.Map;

import io.github.linkedfactory.core.kvin.parquet.ParquetHelpers;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;

/**
 * Write for {@link KvinRecord}
 */
public class KvinParquetWriter extends ParquetWriter<KvinRecord> {

	/**
	 * Create a new {@link KvinParquetWriter}.
	 *
	 * @param file                 The file name to write to.
	 * @param writeSupport         The schema to write with.
	 * @param compressionCodecName Compression code to use, or CompressionCodecName.UNCOMPRESSED
	 * @param blockSize            the block size threshold.
	 * @param pageSize             See parquet write up. Blocks are subdivided into pages for alignment and other purposes.
	 * @param enableDictionary     Whether to use a dictionary to compress columns.
	 * @param conf                 The Configuration to use.
	 * @throws IOException
	 */
	KvinParquetWriter(
			Path file,
			WriteSupport<KvinRecord> writeSupport,
			CompressionCodecName compressionCodecName,
			int blockSize,
			int pageSize,
			boolean enableDictionary,
			boolean enableValidation,
			ParquetProperties.WriterVersion writerVersion,
			Configuration conf)
			throws IOException {
		super(
				file,
				writeSupport,
				compressionCodecName,
				blockSize,
				pageSize,
				pageSize,
				enableDictionary,
				enableValidation,
				writerVersion,
				conf);
	}

	/**
	 * Creates a builder for configuring ParquetWriter.
	 *
	 * @param file the output file to create
	 * @return a {@link Builder} to create a {@link ParquetWriter}
	 */
	public static Builder builder(Path file) {
		return new Builder(file);
	}

	/**
	 * Creates a builder for configuring ParquetWriter.
	 *
	 * @param file the output file to create
	 * @return a {@link Builder} to create a {@link ParquetWriter}
	 */
	public static Builder builder(OutputFile file) {
		return new Builder(file);
	}

	public static class Builder extends ParquetWriter.Builder<KvinRecord, Builder> {
		private Builder(Path file) {
			super(file);
		}

		private Builder(OutputFile file) {
			super(file);
		}

		@Override
		protected Builder self() {
			return this;
		}

		@Override
		protected WriteSupport<KvinRecord> getWriteSupport(Configuration conf) {
			return getWriteSupport((ParquetConfiguration) null);
		}

		@Override
		protected WriteSupport<KvinRecord> getWriteSupport(ParquetConfiguration conf) {
			return new KvinRecordWriteSupport(ParquetHelpers.kvinTupleType);
		}
	}
}
