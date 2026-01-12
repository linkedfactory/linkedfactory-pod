package io.github.linkedfactory.core.kvin.parquet.records;

import io.github.linkedfactory.core.kvin.parquet.ParquetHelpers;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;

/**
 * A ParquetWriter for KvinRecords.
 */
public class KvinParquetWriter {
	/**
	 * Creates a builder for configuring ParquetWriter.
	 *
	 * @param file the output file to create
	 * @return a {@link Builder} to create a {@link ParquetWriter}
	 *
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

	/**
	 * A builder for configuring ParquetWriter for KvinRecords.
	 */
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
