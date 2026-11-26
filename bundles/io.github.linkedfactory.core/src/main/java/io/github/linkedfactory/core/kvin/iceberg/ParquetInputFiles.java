package io.github.linkedfactory.core.kvin.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.conf.HadoopParquetConfiguration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Opens data files for fetches, tuned for where they are stored.
 *
 * <p>Local files are read directly with {@link LocalInputFile}, without the overhead of the
 * Hadoop file system, and without vectored I/O, whose thread handoffs cost more than reading
 * a few small pages from a local file. Remote files are read through the Hadoop file system
 * registered for their scheme, with vectored I/O, which issues the range requests for the
 * pages of a row group in parallel.</p>
 */
final class ParquetInputFiles {
	// Creating a Configuration per reader re-parses Hadoop's default resources. This also
	// applies to ParquetReadOptions.builder() without a configuration, which creates one.
	private static final Configuration CONF = new Configuration();
	private static final ParquetConfiguration PARQUET_CONF = new HadoopParquetConfiguration(CONF);

	private ParquetInputFiles() {
	}

	static InputFile inputFile(String location) throws IOException {
		Path local = ArchiveLocation.localPath(location);
		if (local != null) {
			return new LocalInputFile(local);
		}
		return HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(location), CONF);
	}

	static ParquetReadOptions.Builder readOptions(String location) {
		if (ArchiveLocation.localPath(location) != null) {
			return ParquetReadOptions.builder(PARQUET_CONF).withUseHadoopVectoredIo(false);
		}
		return HadoopReadOptions.builder(CONF, new org.apache.hadoop.fs.Path(location));
	}
}
