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
	/**
	 * Reads with Hadoop's default configuration, for readers outside of a store.
	 */
	static final ParquetInputFiles DEFAULT = new ParquetInputFiles(new Configuration());

	// Creating a Configuration per reader re-parses Hadoop's default resources. This also
	// applies to ParquetReadOptions.builder() without a configuration, which creates one.
	private final Configuration conf;
	private final ParquetConfiguration parquetConf;

	ParquetInputFiles(Configuration conf) {
		this.conf = conf;
		this.parquetConf = new HadoopParquetConfiguration(conf);
	}

	InputFile inputFile(String location) throws IOException {
		Path local = ArchiveLocation.localPath(location);
		if (local != null) {
			return new LocalInputFile(local);
		}
		return HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(location), conf);
	}

	ParquetReadOptions.Builder readOptions(String location) {
		if (ArchiveLocation.localPath(location) != null) {
			return ParquetReadOptions.builder(parquetConf).withUseHadoopVectoredIo(false);
		}
		return HadoopReadOptions.builder(conf, new org.apache.hadoop.fs.Path(location));
	}
}
