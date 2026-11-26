package io.github.linkedfactory.core.kvin.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.LocalInputFile;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.*;

public class ArchiveLocationTest {
	@Rule
	public TemporaryFolder temporary = new TemporaryFolder();

	@Test
	public void localPathsAreAbsoluteAndNormalized() {
		ArchiveLocation location = ArchiveLocation.of("target/../target/archive");
		assertTrue(location.isLocal());
		Path expected = Path.of("target/archive").toAbsolutePath().normalize();
		assertEquals(expected, location.localPath());
		assertEquals(expected.toString(), location.toString());
		assertEquals(expected.resolve("iceberg/data").toString(), location.resolve("iceberg/data"));
	}

	@Test
	public void fileUrisAndDriveLettersAreLocal() {
		assertEquals(Path.of("/tmp/archive"), ArchiveLocation.of("file:///tmp/archive").localPath());
		assertEquals(Path.of("/tmp/archive"), ArchiveLocation.of("file:/tmp/archive").localPath());
		assertNotNull("A drive letter is not a URI scheme", ArchiveLocation.localPath("c:/archive"));
	}

	@Test
	public void otherSchemesAreRemote() {
		ArchiveLocation location = ArchiveLocation.of("s3a://bucket/archive/");
		assertFalse(location.isLocal());
		assertNull(location.localPath());
		assertEquals("s3a://bucket/archive", location.toString());
		assertEquals("s3a://bucket/archive/iceberg/data", location.resolve("iceberg/data"));
		assertNull(ArchiveLocation.localPath("s3://bucket/archive/iceberg/data/file.parquet"));
	}

	@Test
	public void existsChecksLocalAndRemoteFiles() throws Exception {
		Files.createDirectories(temporary.getRoot().toPath().resolve("iceberg/metadata"));
		Configuration conf = new Configuration();
		for (String location : new String[]{temporary.getRoot().toString(),
				RemoteTestFileSystem.SCHEME + ":" + temporary.getRoot()}) {
			ArchiveLocation archive = ArchiveLocation.of(location);
			assertTrue(location, archive.exists("iceberg/metadata", conf));
			assertFalse(location, archive.exists("metadata", conf));
		}
	}

	@Test
	public void readsLocalFilesDirectlyAndRemoteFilesWithVectoredIo() throws Exception {
		Path file = temporary.newFile("data.parquet").toPath();
		assertTrue(ParquetInputFiles.DEFAULT.inputFile(file.toString()) instanceof LocalInputFile);
		assertFalse(ParquetInputFiles.DEFAULT.readOptions(file.toString()).build().useHadoopVectoredIo());

		// Creating a configuration per read parses Hadoop's default resources and made reads 30 times slower.
		assertSame("Local reads share one configuration",
				ParquetInputFiles.DEFAULT.readOptions(file.toString()).build().getConfiguration(),
				ParquetInputFiles.DEFAULT.readOptions(file.toString()).build().getConfiguration());

		String remote = RemoteTestFileSystem.SCHEME + ":" + file;
		assertTrue(ParquetInputFiles.DEFAULT.inputFile(remote) instanceof HadoopInputFile);
		assertTrue(ParquetInputFiles.DEFAULT.readOptions(remote).build().useHadoopVectoredIo());
	}
}
