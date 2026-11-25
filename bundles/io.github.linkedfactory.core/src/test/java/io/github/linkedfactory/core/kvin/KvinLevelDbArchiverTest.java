package io.github.linkedfactory.core.kvin;

import io.github.linkedfactory.core.kvin.iceberg.KvinIceberg;
import io.github.linkedfactory.core.kvin.leveldb.KvinLevelDb;
import io.github.linkedfactory.core.kvin.leveldb.KvinLevelDbArchiver;
import io.github.linkedfactory.core.kvin.parquet.KvinParquet;
import io.github.linkedfactory.core.kvin.util.KvinTupleGenerator;
import net.enilink.komma.core.URIs;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KvinLevelDbArchiverTest {
	File archiveTempDir;
	File leveldbTempDir;
	KvinLevelDb databaseStore;
	KvinParquet archiveStore;

	@Before
	public void setup() throws IOException {
		archiveTempDir = Files.createTempDirectory("archive").toFile();
		leveldbTempDir = Files.createTempDirectory("level0db-test").toFile();
		databaseStore = new KvinLevelDb(leveldbTempDir);
		archiveStore = new KvinParquet(archiveTempDir.getAbsolutePath() + "/");
		addTestData();
	}

	@After
	public void cleanup() throws IOException {
		try {
			try {
				databaseStore.close();
			} finally {
				archiveStore.close();
			}
		} finally {
			FileUtils.deleteDirectory(archiveTempDir);
			FileUtils.deleteDirectory(leveldbTempDir);
		}
	}

	void addTestData() {
		databaseStore.put(new KvinTupleGenerator()
				.setStartTime(1697407200000L)
				.setItems(10)
				.setPropertiesPerItem(10)
				.setValuesPerProperty(10)
				.setItemPattern("http://localhost:8080/linkedfactory/demofactory/new-week/{}")
				.setPropertyPattern("http://example.org/{}")
				.generate());
	}

	@Test
	public void testLevelDbToParquetArchival() {
		KvinLevelDbArchiver dbArchiver = new KvinLevelDbArchiver(databaseStore, archiveStore);
		dbArchiver.archive();
		assertTrue(new File(archiveTempDir.getPath()).listFiles().length > 0);
	}

	@Test
	public void testLevelDbToIcebergArchival() throws IOException {
		File icebergTempDir = Files.createTempDirectory("iceberg-archive").toFile();
		try (KvinIceberg icebergStore = new KvinIceberg(icebergTempDir.getAbsolutePath())) {
			new KvinLevelDbArchiver(databaseStore, icebergStore).archive();
			assertEquals(10, icebergStore.fetch(
					URIs.createURI("http://localhost:8080/linkedfactory/demofactory/new-week/1"),
					URIs.createURI("http://example.org/1"), Kvin.DEFAULT_CONTEXT, 0).toList().size());
		} finally {
			FileUtils.deleteDirectory(icebergTempDir);
		}
	}
}
