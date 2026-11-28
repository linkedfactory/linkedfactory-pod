package io.github.linkedfactory.core.kvin.partitioned;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.iceberg.KvinIceberg;
import io.github.linkedfactory.core.kvin.iceberg.KvinIcebergOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopTables;
import io.github.linkedfactory.core.kvin.util.KvinTupleGenerator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Tests archiving the hot store of KvinPartitioned into its archive store.
 */
public class KvinPartitionedArchiveStoreTest {
	static final URI ITEM = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/1");
	static final URI PROPERTY = URIs.createURI("http://example.org/1");

	static Kvin mockArchiveStore;
	static KvinIcebergOptions archiveOptions;

	File tempDir;

	@Before
	public void setup() throws IOException {
		tempDir = Files.createTempDirectory("kvinPartitionedArchiveStore").toFile();
	}

	@After
	public void cleanup() throws IOException {
		FileUtils.deleteDirectory(tempDir);
	}

	KvinTupleGenerator generator(long startTime) {
		return new KvinTupleGenerator()
				.setStartTime(startTime)
				.setItems(2)
				.setPropertiesPerItem(2)
				.setValuesPerProperty(10)
				.setItemPattern("http://localhost:8080/linkedfactory/demofactory/{}")
				.setPropertyPattern("http://example.org/{}");
	}

	@Test
	public void shouldArchiveIntoIceberg() throws IOException {
		KvinPartitioned store = new KvinPartitioned(tempDir, null, (Duration) null);
		try {
			store.put(generator(1672614000000L).generate());
			File archivedPath = store.hotStorePath;
			store.runArchival();

			assertTrue(store.archiveStore instanceof KvinIceberg);
			assertEquals(0, store.hotStore.fetchAll().toList().size());
			assertTrue(!archivedPath.exists());
			assertEquals(10, store.archiveStore.fetch(ITEM, PROPERTY, Kvin.DEFAULT_CONTEXT, 0).toList().size());

			// archived and hot values are merged
			store.put(generator(1673218800000L).generate());
			assertEquals(20, store.fetch(ITEM, PROPERTY, Kvin.DEFAULT_CONTEXT, 0).toList().size());
			assertEquals(2, store.properties(ITEM, Kvin.DEFAULT_CONTEXT).toList().size());
		} finally {
			store.close();
		}
	}

	@Test
	public void shouldCloseArchiveStore() throws IOException {
		mockArchiveStore = mock(Kvin.class);
		KvinPartitioned store = new KvinPartitioned(tempDir, null, (Duration) null) {
			@Override
			protected Kvin createArchiveStore(KvinIcebergOptions archiveOptions) {
				// captured locals are not yet available while the super constructor runs
				return mockArchiveStore;
			}
		};
		store.close();
		verify(mockArchiveStore).close();
	}

	@Test
	public void shouldOpenTheArchiveWithItsSettings() throws IOException {
		String archive = new File(tempDir, "archive").toString();
		KvinIcebergOptions options = KvinIcebergOptions.builder(archive)
				.tableProperty("kvin.compaction.min-files", "4")
				.cleanUpInterval(Duration.ofMillis(10))
				.build();
		KvinPartitioned store = new KvinPartitioned(tempDir, null, options) {
			@Override
			protected Kvin createArchiveStore(KvinIcebergOptions archiveOptions) {
				KvinPartitionedArchiveStoreTest.archiveOptions = archiveOptions;
				return super.createArchiveStore(archiveOptions);
			}
		};
		store.close();
		assertNull("Archival runs the cleanups", archiveOptions.cleanUpInterval());
		assertEquals("4", new HadoopTables(new Configuration()).load(archive + "/iceberg")
				.properties().get("kvin.compaction.min-files"));
	}

	@Test
	public void shouldArchiveToARemoteLocation() throws IOException {
		String archive = "kvintest:" + new File(tempDir, "remote-archive");
		KvinPartitioned store = new KvinPartitioned(tempDir, null, KvinIcebergOptions.builder(archive).build());
		try {
			store.put(generator(1672614000000L).generate());
			store.runArchival();
			assertEquals(0, store.hotStore.fetchAll().toList().size());
			assertEquals(10, store.fetch(ITEM, PROPERTY, Kvin.DEFAULT_CONTEXT, 0).toList().size());
		} finally {
			store.close();
		}
		assertTrue(new File(tempDir, "remote-archive/iceberg/metadata").isDirectory());
		assertTrue("The default archive directory is not used", !new File(tempDir, "archive").exists());
	}
}
