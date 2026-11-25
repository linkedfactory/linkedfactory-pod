package io.github.linkedfactory.core.kvin.partitioned;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.iceberg.KvinIceberg;
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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Tests KvinPartitioned with an archive store other than KvinParquet.
 */
public class KvinPartitionedArchiveStoreTest {
	static final URI ITEM = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/1");
	static final URI PROPERTY = URIs.createURI("http://example.org/1");

	static Kvin mockArchiveStore;

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
		KvinPartitioned store = icebergPartitioned(null);
		try {
			store.put(generator(1672614000000L).generate());
			store.runArchival();

			assertTrue(store.archiveStore instanceof KvinIceberg);
			assertEquals(0, store.hotStore.fetchAll().toList().size());
			assertTrue(!store.currentStoreArchivePath.exists());
			assertEquals(10, store.archiveStore.fetch(ITEM, PROPERTY, Kvin.DEFAULT_CONTEXT, 0).toList().size());

			// archived and hot values are merged
			store.put(generator(1673218800000L).generate());
			assertEquals(20, store.fetch(ITEM, PROPERTY, Kvin.DEFAULT_CONTEXT, 0).toList().size());
			assertEquals(2, store.properties(ITEM, Kvin.DEFAULT_CONTEXT).toList().size());
		} finally {
			store.close();
		}
	}

	KvinPartitioned icebergPartitioned(Duration retentionPeriod) throws IOException {
		return new KvinPartitioned(tempDir, null, retentionPeriod) {
			@Override
			protected Kvin createArchiveStore(File archiveStorePath, Duration retentionPeriod) {
				return new KvinIceberg(archiveStorePath.toString(), retentionPeriod);
			}
		};
	}

	@Test
	public void shouldApplyRetentionPeriodToIcebergArchive() throws IOException {
		long now = System.currentTimeMillis();
		long week = Duration.ofDays(7).toMillis();

		KvinPartitioned store = icebergPartitioned(null);
		try {
			store.put(generator(now - 4 * week).generate());
			store.put(generator(now).generate());
			store.runArchival();
			assertEquals(20, store.fetch(ITEM, PROPERTY, Kvin.DEFAULT_CONTEXT, 0).toList().size());
		} finally {
			store.close();
		}

		store = icebergPartitioned(Duration.ofDays(7));
		try {
			store.runArchival();
			assertEquals(10, store.fetch(ITEM, PROPERTY, Kvin.DEFAULT_CONTEXT, 0).toList().size());
			assertEquals(0, store.fetch(ITEM, PROPERTY, Kvin.DEFAULT_CONTEXT,
					now - 2 * week, 0, 0, 0, null).toList().size());
		} finally {
			store.close();
		}
	}

	@Test
	public void shouldCloseArchiveStore() throws IOException {
		mockArchiveStore = mock(Kvin.class);
		KvinPartitioned store = new KvinPartitioned(tempDir, null, null) {
			@Override
			protected Kvin createArchiveStore(File archiveStorePath, Duration retentionPeriod) {
				// captured locals are not yet available while the super constructor runs
				return mockArchiveStore;
			}
		};
		store.close();
		verify(mockArchiveStore).close();
	}
}
