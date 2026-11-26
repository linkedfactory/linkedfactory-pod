package io.github.linkedfactory.core.kvin.iceberg;

import io.github.linkedfactory.core.kvin.KvinTuple;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.*;

public class KvinIcebergOptionsTest {
	private final URI item = URIs.createURI("urn:options:item");
	private final URI property = URIs.createURI("urn:options:property");

	@Rule
	public TemporaryFolder temporary = new TemporaryFolder();

	private String location() {
		return temporary.getRoot().toPath().resolve("archive").toString();
	}

	private Table table() {
		return new HadoopTables(new Configuration()).load(location() + "/iceberg");
	}

	private List<Long> times(KvinIceberg store) {
		try (var tuples = store.fetch(item, property, null, 0)) {
			return tuples.toList().stream().map(tuple -> tuple.time).toList();
		}
	}

	@Test
	public void setsTablePropertiesOnCreateAndUpdatesChangedOnesOnReopen() {
		var options = KvinIcebergOptions.builder(location())
				.tableProperty("kvin.compaction.min-files", "4")
				.tableProperty("write.parquet.compression-codec", "snappy")
				.build();
		new KvinIceberg(options).close();
		Map<String, String> created = table().properties();
		assertEquals("4", created.get("kvin.compaction.min-files"));
		assertEquals("Configured properties override the defaults", "snappy", created.get("write.parquet.compression-codec"));

		String metadata = ((org.apache.iceberg.HasTableOperations) table()).operations().current().metadataFileLocation();
		new KvinIceberg(options).close();
		assertEquals("Unchanged properties are not committed again", metadata, ((org.apache.iceberg.HasTableOperations) table()).operations().current().metadataFileLocation());

		new KvinIceberg(KvinIcebergOptions.builder(location()).tableProperty("kvin.compaction.min-files", "16").build()).close();
		Map<String, String> updated = table().properties();
		assertEquals("16", updated.get("kvin.compaction.min-files"));
		assertEquals("Properties that are left out keep their values", "snappy", updated.get("write.parquet.compression-codec"));
	}

	@Test
	public void rejectsInvalidSettingsBeforeCreatingTheTable() {
		for (var property : List.of("kvin.compaction.min-files=1", "kvin.write.batch-records=0",
				"write.target-file-size-bytes=large")) {
			String[] keyValue = property.split("=");
			var options = KvinIcebergOptions.builder(location()).tableProperty(keyValue[0], keyValue[1]).build();
			var e = assertThrows(IllegalArgumentException.class, () -> new KvinIceberg(options));
			assertTrue(e.getMessage(), e.getMessage().contains(keyValue[0]));
		}
		assertFalse(temporary.getRoot().toPath().resolve("archive/iceberg").toFile().exists());
		assertThrows(IllegalArgumentException.class,
				() -> KvinIcebergOptions.builder(location()).retentionPeriod(Duration.ZERO));
		assertThrows(IllegalArgumentException.class,
				() -> KvinIcebergOptions.builder(location()).cleanUpInterval(Duration.ofMillis(-1)));
	}

	@Test
	public void usesTheHadoopPropertiesForAllTablesAndReads() {
		String marker = UUID.randomUUID().toString();
		var options = KvinIcebergOptions.builder(RemoteTestFileSystem.SCHEME + ":" + location())
				.hadoopProperty(RemoteTestFileSystem.MARKER, marker)
				// without the file system cache, every access creates a file system with its configuration
				.hadoopProperty("fs." + RemoteTestFileSystem.SCHEME + ".impl.disable.cache", "true")
				.build();
		try (var store = new KvinIceberg(options)) {
			store.put(new KvinTuple(null, item, property, 1, 0, 1L));
			RemoteTestFileSystem.MARKERS.remove(marker);
			assertEquals(List.of(1L), times(store));
			assertTrue("Fetches read with the configured properties", RemoteTestFileSystem.MARKERS.contains(marker));
		}
	}

	@Test
	public void cleansUpAtTheConfiguredInterval() throws Exception {
		var options = KvinIcebergOptions.builder(location())
				.retentionPeriod(Duration.ofMillis(1))
				.cleanUpInterval(Duration.ofMillis(50))
				.build();
		try (var store = new KvinIceberg(options)) {
			store.put(new KvinTuple(null, item, property, 1, 0, 1L));
			long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
			while (!times(store).isEmpty() && System.nanoTime() < deadline) {
				Thread.sleep(20);
			}
			assertEquals("Old values are removed in the background", List.of(), times(store));
		}
	}

	@Test
	public void recognizesUriSchemes() {
		assertTrue(KvinIcebergOptions.hasUriScheme("s3a://bucket/archive"));
		assertTrue(KvinIcebergOptions.hasUriScheme("file:///tmp/archive"));
		assertFalse(KvinIcebergOptions.hasUriScheme("linkedfactory-iceberg"));
		assertFalse(KvinIcebergOptions.hasUriScheme("/tmp/archive"));
		assertFalse("A drive letter is not a URI scheme", KvinIcebergOptions.hasUriScheme("c:/archive"));
	}
}
