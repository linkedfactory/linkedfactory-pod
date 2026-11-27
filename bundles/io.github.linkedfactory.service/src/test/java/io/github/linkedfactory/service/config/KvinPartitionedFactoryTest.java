package io.github.linkedfactory.service.config;

import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.partitioned.KvinPartitioned;
import net.enilink.komma.core.ILiteral;
import net.enilink.komma.core.Literal;
import net.enilink.komma.core.URIs;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class KvinPartitionedFactoryTest {
	private static final String SPEC = """
			{"spec-id":0,"fields":[{"name":"item_bucket","transform":"bucket[8]","source-id":1,"field-id":1000}]}
			""";

	@Rule
	public TemporaryFolder temporary = new TemporaryFolder();

	private KvinPartitionedFactory factory(String retentionPeriod, String partitionSpec, String tableProperties) {
		return factory(retentionPeriod, partitionSpec, tableProperties, null, null);
	}

	private KvinPartitionedFactory factory(String retentionPeriod, String partitionSpec, String tableProperties,
			String archiveLocation, String hadoopProperties) {
		return new KvinPartitionedFactory() {
			@Override
			public String getDirName() {
				return temporary.getRoot().getAbsolutePath();
			}

			@Override
			public ILiteral getArchiveInterval() {
				return null;
			}

			@Override
			public ILiteral getRetentionPeriod() {
				return retentionPeriod == null ? null : new Literal(retentionPeriod);
			}

			@Override
			public String getPartitionSpec() {
				return partitionSpec;
			}

			@Override
			public String getTableProperties() {
				return tableProperties;
			}

			@Override
			public String getArchiveLocation() {
				return archiveLocation;
			}

			@Override
			public String getHadoopProperties() {
				return hadoopProperties;
			}
		};
	}

	private Table archiveTable() {
		return new HadoopTables(new Configuration()).load(new File(temporary.getRoot(), "archive/iceberg").toString());
	}

	@Test
	public void opensTheArchiveWithItsSettings() throws Exception {
		var item = URIs.createURI("urn:partitioned:item");
		var property = URIs.createURI("urn:partitioned:property");
		try (var store = (KvinPartitioned) factory("P30D", SPEC, "kvin.compaction.min-files=4").create()) {
			store.put(new KvinTuple(null, item, property, System.currentTimeMillis(), 42));
			store.runArchival();
			try (var rows = store.fetch(item, property, null, 0)) {
				assertEquals(List.of(42), rows.toList().stream().map(tuple -> tuple.value).toList());
			}
		}
		Table table = archiveTable();
		assertEquals("4", table.properties().get("kvin.compaction.min-files"));
		assertEquals("item_bucket", table.spec().fields().getFirst().name());
	}

	@Test
	public void passesTheSettingsToTheArchive() {
		File path = temporary.getRoot();
		var options = factory("P30D", null, "write.target-file-size-bytes = 268435456").archiveOptions(path);
		assertEquals(new File(path, "archive").toString(), options.location());
		assertEquals(Duration.ofDays(30), options.retentionPeriod());
		assertNull("Archival runs the cleanups", options.cleanUpInterval());
		assertEquals(Map.of("write.target-file-size-bytes", "268435456"), options.tableProperties());
	}

	@Test
	public void archivesToTheConfiguredLocation() throws Exception {
		File archive = temporary.newFolder("elsewhere");
		var item = URIs.createURI("urn:partitioned:item");
		var property = URIs.createURI("urn:partitioned:property");
		try (var store = (KvinPartitioned) factory(null, null, null, archive.toString(), null).create()) {
			store.put(new KvinTuple(null, item, property, 1, 42));
			store.runArchival();
		}
		assertTrue(new File(archive, "iceberg/metadata").isDirectory());
		assertFalse(new File(temporary.getRoot(), "archive").exists());
	}

	@Test
	public void usesRemoteArchiveLocationsUnchanged() {
		var options = factory(null, null, null, "s3a://bucket/archive", "fs.s3a.endpoint=https://s3.example.org")
				.archiveOptions(temporary.getRoot());
		assertEquals("s3a://bucket/archive", options.location());
		assertEquals(Map.of("fs.s3a.endpoint", "https://s3.example.org"), options.hadoopProperties());
	}

	@Test
	public void rejectsInvalidSettings() {
		assertThrows(IllegalArgumentException.class, () -> factory("30 days", null, null).create());
		assertThrows(IllegalArgumentException.class,
				() -> factory(null, null, "kvin.compaction.min-files=1").create());
	}
}
