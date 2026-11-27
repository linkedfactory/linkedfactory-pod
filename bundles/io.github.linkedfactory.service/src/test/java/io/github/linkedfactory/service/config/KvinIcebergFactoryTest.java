package io.github.linkedfactory.service.config;

import io.github.linkedfactory.core.kvin.KvinTuple;
import net.enilink.komma.core.ILiteral;
import net.enilink.komma.core.Literal;
import net.enilink.komma.core.URIs;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class KvinIcebergFactoryTest {
	private static final String SPEC = """
			{"spec-id":0,"fields":[{"name":"item_bucket","transform":"bucket[32]","source-id":1,"field-id":1000}]}
			""";

	@Rule
	public TemporaryFolder temporary = new TemporaryFolder();

	/**
	 * The settings of a store, like those in the bundle configuration.
	 */
	private static class Settings {
		String location;
		String partitionSpec;
		String retentionPeriod;
		String cleanUpInterval;
		String tableProperties;
		String hadoopProperties;

		Settings(String location) {
			this.location = location;
		}
	}

	private Settings settings() {
		return new Settings(temporary.getRoot().getAbsolutePath());
	}

	private static ILiteral literal(String value) {
		return value == null ? null : new Literal(value);
	}

	private static KvinIcebergFactory factory(Settings settings) {
		return new KvinIcebergFactory() {
			@Override
			public String getLocation() {
				return settings.location;
			}

			@Override
			public String getPartitionSpec() {
				return settings.partitionSpec;
			}

			@Override
			public ILiteral getRetentionPeriod() {
				return literal(settings.retentionPeriod);
			}

			@Override
			public ILiteral getCleanUpInterval() {
				return literal(settings.cleanUpInterval);
			}

			@Override
			public String getTableProperties() {
				return settings.tableProperties;
			}

			@Override
			public String getHadoopProperties() {
				return settings.hadoopProperties;
			}
		};
	}

	private KvinIcebergFactory factory(String partitionSpec) {
		Settings settings = settings();
		settings.partitionSpec = partitionSpec;
		return factory(settings);
	}

	@Test
	public void configuresPartitioningAndAdoptsItWhenReopening() throws Exception {
		var item = URIs.createURI("urn:partition:item");
		var property = URIs.createURI("urn:partition:property");
		try (var store = factory(SPEC).create()) {
			store.put(new KvinTuple(null, item, property, 1, 42));
		}
		try (var store = factory((String) null).create()) {
			try (var rows = store.fetch(item, property, null, 0)) {
				assertEquals(List.of(42), rows.toList().stream().map(tuple -> tuple.value).toList());
			}
		}
		try (var ignored = factory(SPEC).create()) {
			assertThrows(IllegalArgumentException.class,
					() -> factory("{\"spec-id\":0,\"fields\":[]}").create());
		}
	}

	@Test
	public void rejectsInvalidPartitionConfiguration() {
		assertThrows(UncheckedIOException.class, () -> factory("not-json").create());
		assertThrows(IllegalArgumentException.class, () -> factory(SPEC.replace("bucket[32]", "day")).create());
		assertThrows(IllegalArgumentException.class,
				() -> factory(SPEC.replace("\"source-id\":1", "\"source-id\":6")
						.replace("bucket[32]", "identity")).create());
	}

	@Test
	public void passesPropertiesAndDurations() {
		Settings settings = settings();
		settings.retentionPeriod = "P30D";
		settings.cleanUpInterval = "3600000";
		settings.tableProperties = """
				kvin.compaction.min-files = 4
				    write.target-file-size-bytes=268435456  \s
				""";
		settings.hadoopProperties = "fs.s3a.endpoint=https://s3.example.org\nfs.s3a.path.style.access=true";
		var options = factory(settings).options();
		assertEquals(Duration.ofDays(30), options.retentionPeriod());
		assertEquals(Duration.ofHours(1), options.cleanUpInterval());
		assertEquals(Map.of("kvin.compaction.min-files", "4", "write.target-file-size-bytes", "268435456"),
				options.tableProperties());
		assertEquals(Map.of("fs.s3a.endpoint", "https://s3.example.org", "fs.s3a.path.style.access", "true"),
				options.hadoopProperties());
	}

	@Test
	public void cleansUpDailyIfOnlyARetentionPeriodIsSet() {
		Settings settings = settings();
		assertNull(factory(settings).options().cleanUpInterval());
		settings.retentionPeriod = "P30D";
		assertEquals(Duration.ofDays(1), factory(settings).options().cleanUpInterval());
	}

	@Test
	public void usesUriLocationsUnchanged() {
		assertEquals("s3a://bucket/pod", factory(new Settings("s3a://bucket/pod")).options().location());
		assertEquals(temporary.getRoot().getAbsolutePath(), factory(settings()).options().location());
	}

	@Test
	public void rejectsInvalidSettings() {
		Settings settings = settings();
		settings.retentionPeriod = "30 days";
		var e = assertThrows(IllegalArgumentException.class, () -> factory(settings).options());
		assertTrue(e.getMessage(), e.getMessage().contains("retentionPeriod"));

		Settings properties = settings();
		properties.tableProperties = "kvin.compaction.min-files=1";
		assertThrows(IllegalArgumentException.class, () -> factory(properties).create());
	}
}
