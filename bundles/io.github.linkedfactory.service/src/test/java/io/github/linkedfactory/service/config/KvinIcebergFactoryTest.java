package io.github.linkedfactory.service.config;

import io.github.linkedfactory.core.kvin.KvinTuple;
import net.enilink.komma.core.URIs;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.UncheckedIOException;
import java.util.List;

import static org.junit.Assert.*;

public class KvinIcebergFactoryTest {
	private static final String SPEC = """
			{"spec-id":0,"fields":[{"name":"item_bucket","transform":"bucket[32]","source-id":1,"field-id":1000}]}
			""";

	@Rule
	public TemporaryFolder temporary = new TemporaryFolder();

	private KvinIcebergFactory factory(String partitionSpec) {
		return new KvinIcebergFactory() {
			@Override
			public String getDirName() {
				return temporary.getRoot().getAbsolutePath();
			}

			@Override
			public String getPartitionSpec() {
				return partitionSpec;
			}
		};
	}

	@Test
	public void configuresPartitioningAndAdoptsItWhenReopening() throws Exception {
		var item = URIs.createURI("urn:partition:item");
		var property = URIs.createURI("urn:partition:property");
		try (var store = factory(SPEC).create()) {
			store.put(new KvinTuple(item, property, null, 1, 42));
		}
		try (var store = factory(null).create()) {
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
}
