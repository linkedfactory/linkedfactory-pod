package io.github.linkedfactory.core.rdf4j.fts.config;

import io.github.linkedfactory.core.rdf4j.fts.FtsIndexUpdateStrategy;
import io.github.linkedfactory.core.rdf4j.fts.FtsIndexUpdateCoordinatorFactory;
import io.github.linkedfactory.core.rdf4j.fts.FileFtsIndexUpdateCoordinator;
import io.github.linkedfactory.core.rdf4j.fts.FtsSail;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.osgi.framework.Bundle;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.lang.reflect.Proxy;
import java.net.URL;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class FtsIndexUpdateStrategyFactoryTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void backendProviderCanCreateStrategyWithoutHttpConfiguration() throws Exception {
		AtomicBoolean created = new AtomicBoolean();
		FtsIndexUpdateStrategyFactory provider = new FtsIndexUpdateStrategyFactory() {
			@Override
			public String backendType() {
				return "embedded-test";
			}

			@Override
			public FtsIndexUpdateStrategy create(FtsSailConfig config) {
				assertTrue(config.getEndpoint() == null);
				created.set(true);
				return update -> {
				};
			}
		};
		FtsSailConfig config = new FtsSailConfig();
		config.setBackend("embedded-test");
		config.setOutboxDir(Path.of("target", "embedded-test-outbox").toAbsolutePath().toString());
		config.setStoreIdentity("embedded-test-store");

		FtsSail sail = (FtsSail) new FtsSailFactory(List.of(provider)).getSail(config);

		assertNotNull(sail);
		assertTrue(created.get());
	}

	@Test
	public void duplicateBackendProvidersAreRejected() {
		FtsIndexUpdateStrategyFactory first = provider("embedded");
		FtsIndexUpdateStrategyFactory duplicate = provider("EMBEDDED");
		try {
			new FtsSailFactory(List.of(first, duplicate));
		} catch (IllegalArgumentException expected) {
			assertTrue(expected.getMessage().contains("Duplicate FTS strategy factory"));
			return;
		}
		throw new AssertionError("Expected duplicate backend providers to be rejected case-insensitively");
	}

	@Test
	public void missingBackendProviderReportsAvailableStrategies() throws Exception {
		FtsSailConfig config = new FtsSailConfig();
		config.setBackend("missing");
		config.setOutboxDir(temporaryFolder.newFolder("missing-provider-outbox").getAbsolutePath());
		config.setStoreIdentity("missing-provider-store");
		try {
			new FtsSailFactory(List.of(provider("available"))).getSail(config);
		} catch (org.eclipse.rdf4j.sail.config.SailConfigException expected) {
			assertTrue(expected.getMessage().contains("Available strategies: [available]"));
			return;
		}
		throw new AssertionError("Expected missing backend configuration to fail");
	}

	@Test(expected = org.eclipse.rdf4j.sail.config.SailConfigException.class)
	public void elasticProviderOwnsEndpointValidation() throws Exception {
		FtsSailConfig config = new FtsSailConfig();
		config.setOutboxDir(Path.of("target", "elastic-test-outbox").toAbsolutePath().toString());
		config.setStoreIdentity("elastic-test-store");
		new FtsSailFactory().getSail(config);
	}

	@Test
	public void osgiBundleProviderIsLoadedFromItsOwnDescriptorAndClassLoader() throws Exception {
		String descriptor = "META-INF/services/" + FtsIndexUpdateStrategyFactory.class.getName();
		ClassLoader providerLoader = FtsSailFactory.class.getClassLoader();
		URL descriptorUrl = providerLoader.getResource(descriptor);
		assertNotNull(descriptorUrl);
		Bundle bundle = (Bundle) Proxy.newProxyInstance(getClass().getClassLoader(),
				new Class<?>[] {Bundle.class}, (proxy, method, args) -> switch (method.getName()) {
					case "getEntry" -> descriptor.equals(args[0]) ? descriptorUrl : null;
					case "loadClass" -> Class.forName((String) args[0], true, providerLoader);
					case "getSymbolicName" -> "test.fts.provider";
					default -> throw new UnsupportedOperationException(method.getName());
				});
		Map<String, FtsIndexUpdateStrategyFactory> discovered = new LinkedHashMap<>();

		FtsSailFactory.loadBundleProviders(bundle, discovered);

		assertEquals(1, discovered.size());
		assertEquals("elastic", discovered.values().iterator().next().backendType());
	}

	@Test
	public void sailFactoryUsesInjectedCoordinatorFactory() throws Exception {
		AtomicBoolean coordinatorCreated = new AtomicBoolean();
		FtsIndexUpdateStrategyFactory strategyFactory = new FtsIndexUpdateStrategyFactory() {
			@Override
			public String backendType() {
				return "embedded-test";
			}

			@Override
			public FtsIndexUpdateStrategy create(FtsSailConfig config) {
				return update -> {
				};
			}
		};
		FtsIndexUpdateCoordinatorFactory coordinatorFactory = (outbox, storeIdentity, strategy, cleaner) -> {
			coordinatorCreated.set(true);
			return new FileFtsIndexUpdateCoordinator(outbox, storeIdentity, strategy, cleaner);
		};
		FtsSailConfig config = new FtsSailConfig();
		config.setBackend("embedded-test");
		config.setOutboxDir(temporaryFolder.newFolder("injected-coordinator-outbox").getAbsolutePath());
		config.setStoreIdentity("injected-coordinator-store");
		FtsSail sail = (FtsSail) new FtsSailFactory(List.of(strategyFactory), coordinatorFactory).getSail(config);
		sail.setBaseSail(new MemoryStore());
		try {
			sail.init();
			assertTrue(coordinatorCreated.get());
		} finally {
			sail.shutDown();
		}
	}

	private static FtsIndexUpdateStrategyFactory provider(String backendType) {
		return new FtsIndexUpdateStrategyFactory() {
			@Override
			public String backendType() {
				return backendType;
			}

			@Override
			public FtsIndexUpdateStrategy create(FtsSailConfig config) {
				return update -> {
				};
			}
		};
	}
}
