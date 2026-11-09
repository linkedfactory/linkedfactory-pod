package io.github.linkedfactory.core.rdf4j.fts.config;

import io.github.linkedfactory.core.rdf4j.fts.FtsSail;
import io.github.linkedfactory.core.rdf4j.fts.FtsFederatedServiceConfig;
import io.github.linkedfactory.core.rdf4j.fts.FtsIndexUpdateCoordinatorFactory;
import io.github.linkedfactory.core.rdf4j.fts.FileFtsIndexUpdateCoordinatorFactory;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.config.SailConfigException;
import org.eclipse.rdf4j.sail.config.SailFactory;
import org.eclipse.rdf4j.sail.config.SailImplConfig;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

public class FtsSailFactory implements SailFactory {
	public static final String SAIL_TYPE = "fts:FtsSail";
	private final Map<String, FtsIndexUpdateStrategyFactory> strategyFactories;
	private final FtsIndexUpdateCoordinatorFactory coordinatorFactory;

	public FtsSailFactory() {
		this(discoverStrategyFactories(), new FileFtsIndexUpdateCoordinatorFactory());
	}

	private static List<FtsIndexUpdateStrategyFactory> discoverStrategyFactories() {
		Map<String, FtsIndexUpdateStrategyFactory> discovered = new LinkedHashMap<>();
		try {
			ServiceLoader.load(FtsIndexUpdateStrategyFactory.class, FtsSailFactory.class.getClassLoader())
					.forEach(factory -> discovered.putIfAbsent(factory.getClass().getName(), factory));
		} catch (java.util.ServiceConfigurationError e) {
			throw new IllegalStateException("Unable to load an FTS strategy provider from Java services", e);
		}

		Bundle owner = FrameworkUtil.getBundle(FtsSailFactory.class);
		BundleContext context = owner == null ? null : owner.getBundleContext();
		if (context != null) {
			for (Bundle bundle : context.getBundles()) {
				loadBundleProviders(bundle, discovered);
			}
		}
		return List.copyOf(discovered.values());
	}

	static void loadBundleProviders(Bundle bundle,
			Map<String, FtsIndexUpdateStrategyFactory> discovered) {
		String descriptor = "META-INF/services/" + FtsIndexUpdateStrategyFactory.class.getName();
		java.net.URL entry = bundle.getEntry(descriptor);
		if (entry == null) {
			return;
		}
		try (BufferedReader reader = new BufferedReader(
				new InputStreamReader(entry.openStream(), StandardCharsets.UTF_8))) {
			String line;
			while ((line = reader.readLine()) != null) {
				String className = line.split("#", 2)[0].trim();
				if (className.isEmpty() || discovered.containsKey(className)) {
					continue;
				}
				Class<? extends FtsIndexUpdateStrategyFactory> provider = bundle.loadClass(className)
						.asSubclass(FtsIndexUpdateStrategyFactory.class);
				discovered.put(className, provider.getDeclaredConstructor().newInstance());
			}
		} catch (IOException | ReflectiveOperationException | LinkageError | ClassCastException e) {
			throw new IllegalStateException("Unable to load FTS strategy providers from OSGi bundle "
					+ bundle.getSymbolicName(), e);
		}
	}

	public FtsSailFactory(Collection<? extends FtsIndexUpdateStrategyFactory> strategyFactories) {
		this(strategyFactories, new FileFtsIndexUpdateCoordinatorFactory());
	}

	public FtsSailFactory(Collection<? extends FtsIndexUpdateStrategyFactory> strategyFactories,
			FtsIndexUpdateCoordinatorFactory coordinatorFactory) {
		if (coordinatorFactory == null) {
			throw new NullPointerException("FTS update coordinator factory is required");
		}
		this.coordinatorFactory = coordinatorFactory;
		this.strategyFactories = new LinkedHashMap<>();
		for (FtsIndexUpdateStrategyFactory factory : strategyFactories) {
			String backendType = factory.backendType();
			if (backendType == null || backendType.isBlank()) {
				throw new IllegalArgumentException("FTS strategy factory must declare a backend type");
			}
			String key = backendType.toLowerCase(java.util.Locale.ROOT);
			if (this.strategyFactories.putIfAbsent(key, factory) != null) {
				throw new IllegalArgumentException("Duplicate FTS strategy factory for backend: " + backendType);
			}
		}
	}

	@Override
	public String getSailType() {
		return SAIL_TYPE;
	}

	@Override
	public SailImplConfig getConfig() {
		return new FtsSailConfig();
	}

	@Override
	public Sail getSail(SailImplConfig config) throws SailConfigException {
		if (!SAIL_TYPE.equals(config.getType())) {
			throw new SailConfigException("Invalid Sail type: " + config.getType());
		}

		FtsSail sail;
		if (config instanceof FtsSailConfig ftsConfig) {
			if (ftsConfig.getOutboxDir() == null || ftsConfig.getStoreIdentity() == null) {
				throw new SailConfigException(
						"FTS requires a persistent absolute fts:outboxDir and stable fts:storeIdentity");
			}
			String backendType = ftsConfig.getBackend().toLowerCase(java.util.Locale.ROOT);
			FtsIndexUpdateStrategyFactory strategyFactory = strategyFactories.get(backendType);
			if (strategyFactory == null) {
				throw new SailConfigException("Unsupported FTS update strategy: " + ftsConfig.getBackend()
						+ ". Available strategies: " + strategyFactories.keySet());
			}
			sail = new FtsSail(strategyFactory.create(ftsConfig),
					Path.of(ftsConfig.getOutboxDir()), ftsConfig.getStoreIdentity(),
					new FtsFederatedServiceConfig(
							ftsConfig.getBackend(),
							ftsConfig.getEndpoint(),
							ftsConfig.getSearchPath(),
							ftsConfig.isFailOnError(),
							ftsConfig.getDefaultLimit()),
					Set.copyOf(ftsConfig.getExcludedModels()),
					coordinatorFactory);
		} else {
			throw new SailConfigException(
					"Unsupported FTS Sail configuration: " + config.getClass().getName());
		}
		return sail;
	}
}
