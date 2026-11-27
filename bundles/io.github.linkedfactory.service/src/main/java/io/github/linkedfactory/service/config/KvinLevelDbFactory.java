package io.github.linkedfactory.service.config;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.iceberg.KvinIcebergOptions;
import io.github.linkedfactory.core.kvin.leveldb.KvinLevelDb;
import net.enilink.composition.annotations.Iri;
import org.eclipse.core.runtime.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;

@Iri("plugin://io.github.linkedfactory.service/data/KvinLevelDb")
public abstract class KvinLevelDbFactory implements IKvinFactory {
	private static final Logger log = LoggerFactory.getLogger(KvinLevelDbFactory.class);

	public static String TYPE = "plugin://io.github.linkedfactory.service/data/KvinLevelDb";

	@Override
	public Kvin create() {
		File valueStorePath = getStorePathOr("linkedfactory-valuestore");
		log.info("Using store path: {}", valueStorePath);
		return new KvinLevelDb(valueStorePath);
	}

	/**
	 * Returns the configured location, or a default location.
	 */
	protected String getLocationOr(String defaultLocation) {
		String location = getLocation();
		String dirName = getDirName();
		if (dirName != null) {
			if (location != null) {
				throw new IllegalArgumentException("Set only location, not both location and the deprecated dirName");
			}
			log.warn("The setting dirName is deprecated, use location instead");
			location = dirName;
		}
		return location == null ? defaultLocation : location;
	}

	/**
	 * Returns the configured location as a local directory in the workspace, or a default
	 * directory. Accepts {@code file:} URIs and rejects other URIs, because LevelDB stores
	 * its files in a local directory.
	 */
	protected File getStorePathOr(String name) {
		String location = getLocationOr(name);
		if (location.startsWith("file:")) {
			try {
				return Path.of(URI.create(location)).toFile();
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException("Invalid file URI, use an absolute one such as file:///data/store: " + location, e);
			}
		}
		if (KvinIcebergOptions.hasUriScheme(location)) {
			throw new IllegalArgumentException("The location of a LevelDB store must be a local path: " + location);
		}
		return inWorkspace(location);
	}

	/**
	 * Resolves a relative path against the workspace; absolute paths are returned unchanged.
	 */
	protected File inWorkspace(String path) {
		if (new File(path).isAbsolute()) {
			return new File(path);
		}
		try {
			return new File(new File(Platform.getInstanceLocation().getURL().toURI()), path);
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Returns a URI such as {@code s3a://bucket/archive} unchanged, and resolves a path
	 * with {@link #inWorkspace(String)}.
	 */
	protected String locationInWorkspace(String location) {
		return KvinIcebergOptions.hasUriScheme(location) ? location : inWorkspace(location).toString();
	}

	@Iri("plugin://io.github.linkedfactory.service/data/location")
	public abstract String getLocation();

	/**
	 * @deprecated replaced by {@link #getLocation()}
	 */
	@Deprecated
	@Iri("plugin://io.github.linkedfactory.service/data/dirName")
	public abstract String getDirName();
}
