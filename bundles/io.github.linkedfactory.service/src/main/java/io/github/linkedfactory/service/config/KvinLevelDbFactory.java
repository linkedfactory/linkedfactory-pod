package io.github.linkedfactory.service.config;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.iceberg.KvinIcebergOptions;
import io.github.linkedfactory.core.kvin.leveldb.KvinLevelDb;
import net.enilink.composition.annotations.Iri;
import org.eclipse.core.runtime.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URISyntaxException;

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

	protected File getStorePathOr(String name) {
		String dirName = getDirName();
		return inWorkspace(dirName == null ? name : dirName);
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

	@Iri("plugin://io.github.linkedfactory.service/data/dirName")
	public abstract String getDirName();
}
