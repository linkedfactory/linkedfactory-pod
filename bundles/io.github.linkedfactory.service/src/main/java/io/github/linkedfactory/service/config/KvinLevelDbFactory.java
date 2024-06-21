package io.github.linkedfactory.service.config;

import io.github.linkedfactory.core.kvin.Kvin;
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
		File valueStorePath = getStorePAthOr("linkedfactory-valuestore");
		log.info("Using store path: {}", valueStorePath);
		return new KvinLevelDb(valueStorePath);
	}

	protected File getStorePAthOr(String name) {
		String dirName = getDirName();
		if (dirName == null) {
			dirName = name;
		}
		File valueStorePath;
		if (new File(dirName).isAbsolute()) {
			valueStorePath = new File(dirName);
		} else {
			try {
				valueStorePath = new File(new File(Platform.getInstanceLocation().getURL().toURI()), dirName);
			} catch (URISyntaxException e) {
				throw new RuntimeException(e);
			}
		}
		return valueStorePath;
	}

	@Iri("plugin://io.github.linkedfactory.service/data/dirName")
	public abstract String getDirName();
}
