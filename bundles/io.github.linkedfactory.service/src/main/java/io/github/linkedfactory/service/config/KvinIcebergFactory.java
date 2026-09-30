package io.github.linkedfactory.service.config;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.iceberg.KvinIceberg;
import net.enilink.composition.annotations.Iri;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

@Iri("plugin://io.github.linkedfactory.service/data/KvinIceberg")
public abstract class KvinIcebergFactory extends KvinLevelDbFactory {
	private static final Logger log = LoggerFactory.getLogger(KvinIcebergFactory.class);

	@Override
	public Kvin create() {
		File path = getStorePathOr("linkedfactory-iceberg");
		log.info("Using path {} for iceberg archive", path);
		return new KvinIceberg(path.toString());
	}
}
