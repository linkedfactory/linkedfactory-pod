package io.github.linkedfactory.service.config;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.parquet.KvinParquet;
import net.enilink.composition.annotations.Iri;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

@Iri("plugin://io.github.linkedfactory.service/data/KvinParquet")
public abstract class KvinParquetFactory extends KvinLevelDbFactory {
	private static final Logger log = LoggerFactory.getLogger(KvinParquetFactory.class);

	@Override
	public Kvin create() {
		File path = getStorePathOr("linkedfactory-parquet");
		log.info("Using path {} for parquet archive", path);
		return new KvinParquet(path.toString());
	}
}
