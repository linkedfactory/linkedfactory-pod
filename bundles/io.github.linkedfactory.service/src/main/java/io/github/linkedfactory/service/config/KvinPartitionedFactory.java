package io.github.linkedfactory.service.config;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.partitioned.KvinPartitioned;
import net.enilink.composition.annotations.Iri;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

@Iri("plugin://io.github.linkedfactory.service/data/KvinPartitioned")
public abstract class KvinPartitionedFactory extends KvinLevelDbFactory {
	private static final Logger log = LoggerFactory.getLogger(KvinPartitionedFactory.class);

	@Override
	public Kvin create() {
		try {
			File archivePath = getStorePAthOr("linkedfactory-partition");
			int archiveInterval = getArchiveInterval();
			log.info("Using path: {} for archiving", archivePath);

			return new KvinPartitioned(archivePath, archiveInterval);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Iri("plugin://io.github.linkedfactory.service/data/archiveInterval")
	public abstract int getArchiveInterval();
}
