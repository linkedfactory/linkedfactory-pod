package io.github.linkedfactory.service.config;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.partitioned.KvinPartitioned;
import net.enilink.composition.annotations.Iri;
import net.enilink.komma.core.ILiteral;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;

@Iri("plugin://io.github.linkedfactory.service/data/KvinPartitioned")
public abstract class KvinPartitionedFactory extends KvinLevelDbFactory {
	private static final Logger log = LoggerFactory.getLogger(KvinPartitionedFactory.class);

	@Override
	public Kvin create() {
		try {
			File archivePath = getStorePathOr("linkedfactory-partition");
			ILiteral archiveInterval = getArchiveInterval();
			ILiteral retentionPeriod = getRetentionPeriod();
			log.info("Using path: {} for archiving", archivePath);

			Duration archiveIntervalDuration = ConfigLiterals.duration(archiveInterval);
			Duration retentionPeriodDuration = ConfigLiterals.duration(retentionPeriod);

			if (archiveIntervalDuration == null ) {
				log.error("invalid archive interval : {}", archiveInterval);
			}
			return new KvinPartitioned(archivePath, archiveIntervalDuration, retentionPeriodDuration);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Iri("plugin://io.github.linkedfactory.service/data/archiveInterval")
	public abstract ILiteral getArchiveInterval();

	@Iri("plugin://io.github.linkedfactory.service/data/retentionPeriod")
	public abstract ILiteral getRetentionPeriod();
}
