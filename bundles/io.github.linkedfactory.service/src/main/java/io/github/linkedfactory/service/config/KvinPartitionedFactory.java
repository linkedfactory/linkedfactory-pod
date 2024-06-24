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
import java.time.format.DateTimeParseException;

@Iri("plugin://io.github.linkedfactory.service/data/KvinPartitioned")
public abstract class KvinPartitionedFactory extends KvinLevelDbFactory {
	private static final Logger log = LoggerFactory.getLogger(KvinPartitionedFactory.class);

	@Override
	public Kvin create() {
		try {
			File archivePath = getStorePAthOr("linkedfactory-partition");
			ILiteral archiveInterval = getArchiveInterval();
			log.info("Using path: {} for archiving", archivePath);

			Duration archiveIntervalDuration = null;
			if (archiveInterval != null) {
				try {
					archiveIntervalDuration = Duration.ofMillis(Long.parseLong(archiveInterval.getLabel()));
				} catch (NumberFormatException nfe) {
					try {
						archiveIntervalDuration = Duration.parse(archiveInterval.getLabel());
					} catch (DateTimeParseException dtpe) {
						// ignore
					}
				}
				if (archiveIntervalDuration == null) {
					log.error("invalid archive interval: {}", archiveInterval);
				}
			}

			return new KvinPartitioned(archivePath, archiveIntervalDuration);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Iri("plugin://io.github.linkedfactory.service/data/archiveInterval")
	public abstract ILiteral getArchiveInterval();
}
