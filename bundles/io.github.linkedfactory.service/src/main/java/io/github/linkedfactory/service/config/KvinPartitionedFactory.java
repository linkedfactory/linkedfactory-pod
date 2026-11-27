package io.github.linkedfactory.service.config;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.iceberg.KvinIcebergOptions;
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
			File path = getStorePathOr("linkedfactory-partition");
			ILiteral archiveInterval = getArchiveInterval();
			log.info("Using path: {} for archiving", path);

			Duration archiveIntervalDuration = ConfigLiterals.duration(archiveInterval);
			if (archiveIntervalDuration == null) {
				log.error("invalid archive interval : {}", archiveInterval);
			}
			return new KvinPartitioned(path, archiveIntervalDuration, archiveOptions(path));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * The settings of the Iceberg archive, by default in the directory {@code archive} of the store.
	 */
	KvinIcebergOptions archiveOptions(File path) {
		String archiveLocation = getArchiveLocation();
		String location = archiveLocation == null ? new File(path, "archive").toString()
				: locationInWorkspace(archiveLocation);
		log.info("Using location {} for the archive", location);
		return KvinIcebergOptions.builder(location)
				.retentionPeriod(ConfigLiterals.requireDuration(getRetentionPeriod(), "retentionPeriod"))
				.partitionSpec(getPartitionSpec())
				.tableProperties(ConfigLiterals.properties(getTableProperties()))
				.hadoopProperties(ConfigLiterals.properties(getHadoopProperties()))
				.build();
	}

	@Iri("plugin://io.github.linkedfactory.service/data/archiveInterval")
	public abstract ILiteral getArchiveInterval();

	@Iri("plugin://io.github.linkedfactory.service/data/retentionPeriod")
	public abstract ILiteral getRetentionPeriod();

	@Iri("plugin://io.github.linkedfactory.service/data/partitionSpec")
	public abstract String getPartitionSpec();

	@Iri("plugin://io.github.linkedfactory.service/data/tableProperties")
	public abstract String getTableProperties();

	@Iri("plugin://io.github.linkedfactory.service/data/archiveLocation")
	public abstract String getArchiveLocation();

	@Iri("plugin://io.github.linkedfactory.service/data/hadoopProperties")
	public abstract String getHadoopProperties();
}
