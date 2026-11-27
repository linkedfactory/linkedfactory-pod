package io.github.linkedfactory.service.config;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.iceberg.KvinIceberg;
import io.github.linkedfactory.core.kvin.iceberg.KvinIcebergOptions;
import net.enilink.composition.annotations.Iri;
import net.enilink.komma.core.ILiteral;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

@Iri("plugin://io.github.linkedfactory.service/data/KvinIceberg")
public abstract class KvinIcebergFactory extends KvinLevelDbFactory {
	private static final Logger log = LoggerFactory.getLogger(KvinIcebergFactory.class);
	// cleanup interval if only a retention period is configured
	static final Duration DEFAULT_CLEAN_UP_INTERVAL = Duration.ofDays(1);

	@Override
	public Kvin create() {
		return new KvinIceberg(options());
	}

	KvinIcebergOptions options() {
		String location = locationInWorkspace(getLocationOr("linkedfactory-iceberg"));
		Duration retentionPeriod = ConfigLiterals.requireDuration(getRetentionPeriod(), "retentionPeriod");
		Duration cleanUpInterval = ConfigLiterals.requireDuration(getCleanUpInterval(), "cleanUpInterval");
		if (cleanUpInterval == null && retentionPeriod != null) {
			cleanUpInterval = DEFAULT_CLEAN_UP_INTERVAL;
		}
		log.info("Using location {} for iceberg archive", location);
		return KvinIcebergOptions.builder(location)
				.retentionPeriod(retentionPeriod)
				.cleanUpInterval(cleanUpInterval)
				.partitionSpec(getPartitionSpec())
				.tableProperties(ConfigLiterals.properties(getTableProperties()))
				.hadoopProperties(ConfigLiterals.properties(getHadoopProperties()))
				.build();
	}

	@Iri("plugin://io.github.linkedfactory.service/data/partitionSpec")
	public abstract String getPartitionSpec();

	@Iri("plugin://io.github.linkedfactory.service/data/retentionPeriod")
	public abstract ILiteral getRetentionPeriod();

	@Iri("plugin://io.github.linkedfactory.service/data/cleanUpInterval")
	public abstract ILiteral getCleanUpInterval();

	@Iri("plugin://io.github.linkedfactory.service/data/tableProperties")
	public abstract String getTableProperties();

	@Iri("plugin://io.github.linkedfactory.service/data/hadoopProperties")
	public abstract String getHadoopProperties();
}
