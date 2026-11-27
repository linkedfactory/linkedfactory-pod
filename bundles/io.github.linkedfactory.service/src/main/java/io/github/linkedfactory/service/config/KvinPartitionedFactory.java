package io.github.linkedfactory.service.config;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.iceberg.KvinIcebergOptions;
import io.github.linkedfactory.core.kvin.partitioned.HotStore;
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
			HotStore.Factory hotStoreFactory = hotStoreFactory();
			long maxHotStoreSize = maxHotStoreSize();
			return new KvinPartitioned(path, archiveIntervalDuration, archiveOptions(path), hotStoreFactory,
					maxHotStoreSize);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * The type of the hot stores: {@code leveldb} by default, or {@code memtable}.
	 */
	HotStore.Factory hotStoreFactory() {
		boolean memTable = isMemTableHotStore();
		boolean syncWrites = ConfigLiterals.bool(getHotStoreSyncWrites(), "hotStoreSyncWrites", false);
		if (memTable) {
			return HotStore.memTable(syncWrites);
		}
		if (syncWrites) {
			throw new IllegalArgumentException("hotStoreSyncWrites requires the hotStore memtable");
		}
		return HotStore.LEVELDB;
	}

	/**
	 * True if the hot store type is {@code memtable}, false for {@code leveldb}.
	 */
	private boolean isMemTableHotStore() {
		String type = getHotStore();
		if (type == null || type.isBlank() || type.trim().equalsIgnoreCase("leveldb")) {
			return false;
		}
		if (type.trim().equalsIgnoreCase("memtable")) {
			return true;
		}
		throw new IllegalArgumentException("Invalid hotStore, use leveldb or memtable: " + type);
	}

	/**
	 * The size in bytes at which the hot store is archived, 0 for no limit. Only hot stores
	 * of the type {@code memtable} have a size.
	 */
	long maxHotStoreSize() {
		long size = ConfigLiterals.bytes(getMaxHotStoreSize(), "maxHotStoreSize", 0);
		if (size > 0 && !isMemTableHotStore()) {
			throw new IllegalArgumentException("maxHotStoreSize requires the hotStore memtable");
		}
		return size;
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
				.pageCacheSize(ConfigLiterals.bytes(getPageCacheSize(), "pageCacheSize",
						KvinIcebergOptions.DEFAULT_PAGE_CACHE_SIZE))
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

	@Iri("plugin://io.github.linkedfactory.service/data/pageCacheSize")
	public abstract ILiteral getPageCacheSize();

	@Iri("plugin://io.github.linkedfactory.service/data/hotStore")
	public abstract String getHotStore();

	@Iri("plugin://io.github.linkedfactory.service/data/maxHotStoreSize")
	public abstract ILiteral getMaxHotStoreSize();

	@Iri("plugin://io.github.linkedfactory.service/data/hotStoreSyncWrites")
	public abstract ILiteral getHotStoreSyncWrites();
}
