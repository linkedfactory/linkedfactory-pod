package io.github.linkedfactory.core.rdf4j.fts;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;

/** Creates the default file-backed asynchronous coordinator. */
public final class FileFtsIndexUpdateCoordinatorFactory implements FtsIndexUpdateCoordinatorFactory {
	private final Duration shutdownGracePeriod;

	public FileFtsIndexUpdateCoordinatorFactory() {
		this(Duration.ofSeconds(10));
	}

	public FileFtsIndexUpdateCoordinatorFactory(Duration shutdownGracePeriod) {
		if (shutdownGracePeriod == null || shutdownGracePeriod.isNegative()) {
			throw new IllegalArgumentException("FTS shutdown grace period must not be negative");
		}
		this.shutdownGracePeriod = shutdownGracePeriod;
	}

	@Override
	public FtsIndexUpdateCoordinator create(Path outboxDirectory, String storeIdentity,
			FtsIndexUpdateStrategy strategy, FtsIndexUpdateCoordinator.JournalCleaner journalCleaner)
			throws IOException {
		return new FileFtsIndexUpdateCoordinator(
				outboxDirectory, storeIdentity, strategy, journalCleaner, shutdownGracePeriod);
	}
}
