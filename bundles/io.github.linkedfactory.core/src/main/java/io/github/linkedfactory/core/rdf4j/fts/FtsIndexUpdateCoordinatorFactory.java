package io.github.linkedfactory.core.rdf4j.fts;

import java.io.IOException;
import java.nio.file.Path;

/** Creates the coordinator that owns durable FTS delivery for a Sail. */
@FunctionalInterface
public interface FtsIndexUpdateCoordinatorFactory {
	FtsIndexUpdateCoordinator create(Path outboxDirectory, String storeIdentity,
			FtsIndexUpdateStrategy strategy, FtsIndexUpdateCoordinator.JournalCleaner journalCleaner)
			throws IOException;
}
