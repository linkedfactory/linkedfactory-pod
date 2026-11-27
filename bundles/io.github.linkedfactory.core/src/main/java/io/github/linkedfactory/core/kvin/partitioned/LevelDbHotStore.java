package io.github.linkedfactory.core.kvin.partitioned;

import io.github.linkedfactory.core.kvin.leveldb.KvinLevelDb;

import java.io.File;

/**
 * A hot store in a LevelDB database.
 */
final class LevelDbHotStore extends KvinLevelDb implements HotStore {
	LevelDbHotStore(File directory) {
		super(directory);
	}
}
