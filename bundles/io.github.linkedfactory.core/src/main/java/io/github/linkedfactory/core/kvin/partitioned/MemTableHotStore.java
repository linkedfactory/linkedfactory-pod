package io.github.linkedfactory.core.kvin.partitioned;

import io.github.linkedfactory.core.kvin.memtable.KvinMemTable;

import java.io.File;
import java.io.IOException;

/**
 * A hot store in a {@link KvinMemTable}.
 */
final class MemTableHotStore extends KvinMemTable implements HotStore {
	MemTableHotStore(File directory, boolean syncWrites) throws IOException {
		super(directory.toPath(), syncWrites);
	}
}
