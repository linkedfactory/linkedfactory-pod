package io.github.linkedfactory.core.kvin.partitioned;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.memtable.KvinMemTable;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.NiceIterator;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * The store of a {@link KvinPartitioned} store that receives new values. An archival run
 * copies all values of the hot store into the archive and then removes the hot store.
 */
public interface HotStore extends Kvin {
	/**
	 * Hot stores in LevelDB databases, the default.
	 */
	Factory LEVELDB = LevelDbHotStore::new;

	/**
	 * Hot stores in {@link KvinMemTable}s whose writes are persisted by the operating system,
	 * see {@link KvinMemTable#KvinMemTable(java.nio.file.Path)}.
	 */
	Factory MEMTABLE = memTable(false);

	/**
	 * Hot stores in {@link KvinMemTable}s, see {@link KvinMemTable#KvinMemTable(java.nio.file.Path, boolean)}.
	 *
	 * @param syncWrites whether each write persists its values before it returns
	 */
	static Factory memTable(boolean syncWrites) {
		return directory -> new MemTableHotStore(directory, syncWrites);
	}

	/**
	 * Returns the approximate number of bytes of the stored values, or -1 if it is unknown.
	 * {@link KvinPartitioned} archives a hot store when it reaches the maximum size, so only
	 * hot stores of a known size are archived by size.
	 */
	default long sizeInBytes() {
		return -1;
	}

	/**
	 * Returns all values of the store. The values of each series of context, item and
	 * property follow one another, newest first.
	 */
	IExtendedIterator<KvinTuple> fetchAll();

	/**
	 * Returns the newest value of each series of context, item and property. By default, the
	 * first value of each series of {@link #fetchAll()}.
	 */
	default IExtendedIterator<KvinTuple> fetchLatest() {
		IExtendedIterator<KvinTuple> all = fetchAll();
		return new NiceIterator<>() {
			KvinTuple previous, next;

			@Override
			public boolean hasNext() {
				while (next == null && all.hasNext()) {
					KvinTuple tuple = all.next();
					if (previous == null || !tuple.context.equals(previous.context) || !tuple.item.equals(previous.item)
							|| !tuple.property.equals(previous.property)) {
						next = tuple;
					}
					previous = tuple;
				}
				return next != null;
			}

			@Override
			public KvinTuple next() {
				if (!hasNext()) {
					throw new NoSuchElementException();
				}
				KvinTuple result = next;
				next = null;
				return result;
			}

			@Override
			public void close() {
				all.close();
			}
		};
	}

	/**
	 * Opens the hot stores of a {@link KvinPartitioned} store.
	 */
	@FunctionalInterface
	interface Factory {
		/**
		 * Opens the hot store in the given directory, creating it if required. The directory
		 * contains only the files of this store.
		 */
		HotStore open(File directory) throws IOException;
	}
}
