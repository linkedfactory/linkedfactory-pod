package io.github.linkedfactory.core.kvin.memtable;

import io.github.linkedfactory.core.kvin.records.KvinRecord;
import io.github.linkedfactory.core.kvin.records.KvinRecordStore;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.commons.iterator.WrappedIterator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * A sequence of {@link TableFile}s in one directory that acts as a single record store.
 * <p>
 * The files are named <code>entries_&lt;n&gt;.dat</code>. Records are appended to the file with the highest
 * number. When that file reaches the maximum length, a new file is started. Reads merge the files. If
 * several files contain a record with the same time and sequence number, the record of the newest file
 * wins. Deletes are passed to every file, so each file appends a tombstone that covers only its own
 * older records, and records written after a delete are kept.
 */
public class TableFileSet implements KvinRecordStore {
	/**
	 * Default number of bytes after which a new table file is started.
	 */
	public static final long DEFAULT_MAX_FILE_LENGTH = 20L * 1024 * 1024;

	static final Pattern FILE_NAME = Pattern.compile("entries_(\\d+)\\.dat");

	final Path directory;
	final long maxFileLength;
	// ordered from oldest to newest
	final List<TableFile> files = new CopyOnWriteArrayList<>();
	long nextFileNr;

	public TableFileSet(Path directory) throws IOException {
		this(directory, DEFAULT_MAX_FILE_LENGTH);
	}

	public TableFileSet(Path directory, long maxFileLength) throws IOException {
		this.directory = directory;
		this.maxFileLength = maxFileLength;
		Files.createDirectories(directory);
		SortedMap<Long, Path> existing = new TreeMap<>();
		try (Stream<Path> paths = Files.list(directory)) {
			paths.forEach(p -> {
				Matcher m = FILE_NAME.matcher(p.getFileName().toString());
				if (m.matches()) {
					existing.put(Long.parseLong(m.group(1)), p);
				}
			});
		}
		try {
			for (Path p : existing.values()) {
				files.add(new TableFile(p));
			}
		} catch (IOException | RuntimeException e) {
			close();
			throw e;
		}
		nextFileNr = existing.isEmpty() ? 0 : existing.lastKey() + 1;
	}

	/**
	 * Returns the number of bytes written to all table files, including tombstones.
	 */
	public long length() {
		long length = 0;
		for (TableFile file : files) {
			length += file.length();
		}
		return length;
	}

	/**
	 * Returns the table files from oldest to newest.
	 */
	public List<TableFile> getFiles() {
		return Collections.unmodifiableList(files);
	}

	@Override
	public void put(KvinRecord... records) {
		put(Arrays.asList(records));
	}

	@Override
	public synchronized void put(Iterable<KvinRecord> records) {
		try {
			for (KvinRecord record : records) {
				put(record);
			}
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	/**
	 * Appends a record to the newest table file and starts a new file if required.
	 */
	synchronized void put(KvinRecord record) throws IOException {
		TableFile current = files.isEmpty() ? null : files.get(files.size() - 1);
		if (current == null || current.length() >= maxFileLength) {
			current = new TableFile(directory.resolve("entries_" + (nextFileNr++) + ".dat"));
			files.add(current);
		}
		current.put(record);
	}

	@Override
	public IExtendedIterator<KvinRecord> fetch(long itemId, Long propertyId, Long contextId, long limit) {
		return fetch(itemId, propertyId, contextId, Long.MAX_VALUE, 0, limit, 0, null);
	}

	@Override
	public IExtendedIterator<KvinRecord> fetch(long itemId, Long propertyId, Long contextId, long end, long begin,
	                                           long limit, long interval, String op) {
		List<TableFile> snapshot = new ArrayList<>(files);
		if (limit <= 0 || snapshot.isEmpty()) {
			return NiceIterator.emptyIterator();
		}
		if (snapshot.size() == 1) {
			return snapshot.get(0).fetch(itemId, propertyId, contextId, end, begin, limit, interval, op);
		}
		List<Long> propertyIds = propertyId != null ? List.of(propertyId) : properties(snapshot, itemId, contextId);
		List<KvinRecord> result = new ArrayList<>();
		for (Long property : propertyIds) {
			mergeSeries(snapshot, itemId, property, contextId, end, begin, limit - result.size(), interval, result);
			if (result.size() >= limit) {
				break;
			}
		}
		return WrappedIterator.create(result.iterator());
	}

	/**
	 * Returns all records, ordered by context, item, property, descending time and descending
	 * sequence number. Of the records with the same key in several files, the record of the
	 * newest file is returned. The files are merged one item at a time.
	 */
	public IExtendedIterator<KvinRecord> fetchAll() {
		return fetchAll(Long.MAX_VALUE);
	}

	/**
	 * Returns the newest record of each series of context, item and property, in the order of
	 * {@link #fetchAll()}.
	 */
	public IExtendedIterator<KvinRecord> fetchLatest() {
		return fetchAll(1);
	}

	private IExtendedIterator<KvinRecord> fetchAll(long limitPerSeries) {
		List<TableFile> snapshot = new ArrayList<>(files);
		SortedSet<TableFile.ItemWithContext> keys = new TreeSet<>();
		for (TableFile file : snapshot) {
			keys.addAll(file.items.keySet());
		}
		Iterator<TableFile.ItemWithContext> remainingKeys = keys.iterator();
		return new NiceIterator<>() {
			Iterator<KvinRecord> records = Collections.emptyIterator();

			@Override
			public boolean hasNext() {
				while (!records.hasNext() && remainingKeys.hasNext()) {
					TableFile.ItemWithContext key = remainingKeys.next();
					List<KvinRecord> itemRecords = new ArrayList<>();
					for (Long property : properties(snapshot, key.itemId, key.contextId)) {
						mergeSeries(snapshot, key.itemId, property, key.contextId, Long.MAX_VALUE, Long.MIN_VALUE,
								limitPerSeries, 0, itemRecords);
					}
					records = itemRecords.iterator();
				}
				return records.hasNext();
			}

			@Override
			public KvinRecord next() {
				if (!hasNext()) {
					throw new NoSuchElementException();
				}
				return records.next();
			}
		};
	}

	/**
	 * Merges the values of one property from all files into <code>result</code>, ordered by descending time
	 * and sequence number.
	 */
	private void mergeSeries(List<TableFile> snapshot, long itemId, long propertyId, Long contextId, long end,
	                         long begin, long limit, long interval, List<KvinRecord> result) {
		// with an interval, the selected values depend on the merged series, so no file may be cut off early
		long fileLimit = interval > 0 ? Long.MAX_VALUE : limit;
		record Head(KvinRecord record, int fileIndex, Iterator<KvinRecord> rest) {
		}
		// newest file first for equal keys
		PriorityQueue<Head> heads = new PriorityQueue<>((a, b) -> {
			int c = a.record.compareTo(b.record);
			return c != 0 ? c : Integer.compare(b.fileIndex, a.fileIndex);
		});
		for (int i = 0; i < snapshot.size(); i++) {
			Iterator<KvinRecord> it = snapshot.get(i)
					.fetch(itemId, propertyId, contextId, end, begin, fileLimit, 0, null).toList().iterator();
			if (it.hasNext()) {
				heads.add(new Head(it.next(), i, it));
			}
		}

		long remaining = limit;
		long nextTimeThreshold = end;
		KvinRecord last = null;
		while (!heads.isEmpty() && remaining > 0) {
			Head head = heads.poll();
			if (head.rest.hasNext()) {
				heads.add(new Head(head.rest.next(), head.fileIndex, head.rest));
			}
			KvinRecord r = head.record;
			if (last != null && last.time() == r.time() && last.seqNr() == r.seqNr()) {
				// shadowed by the same key in a newer file
				continue;
			}
			last = r;
			if (interval > 0 && r.time() > nextTimeThreshold) {
				continue;
			}
			result.add(r);
			remaining--;
			if (interval > 0) {
				nextTimeThreshold = r.time() - interval;
			}
		}
	}

	@Override
	public long delete(long itemId, Long propertyId, Long contextId, long end, long begin) {
		long deleted = 0;
		for (TableFile file : files) {
			deleted += file.delete(itemId, propertyId, contextId, end, begin);
		}
		return deleted;
	}

	@Override
	public boolean delete(long itemId, Long contextId) {
		boolean deleted = false;
		for (TableFile file : files) {
			deleted |= file.delete(itemId, contextId);
		}
		return deleted;
	}

	@Override
	public long delete(Long contextId) {
		long deleted = 0;
		for (TableFile file : files) {
			deleted += file.delete(contextId);
		}
		return deleted;
	}

	@Override
	public IExtendedIterator<Long> properties(long itemId, Long contextId) {
		return WrappedIterator.create(properties(new ArrayList<>(files), itemId, contextId).iterator());
	}

	private List<Long> properties(List<TableFile> snapshot, long itemId, Long contextId) {
		SortedSet<Long> properties = new TreeSet<>();
		for (TableFile file : snapshot) {
			try (IExtendedIterator<Long> it = file.properties(itemId, contextId)) {
				it.forEachRemaining(properties::add);
			}
		}
		return new ArrayList<>(properties);
	}

	@Override
	public void close() {
		RuntimeException error = null;
		for (TableFile file : files) {
			try {
				file.close();
			} catch (RuntimeException e) {
				if (error == null) {
					error = e;
				} else {
					error.addSuppressed(e);
				}
			}
		}
		if (error != null) {
			throw error;
		}
	}
}
