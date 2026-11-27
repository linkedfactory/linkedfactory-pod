package io.github.linkedfactory.core.kvin.iceberg;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;

import java.io.Closeable;
import java.util.Map;

/**
 * The caches that fetches of a store share: file footers, page indexes, decoded pages and open
 * file handles. Data files are immutable, so cached entries never become stale; entries of
 * files that were rewritten or deleted are {@link #invalidate(String) invalidated} to free
 * their memory. All caches are thread-safe.
 */
final class ReadCaches implements Closeable {
	static final long DEFAULT_PAGE_CACHE_BYTES = KvinIcebergOptions.DEFAULT_PAGE_CACHE_SIZE;
	private static final int FOOTER_CACHE_SIZE = 4096;

	/**
	 * A page of a column chunk. The page {@link #DICTIONARY} is the dictionary page.
	 *
	 * @param rowGroupStart the start position of the row group in the file
	 * @param column        the index of the column in the KVIN schema
	 */
	record PageKey(String location, long rowGroupStart, int column, int page) {
		static final int DICTIONARY = -1;
	}

	final Cache<String, ParquetMetadata> footers;
	// page indexes per file and row group start position
	final Cache<String, Map<Long, ColumnIndexStore>> columnIndexes;
	// DecodedPage for data pages, uncompressed DictionaryPage for dictionary pages
	final Cache<PageKey, Object> pages;
	final DataFileHandles handles;
	final ParquetInputFiles inputFiles;

	ReadCaches(Configuration conf, long pageCacheBytes) {
		this(CacheBuilder.newBuilder().maximumSize(FOOTER_CACHE_SIZE).build(),
				CacheBuilder.newBuilder().maximumSize(FOOTER_CACHE_SIZE).build(),
				pageCacheBytes, new ParquetInputFiles(conf), conf);
	}

	ReadCaches(Cache<String, ParquetMetadata> footers, Cache<String, Map<Long, ColumnIndexStore>> columnIndexes,
			long pageCacheBytes, ParquetInputFiles inputFiles, Configuration conf) {
		if (pageCacheBytes < 0) {
			throw new IllegalArgumentException("Page cache size must not be negative: " + pageCacheBytes);
		}
		this.footers = footers;
		this.columnIndexes = columnIndexes;
		this.pages = CacheBuilder.newBuilder().maximumWeight(pageCacheBytes)
				.weigher((Weigher<PageKey, Object>) (key, value) -> 64 + weight(value)).build();
		this.inputFiles = inputFiles;
		this.handles = new DataFileHandles(conf, DataFileHandles.DEFAULT_MAX_OPEN_FILES);
	}

	private static int weight(Object value) {
		if (value instanceof DecodedPage page) {
			return page.weight;
		}
		long size = ((DictionaryPage) value).getBytes().size();
		return (int) Math.min(Integer.MAX_VALUE, size);
	}

	/**
	 * Drops the cached data of a file and closes its handle once no reader uses it.
	 */
	void invalidate(String location) {
		invalidate(java.util.Set.of(location));
	}

	/**
	 * Like {@link #invalidate(String)} for several files, with one pass over the cached pages.
	 */
	void invalidate(java.util.Set<String> locations) {
		if (locations.isEmpty()) {
			return;
		}
		footers.invalidateAll(locations);
		columnIndexes.invalidateAll(locations);
		pages.asMap().keySet().removeIf(key -> locations.contains(key.location()));
		locations.forEach(handles::invalidate);
	}

	@Override
	public void close() {
		handles.close();
		pages.invalidateAll();
	}
}
