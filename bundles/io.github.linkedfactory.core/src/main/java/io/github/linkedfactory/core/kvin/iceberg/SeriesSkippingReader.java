package io.github.linkedfactory.core.kvin.iceberg;

import com.github.luben.zstd.Zstd;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.parquet.ParquetMetricsRowGroupFilter;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.VersionParser;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * Flat KVIN reader that reads rows from cached, decoded pages.
 *
 * <p>For each row group that its statistics do not exclude, the reader computes the rows that
 * may match the filter from the page indexes, and reads only the pages that hold these rows.
 * Pages are decoded once into arrays and kept in the {@link ReadCaches#pages page cache}, so
 * repeated fetches of the same data read, decompress and decode nothing. Pages of the key
 * columns that are not cached are read together, in one batch per row group. Pages of value
 * columns are read only for rows that are returned, and value columns without any value in a
 * row group are not read at all.</p>
 *
 * <p>Once a series is {@link #skipThrough completed}, its remaining rows are skipped without
 * reading values, and subsequent row groups are pruned using the advancing series boundary.</p>
 *
 * <p>Because a reader sees all row groups of a file, the task's split range is applied here,
 * using the same midpoint rule as Parquet's range metadata filter.</p>
 */
final class SeriesSkippingReader implements CloseableIterable<KvinRow>, CloseableIterator<KvinRow> {
	// Indexes in the KVIN schema: the series start marker and the first optional
	// value column (valueInt). Columns 0-4 are the key columns.
	private static final int FIRST_COLUMN = 5;
	private static final int FIRST_VALUE_COLUMN = 6;
	private static final ParquetMetadataConverter CONVERTER = new ParquetMetadataConverter();

	private final Schema schema;
	private final Expression filter;
	private final KeyFilter keyFilter;
	private final ReadCaches caches;
	private final boolean ownsCaches;
	private final String location;
	private final ParquetMetadata footer;
	private final MessageType fileSchema;
	private final ParquetReadOptions options;
	private final VersionParser.ParsedVersion writerVersion;
	private final ColumnDescriptor[] descriptors;
	private final long rangeStart;
	private final long rangeEnd;
	private ParquetMetricsRowGroupFilter groupFilter;
	private DataFileHandles.Handle handle;
	private CodecFactory codecs;
	private ParquetFileReader indexReader;

	// the current row group
	private BlockMetaData group;
	private final ColumnChunkMetaData[] chunks;
	private final OffsetIndex[] offsets;
	// columns that are read in the current row group
	private final boolean[] readColumns;
	private final DecodedPage[] current;
	// pages of the current row group that were looked up or read, so that the cache may evict them meanwhile
	private final Map<ReadCaches.PageKey, Object> groupPages = new HashMap<>();
	// rows of the current row group that may match, see PageRanges
	private long[] ranges = PageRanges.NONE;
	private int rangeIndex;
	private long row;
	private long lastRow = -1;
	private int groupIndex;

	private long completedItem;
	private long completedContext;
	private long completedProperty;
	private boolean completed;
	private boolean groupFilterDirty;
	private boolean closed;
	private KvinRow next;
	// the series that the filter allows, for searching sorted files, or null
	private final SeriesBounds series;

	// for tests: row groups whose pages were read, rows that were looked at, values that were
	// looked up, and pages read from the file per column
	int rowGroupsRead;
	long rowsScanned;
	long valuesRead;
	final Map<Integer, Integer> pagesRead = new HashMap<>();

	SeriesSkippingReader(Schema schema, FileScanTask task, Expression filter,
			Cache<String, ParquetMetadata> footers) throws IOException {
		this(schema, task, filter, footers, CacheBuilder.newBuilder().build());
	}

	SeriesSkippingReader(Schema schema, FileScanTask task, Expression filter,
			Cache<String, ParquetMetadata> footers,
			Cache<String, Map<Long, ColumnIndexStore>> columnIndexes) throws IOException {
		this(schema, task, filter, new ReadCaches(footers, columnIndexes, ReadCaches.DEFAULT_PAGE_CACHE_BYTES,
				ParquetInputFiles.DEFAULT, new Configuration()), true, false);
	}

	/**
	 * @param caches the caches of the store, which the reader uses but does not close
	 */
	SeriesSkippingReader(Schema schema, FileScanTask task, Expression filter, ReadCaches caches) throws IOException {
		this(schema, task, filter, caches, false, false);
	}

	/**
	 * @param caches the caches of the store, which the reader uses but does not close
	 * @param sorted whether the rows of the file are sorted by context, item, property,
	 *               descending time and descending sequence number, so that the reader can
	 *               search for the rows of matching series instead of looking at each row
	 */
	SeriesSkippingReader(Schema schema, FileScanTask task, Expression filter, ReadCaches caches, boolean sorted)
			throws IOException {
		this(schema, task, filter, caches, false, sorted);
	}

	private SeriesSkippingReader(Schema schema, FileScanTask task, Expression filter, ReadCaches caches,
			boolean ownsCaches, boolean sorted) throws IOException {
		this.schema = schema;
		this.filter = filter;
		this.keyFilter = KeyFilter.of(schema, filter);
		this.groupFilter = new ParquetMetricsRowGroupFilter(schema, filter);
		this.caches = caches;
		this.ownsCaches = ownsCaches;
		this.rangeStart = task.start();
		this.rangeEnd = task.start() + task.length();
		this.location = task.file().location();
		// Dictionary and bloom filters would read extra data on every open.
		this.options = caches.inputFiles.readOptions(location).useDictionaryFilter(false).useBloomFilter(false).build();
		ParquetMetadata cached = caches.footers.getIfPresent(location);
		if (cached == null) {
			try (ParquetFileReader footerReader = ParquetFileReader.open(caches.inputFiles.inputFile(location), options)) {
				cached = footerReader.getFooter();
			}
			caches.footers.put(location, cached);
		}
		this.footer = cached;
		this.fileSchema = footer.getFileMetaData().getSchema();
		this.writerVersion = parseVersion(footer.getFileMetaData().getCreatedBy());
		int columnCount = schema.columns().size();
		this.descriptors = new ColumnDescriptor[columnCount];
		for (int i = 0; i < columnCount; i++) {
			descriptors[i] = fileSchema.getColumnDescription(new String[]{schema.columns().get(i).name()});
		}
		this.chunks = new ColumnChunkMetaData[columnCount];
		this.offsets = new OffsetIndex[columnCount];
		this.readColumns = new boolean[columnCount];
		this.current = new DecodedPage[columnCount];
		this.series = sorted ? SeriesBounds.of(filter) : null;
	}

	private static VersionParser.ParsedVersion parseVersion(String createdBy) {
		try {
			return VersionParser.parse(createdBy);
		} catch (RuntimeException | VersionParser.VersionParseException e) {
			return null;
		}
	}

	/**
	 * True if the statistics of a column chunk show that all its rows are null.
	 */
	private static boolean allNull(ColumnChunkMetaData chunk, long rowCount) {
		if (chunk == null) {
			return false;
		}
		var statistics = chunk.getStatistics();
		return statistics != null && statistics.isNumNullsSet() && statistics.getNumNulls() == rowCount;
	}

	private boolean inRange(BlockMetaData group) {
		long midpoint = group.getStartingPos() + group.getCompressedSize() / 2;
		return midpoint >= rangeStart && midpoint < rangeEnd;
	}

	/**
	 * Skips all rows up to and including the series of the given context, item, and property.
	 */
	void skipThrough(long context, long item, long property) {
		completed = true;
		completedContext = context;
		completedItem = item;
		completedProperty = property;
		groupFilterDirty = true;
	}

	@Override
	public CloseableIterator<KvinRow> iterator() {
		return this;
	}

	private boolean nextGroup() throws IOException {
		Arrays.fill(current, null);
		groupPages.clear();
		if (groupFilterDirty) {
			Expression after = Expressions.or(Expressions.greaterThan("contextId", completedContext),
					Expressions.and(Expressions.equal("contextId", completedContext),
							Expressions.or(Expressions.greaterThan("itemId", completedItem),
									Expressions.and(Expressions.equal("itemId", completedItem),
											Expressions.greaterThan("propertyId", completedProperty)))));
			groupFilter = new ParquetMetricsRowGroupFilter(schema, Expressions.and(filter, after));
			groupFilterDirty = false;
		}
		List<BlockMetaData> groups = footer.getBlocks();
		while (groupIndex < groups.size()) {
			BlockMetaData candidate = groups.get(groupIndex++);
			if (candidate.getRowCount() == 0 || !inRange(candidate) || !groupFilter.shouldRead(fileSchema, candidate)) {
				continue;
			}
			ColumnIndexStore store = columnIndexes(candidate);
			PageRanges.RowGroupIndexes indexes = store instanceof PageRanges.RowGroupIndexes cached ? cached
					: new PageRanges.RowGroupIndexes(store, candidate.getRowCount());
			// only the rows of pages that may match, or none
			long[] rows = PageRanges.compute(filter, indexes, candidate.getRowCount());
			if (rows.length == 0) {
				continue;
			}
			group = candidate;
			Map<String, ColumnChunkMetaData> byName = new HashMap<>();
			for (ColumnChunkMetaData chunk : candidate.getColumns()) {
				byName.put(chunk.getPath().toDotString(), chunk);
			}
			for (int i = 0; i < chunks.length; i++) {
				chunks[i] = byName.get(schema.columns().get(i).name());
				offsets[i] = indexes.getOffsetIndex(chunks[i].getPath());
				// fetches do not use the series start marker; value columns without
				// any value in this row group are not read at all
				readColumns[i] = i != FIRST_COLUMN && !(i >= FIRST_VALUE_COLUMN && allNull(chunks[i], candidate.getRowCount()));
			}
			ranges = rows;
			rangeIndex = 0;
			row = ranges[0];
			lastRow = ranges[1];
			rowGroupsRead++;
			prefetchKeyPages();
			return true;
		}
		return false;
	}

	/**
	 * Reads the pages of the key columns that hold the rows of the current row group and are
	 * not cached, in one batch.
	 */
	private void prefetchKeyPages() throws IOException {
		List<int[]> missing = new ArrayList<>();
		for (int column = 0; column < FIRST_COLUMN; column++) {
			OffsetIndex index = offsets[column];
			int previous = -1;
			for (int range = 0; range < ranges.length; range += 2) {
				for (int page = Math.max(pageOf(index, ranges[range]), previous + 1);
					 page < index.getPageCount() && index.getFirstRowIndex(page) <= ranges[range + 1]; page++) {
					ReadCaches.PageKey key = key(column, page);
					Object cached = caches.pages.getIfPresent(key);
					if (cached == null) {
						missing.add(new int[]{column, page});
					} else {
						groupPages.put(key, cached);
					}
					previous = page;
				}
			}
		}
		if (!missing.isEmpty()) {
			loadPages(missing);
		}
	}

	private ReadCaches.PageKey key(int column, int page) {
		return new ReadCaches.PageKey(location, group.getStartingPos(), column, page);
	}

	/**
	 * Returns the index of the page that holds a row.
	 */
	private static int pageOf(OffsetIndex index, long row) {
		int low = 0;
		int high = index.getPageCount() - 1;
		while (low < high) {
			int middle = (low + high + 1) >>> 1;
			if (index.getFirstRowIndex(middle) <= row) {
				low = middle;
			} else {
				high = middle - 1;
			}
		}
		return low;
	}

	/**
	 * Returns the page of a column that holds a row of the current row group.
	 */
	private DecodedPage page(int column, long row) throws IOException {
		DecodedPage page = current[column];
		if (page != null && page.contains(row)) {
			return page;
		}
		ReadCaches.PageKey key = key(column, pageOf(offsets[column], row));
		page = (DecodedPage) groupPages.get(key);
		if (page == null) {
			page = (DecodedPage) caches.pages.getIfPresent(key);
			if (page == null) {
				page = (DecodedPage) loadPages(List.<int[]>of(new int[]{column, key.page()})).get(key);
			} else {
				groupPages.put(key, page);
			}
		}
		current[column] = page;
		return page;
	}

	/**
	 * Reads, decodes and caches data pages of the current row group, and the dictionary pages
	 * that they need.
	 *
	 * @param requested pairs of column and page index
	 * @return the decoded pages and dictionaries by key
	 */
	private Map<ReadCaches.PageKey, Object> loadPages(List<int[]> requested) throws IOException {
		Map<ReadCaches.PageKey, Object> loaded = new HashMap<>();
		// dictionaries first, since decoding data pages needs them
		Map<ReadCaches.PageKey, DataFileHandles.Range> reads = new LinkedHashMap<>();
		for (int[] request : requested) {
			int column = request[0];
			ColumnChunkMetaData chunk = chunks[column];
			ReadCaches.PageKey dictionaryKey = key(column, ReadCaches.PageKey.DICTIONARY);
			if (chunk.hasDictionaryPage() && !reads.containsKey(dictionaryKey)) {
				Object dictionary = caches.pages.getIfPresent(dictionaryKey);
				if (dictionary != null) {
					loaded.put(dictionaryKey, dictionary);
				} else {
					long offset = chunk.getDictionaryPageOffset();
					long end = offsets[column].getOffset(0);
					reads.put(dictionaryKey, new DataFileHandles.Range(offset, new byte[Math.toIntExact(end - offset)]));
				}
			}
		}
		for (int[] request : requested) {
			OffsetIndex index = offsets[request[0]];
			reads.put(key(request[0], request[1]), new DataFileHandles.Range(index.getOffset(request[1]),
					new byte[index.getCompressedPageSize(request[1])]));
		}
		handle().read(new ArrayList<>(reads.values()));
		for (Map.Entry<ReadCaches.PageKey, DataFileHandles.Range> read : reads.entrySet()) {
			ReadCaches.PageKey key = read.getKey();
			int column = key.column();
			pagesRead.merge(column, 1, Integer::sum);
			Object page;
			if (key.page() == ReadCaches.PageKey.DICTIONARY) {
				page = parsePage(chunks[column], read.getValue().target());
			} else {
				DictionaryPage dictionary = (DictionaryPage) loaded.get(key(column, ReadCaches.PageKey.DICTIONARY));
				DataPage data = (DataPage) parsePage(chunks[column], read.getValue().target());
				page = DecodedPage.decode(descriptors[column], data, dictionary, writerVersion,
						offsets[column].getFirstRowIndex(key.page()));
			}
			loaded.put(key, page);
			groupPages.put(key, page);
			caches.pages.put(key, page);
		}
		return loaded;
	}

	/**
	 * Parses and decompresses a page, including its header.
	 *
	 * @return a {@link DictionaryPage} or an uncompressed {@link DataPage}
	 */
	private Object parsePage(ColumnChunkMetaData chunk, byte[] bytes) throws IOException {
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		PageHeader header = Util.readPageHeader(in);
		int start = bytes.length - in.available();
		int compressedSize = header.getCompressed_page_size();
		int uncompressedSize = header.getUncompressed_page_size();
		CompressionCodecName codec = chunk.getCodec();
		if (header.getType() == PageType.DICTIONARY_PAGE) {
			var dictionaryHeader = header.getDictionary_page_header();
			return new DictionaryPage(decompress(codec, bytes, start, compressedSize, uncompressedSize),
					uncompressedSize, dictionaryHeader.getNum_values(), CONVERTER.getEncoding(dictionaryHeader.getEncoding()));
		} else if (header.getType() == PageType.DATA_PAGE) {
			DataPageHeader dataHeader = header.getData_page_header();
			return new DataPageV1(decompress(codec, bytes, start, compressedSize, uncompressedSize),
					dataHeader.getNum_values(), uncompressedSize, null,
					CONVERTER.getEncoding(dataHeader.getRepetition_level_encoding()),
					CONVERTER.getEncoding(dataHeader.getDefinition_level_encoding()),
					CONVERTER.getEncoding(dataHeader.getEncoding()));
		} else if (header.getType() == PageType.DATA_PAGE_V2) {
			DataPageHeaderV2 dataHeader = header.getData_page_header_v2();
			int repetitionLength = dataHeader.getRepetition_levels_byte_length();
			int definitionLength = dataHeader.getDefinition_levels_byte_length();
			int levelsLength = repetitionLength + definitionLength;
			int dataStart = start + levelsLength;
			BytesInput data = dataHeader.isIs_compressed()
					? decompress(codec, bytes, dataStart, compressedSize - levelsLength, uncompressedSize - levelsLength)
					: BytesInput.from(bytes, dataStart, compressedSize - levelsLength);
			return DataPageV2.uncompressed(dataHeader.getNum_rows(), dataHeader.getNum_nulls(), dataHeader.getNum_values(),
					BytesInput.from(bytes, start, repetitionLength), BytesInput.from(bytes, start + repetitionLength, definitionLength),
					CONVERTER.getEncoding(dataHeader.getEncoding()), data, null);
		}
		throw new IOException("Unexpected page type " + header.getType() + " in " + location);
	}

	private BytesInput decompress(CompressionCodecName codec, byte[] bytes, int offset, int length, int uncompressedSize)
			throws IOException {
		if (codec == CompressionCodecName.UNCOMPRESSED) {
			return BytesInput.from(bytes, offset, length);
		}
		if (codec == CompressionCodecName.ZSTD) {
			// directly, without the stream that Parquet's codec creates per page
			byte[] uncompressed = new byte[uncompressedSize];
			long size = Zstd.decompressByteArray(uncompressed, 0, uncompressedSize, bytes, offset, length);
			if (Zstd.isError(size) || size != uncompressedSize) {
				throw new IOException("Unable to decompress page of " + location + ": "
						+ (Zstd.isError(size) ? Zstd.getErrorName(size) : size + " instead of " + uncompressedSize + " bytes"));
			}
			return BytesInput.from(uncompressed);
		}
		if (codecs == null) {
			codecs = new CodecFactory(new Configuration(false), 0);
		}
		// The result is read lazily from a stream of the decompressor, which the next page reuses.
		BytesInput lazy = codecs.getDecompressor(codec).decompress(BytesInput.from(bytes, offset, length), uncompressedSize);
		return BytesInput.from(lazy.toByteArray());
	}

	private DataFileHandles.Handle handle() throws IOException {
		if (handle == null) {
			handle = caches.handles.acquire(location);
		}
		return handle;
	}

	/**
	 * Returns the page indexes of a row group, from the cache if possible.
	 */
	private ColumnIndexStore columnIndexes(BlockMetaData block) throws IOException {
		try {
			return caches.columnIndexes.get(location, ConcurrentHashMap::new)
					.computeIfAbsent(block.getStartingPos(), ignored -> {
						try {
							return readColumnIndexes(block);
						} catch (IOException e) {
							throw new UncheckedIOException(e);
						}
					});
		} catch (ExecutionException e) {
			throw new IOException(e.getCause());
		} catch (UncheckedIOException e) {
			throw e.getCause();
		}
	}

	/**
	 * Reads the column and offset indexes of all columns of a row group. A missing offset
	 * index is built from the page headers, since reading pages requires their locations.
	 */
	private ColumnIndexStore readColumnIndexes(BlockMetaData block) throws IOException {
		if (indexReader == null) {
			InputFile inputFile = caches.inputFiles.inputFile(location);
			indexReader = ParquetFileReader.open(inputFile, footer, options, inputFile.newStream());
		}
		Map<ColumnPath, ColumnIndex> columnIndexes = new HashMap<>();
		Map<ColumnPath, OffsetIndex> offsetIndexes = new HashMap<>();
		for (ColumnChunkMetaData column : block.getColumns()) {
			ColumnIndex columnIndex = indexReader.readColumnIndex(column);
			if (columnIndex != null) {
				columnIndexes.put(column.getPath(), columnIndex);
			}
			OffsetIndex offsetIndex = indexReader.readOffsetIndex(column);
			offsetIndexes.put(column.getPath(), offsetIndex != null ? offsetIndex : scanOffsetIndex(column));
		}
		return new PageRanges.RowGroupIndexes(new ColumnIndexStore() {
			@Override
			public ColumnIndex getColumnIndex(ColumnPath column) {
				return columnIndexes.get(column);
			}

			@Override
			public OffsetIndex getOffsetIndex(ColumnPath column) throws MissingOffsetIndexException {
				OffsetIndex offsetIndex = offsetIndexes.get(column);
				if (offsetIndex == null) {
					throw new MissingOffsetIndexException(column);
				}
				return offsetIndex;
			}
		}, block.getRowCount());
	}

	/**
	 * Builds the offset index of a column chunk by reading its page headers.
	 */
	OffsetIndex scanOffsetIndex(ColumnChunkMetaData chunk) throws IOException {
		byte[] bytes = new byte[Math.toIntExact(chunk.getTotalSize())];
		handle().read(List.of(new DataFileHandles.Range(chunk.getStartingPos(), bytes)));
		List<long[]> pages = new ArrayList<>();
		long firstRow = 0;
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		while (in.available() > 0) {
			int start = bytes.length - in.available();
			PageHeader header = Util.readPageHeader(in);
			int size = bytes.length - in.available() - start + header.getCompressed_page_size();
			in.skipNBytes(header.getCompressed_page_size());
			if (header.getType() == PageType.DATA_PAGE) {
				pages.add(new long[]{chunk.getStartingPos() + start, size, firstRow});
				// flat columns have one value per row
				firstRow += header.getData_page_header().getNum_values();
			} else if (header.getType() == PageType.DATA_PAGE_V2) {
				pages.add(new long[]{chunk.getStartingPos() + start, size, firstRow});
				firstRow += header.getData_page_header_v2().getNum_rows();
			}
		}
		return new OffsetIndex() {
			@Override
			public int getPageCount() {
				return pages.size();
			}

			@Override
			public long getOffset(int pageIndex) {
				return pages.get(pageIndex)[0];
			}

			@Override
			public int getCompressedPageSize(int pageIndex) {
				return (int) pages.get(pageIndex)[1];
			}

			@Override
			public long getFirstRowIndex(int pageIndex) {
				return pages.get(pageIndex)[2];
			}
		};
	}

	private boolean isCompleted(long context, long item, long property) {
		if (!completed) {
			return false;
		}
		int order = Long.compare(context, completedContext);
		if (order == 0) {
			order = Long.compare(item, completedItem);
		}
		if (order == 0) {
			order = Long.compare(property, completedProperty);
		}
		return order <= 0;
	}

	/**
	 * Moves to the next row that may match, in the current or a later row group.
	 */
	private boolean nextRow() throws IOException {
		while (row > lastRow) {
			if ((rangeIndex += 2) < ranges.length) {
				row = ranges[rangeIndex];
				lastRow = ranges[rangeIndex + 1];
			} else if (!nextGroup()) {
				return false;
			}
		}
		return true;
	}

	@Override
	public boolean hasNext() {
		if (next != null) {
			return true;
		}
		if (closed) {
			return false;
		}
		try {
			while (nextRow()) {
				long r = row++;
				rowsScanned++;
				long itemId = page(0, r).getLong(r);
				long contextId = page(1, r).getLong(r);
				long propertyId = page(2, r).getLong(r);
				long time = page(3, r).getLong(r);
				int seqNr = page(4, r).getInt(r);
				if (isCompleted(contextId, itemId, propertyId)
						|| !keyFilter.matches(itemId, contextId, propertyId, time, seqNr)) {
					if (series != null) {
						seek(contextId, itemId, propertyId);
					}
					continue;
				}
				// a row has at most one value; keep the first, as before
				Object value = null;
				for (int i = FIRST_VALUE_COLUMN; i < readColumns.length && value == null; i++) {
					if (readColumns[i]) {
						valuesRead++;
						value = page(i, r).get(r);
					}
				}
				next = new KvinRow(contextId, itemId, propertyId, time, seqNr, value);
				return true;
			}
			close();
			return false;
		} catch (IOException e) {
			throw new UncheckedIOException("Unable to read Iceberg series", e);
		}
	}

	/**
	 * Moves to the first row of the next series that may match, after a row of a sorted file
	 * that does not match. Rows of the same series that do not match, for example because of
	 * their time, are looked at one by one.
	 */
	private void seek(long context, long item, long property) throws IOException {
		long[] from = {context, item, property};
		if (completed && compare(from, completedContext, completedItem, completedProperty) <= 0) {
			// the series after the completed one
			from = new long[]{completedContext, completedItem, completedProperty == Long.MAX_VALUE
					? Long.MAX_VALUE : completedProperty + 1};
		}
		long[] target = series.next(from[0], from[1], from[2]);
		if (target == null) {
			// no later row of the file can match
			groupIndex = footer.getBlocks().size();
			rangeIndex = ranges.length;
			row = lastRow + 1;
			return;
		}
		if (compare(target, context, item, property) <= 0) {
			// the current series may still match
			return;
		}
		// the first row of the current range whose series is not before the target
		long low = row;
		long high = lastRow + 1;
		while (low < high) {
			long middle = (low + high) >>> 1;
			if (compare(target, page(1, middle).getLong(middle), page(0, middle).getLong(middle),
					page(2, middle).getLong(middle)) > 0) {
				low = middle + 1;
			} else {
				high = middle;
			}
		}
		row = low;
	}

	private static int compare(long[] key, long context, long item, long property) {
		int order = Long.compare(key[0], context);
		if (order == 0) {
			order = Long.compare(key[1], item);
		}
		return order != 0 ? order : Long.compare(key[2], property);
	}

	@Override
	public KvinRow next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}
		KvinRow result = next;
		next = null;
		return result;
	}

	@Override
	public void close() throws IOException {
		if (!closed) {
			closed = true;
			Arrays.fill(current, null);
			groupPages.clear();
			try {
				if (handle != null) {
					handle.close();
				}
				if (codecs != null) {
					codecs.release();
				}
			} finally {
				try {
					if (indexReader != null) {
						indexReader.close();
					}
				} finally {
					if (ownsCaches) {
						caches.close();
					}
				}
			}
		}
	}
}
