package io.github.linkedfactory.core.kvin.iceberg;

import com.google.common.cache.Cache;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.parquet.ParquetMetricsRowGroupFilter;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * Flat KVIN reader that skips value decoding for completed series and prunes
 * subsequent row groups using the advancing series boundary. Within a row group,
 * only the pages that may match the filter are read (via Parquet's column index).
 * Column indexes can be cached per file like footers.
 *
 * <p>File footers are taken from a shared cache keyed by file location (data
 * files are immutable). Because a reader opened with a complete footer sees
 * all row groups, the task's split range is applied here, using the same
 * midpoint rule as Parquet's range metadata filter.</p>
 */
final class SeriesSkippingReader implements CloseableIterable<KvinRow>, CloseableIterator<KvinRow> {
	// Indexes in the KVIN schema: the series start marker and the first optional
	// value column (valueInt). Columns 0-4 are the key columns.
	private static final int FIRST_COLUMN = 5;
	private static final int FIRST_VALUE_COLUMN = 6;
	// Creating a Configuration per reader re-parses Hadoop's default resources.
	private static final Configuration CONF = new Configuration();

	private final Schema schema;
	private final Expression filter;
	private final KeyFilter keyFilter;
	private final ParquetFileReader reader;
	private final MessageType fileSchema;
	private final long rangeStart;
	private final long rangeEnd;
	private ParquetMetricsRowGroupFilter groupFilter;
	private PageReadStore pages;
	private ColumnReader[] columns;
	private long remaining;
	private int groupIndex;
	private long completedItem;
	private long completedContext;
	private long completedProperty;
	private boolean completed;
	private boolean groupFilterDirty;
	private boolean closed;
	private KvinRow next;

	SeriesSkippingReader(Schema schema, FileScanTask task, Expression filter,
			Cache<String, ParquetMetadata> footers) throws IOException {
		this(schema, task, filter, footers, null);
	}

	/**
	 * @param columnIndexes optional cache of page indexes, keyed by file location
	 *                      and row group start position; {@code null} reads them per reader
	 */
	SeriesSkippingReader(Schema schema, FileScanTask task, Expression filter,
			Cache<String, ParquetMetadata> footers,
			Cache<String, Map<Long, ColumnIndexStore>> columnIndexes) throws IOException {
		this.schema = schema;
		this.filter = filter;
		this.keyFilter = KeyFilter.of(schema, filter);
		this.groupFilter = new ParquetMetricsRowGroupFilter(schema, filter);
		this.rangeStart = task.start();
		this.rangeEnd = task.start() + task.length();
		String location = task.file().location();
		var path = new org.apache.hadoop.fs.Path(location);
		HadoopInputFile inputFile = HadoopInputFile.fromPath(path, CONF);
		// The record filter lets readFilteredRowGroup skip pages via the column index.
		// Dictionary and bloom filters would read extra data on every open.
		ParquetReadOptions.Builder optionsBuilder = HadoopReadOptions.builder(CONF, path)
				.useDictionaryFilter(false).useBloomFilter(false);
		FilterPredicate pageFilter = ParquetPageFilters.convert(schema, filter);
		if (pageFilter != null) {
			optionsBuilder.withRecordFilter(FilterCompat.get(pageFilter));
		}
		ParquetReadOptions options = optionsBuilder.build();
		ParquetMetadata footer = footers.getIfPresent(location);
		if (footer == null) {
			try (ParquetFileReader footerReader = ParquetFileReader.open(inputFile, options)) {
				footer = footerReader.getFooter();
			}
			footers.put(location, footer);
		}
		SeekableInputStream stream = inputFile.newStream();
		try {
			this.reader = columnIndexes == null ? ParquetFileReader.open(inputFile, footer, options, stream)
					: new ParquetFileReader(inputFile, footer, options, stream) {
				@Override
				public ColumnIndexStore getColumnIndexStore(int blockIndex) {
					BlockMetaData block = getRowGroups().get(blockIndex);
					try {
						return columnIndexes.get(location, ConcurrentHashMap::new)
								.computeIfAbsent(block.getStartingPos(), ignored -> readColumnIndexes(this, block));
					} catch (ExecutionException e) {
						throw new UncheckedIOException(new IOException(e.getCause()));
					}
				}
			};
		} catch (IOException | RuntimeException e) {
			stream.close();
			throw e;
		}
		this.fileSchema = reader.getFileMetaData().getSchema();
	}

	/**
	 * Reads the column and offset indexes of all columns of a row group. Unlike
	 * Parquet's own store, the result does not read lazily through the reader and
	 * can be cached beyond the reader's lifetime.
	 */
	private static ColumnIndexStore readColumnIndexes(ParquetFileReader reader, BlockMetaData block) {
		Map<ColumnPath, ColumnIndex> columnIndexes = new HashMap<>();
		Map<ColumnPath, OffsetIndex> offsetIndexes = new HashMap<>();
		boolean complete = true;
		try {
			for (ColumnChunkMetaData column : block.getColumns()) {
				ColumnIndex columnIndex = reader.readColumnIndex(column);
				if (columnIndex != null) {
					columnIndexes.put(column.getPath(), columnIndex);
				}
				OffsetIndex offsetIndex = reader.readOffsetIndex(column);
				if (offsetIndex == null) {
					complete = false;
				} else {
					offsetIndexes.put(column.getPath(), offsetIndex);
				}
			}
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		// like Parquet: without all offset indexes, pages cannot be skipped
		boolean skipPages = complete;
		return new ColumnIndexStore() {
			@Override
			public ColumnIndex getColumnIndex(ColumnPath column) {
				return skipPages ? columnIndexes.get(column) : null;
			}

			@Override
			public OffsetIndex getOffsetIndex(ColumnPath column) throws MissingOffsetIndexException {
				OffsetIndex offsetIndex = skipPages ? offsetIndexes.get(column) : null;
				if (offsetIndex == null) {
					throw new MissingOffsetIndexException(column);
				}
				return offsetIndex;
			}
		};
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

	void skipThrough(long item, long context, long property) {
		completed = true;
		completedItem = item;
		completedContext = context;
		completedProperty = property;
		groupFilterDirty = true;
	}

	@Override
	public CloseableIterator<KvinRow> iterator() {
		return this;
	}

	private boolean nextGroup() throws IOException {
		if (pages != null) {
			pages.close();
			pages = null;
		}
		if (groupFilterDirty) {
			Expression after = Expressions.or(Expressions.greaterThan("itemId", completedItem),
					Expressions.and(Expressions.equal("itemId", completedItem),
							Expressions.or(Expressions.greaterThan("contextId", completedContext),
									Expressions.and(Expressions.equal("contextId", completedContext),
											Expressions.greaterThan("propertyId", completedProperty)))));
			groupFilter = new ParquetMetricsRowGroupFilter(schema, Expressions.and(filter, after));
			groupFilterDirty = false;
		}
		while (groupIndex < reader.getRowGroups().size()) {
			int index = groupIndex++;
			var group = reader.getRowGroups().get(index);
			if (!inRange(group) || !groupFilter.shouldRead(fileSchema, group)) {
				continue;
			}
			// reads only the pages that may match, or null if none does
			pages = reader.readFilteredRowGroup(index);
			if (pages == null) {
				continue;
			}
			var store = new ColumnReadStoreImpl(pages,
					new GroupRecordConverter(fileSchema).getRootConverter(), fileSchema,
					reader.getFileMetaData().getCreatedBy());
			columns = new ColumnReader[schema.columns().size()];
			Map<String, ColumnChunkMetaData> chunks = new HashMap<>();
			for (ColumnChunkMetaData chunk : group.getColumns()) {
				chunks.put(chunk.getPath().toDotString(), chunk);
			}
			for (int i = 0; i < columns.length; i++) {
				String name = schema.columns().get(i).name();
				// fetches do not use the series start marker; value columns without
				// any value in this row group are not read at all
				if (i == FIRST_COLUMN || i >= FIRST_VALUE_COLUMN && allNull(chunks.get(name), group.getRowCount())) {
					continue;
				}
				columns[i] = store.getColumnReader(fileSchema.getColumnDescription(new String[]{name}));
			}
			remaining = pages.getRowCount();
			if (remaining > 0) {
				return true;
			}
		}
		return false;
	}

	private boolean isCompleted(long item, long context, long property) {
		if (!completed) {
			return false;
		}
		int order = Long.compare(item, completedItem);
		if (order == 0) {
			order = Long.compare(context, completedContext);
		}
		if (order == 0) {
			order = Long.compare(property, completedProperty);
		}
		return order <= 0;
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
			while (remaining > 0 || nextGroup()) {
				remaining--;
				long itemId = columns[0].getLong();
				columns[0].consume();
				long contextId = columns[1].getLong();
				columns[1].consume();
				long propertyId = columns[2].getLong();
				columns[2].consume();
				long time = columns[3].getLong();
				columns[3].consume();
				int seqNr = columns[4].getInteger();
				columns[4].consume();
				boolean keep = !isCompleted(itemId, contextId, propertyId)
						&& keyFilter.matches(itemId, contextId, propertyId, time, seqNr);
				Object value = null;
				for (int i = FIRST_VALUE_COLUMN; i < columns.length; i++) {
					ColumnReader column = columns[i];
					if (column == null) {
						continue;
					}
					boolean present = column.getCurrentDefinitionLevel() == column.getDescriptor().getMaxDefinitionLevel();
					if (present) {
						// a row has at most one value; keep the first, as before
						if (keep && value == null) {
							value = switch (i) {
								case 6 -> column.getInteger();
								case 7 -> column.getLong();
								case 8 -> column.getFloat();
								case 9 -> column.getDouble();
								case 10 -> column.getBinary().toStringUsingUTF8();
								case 11 -> column.getBoolean();
								case 12 -> ByteBuffer.wrap(column.getBinary().getBytes());
								default -> throw new IllegalStateException("Unexpected KVIN column: " + i);
							};
						} else {
							column.skip();
						}
					}
					column.consume();
				}
				if (keep) {
					next = new KvinRow(itemId, contextId, propertyId, time, seqNr, value);
					return true;
				}
			}
			close();
			return false;
		} catch (IOException e) {
			throw new UncheckedIOException("Unable to read Iceberg series", e);
		}
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
			try {
				if (pages != null) {
					pages.close();
				}
			} finally {
				reader.close();
			}
		}
	}
}
