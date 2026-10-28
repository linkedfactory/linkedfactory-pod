package io.github.linkedfactory.core.kvin.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.parquet.ParquetMetricsRowGroupFilter;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

/**
 * Flat KVIN reader that skips value decoding for completed series and prunes
 * subsequent row groups using the advancing series boundary.
 */
final class SeriesSkippingReader implements CloseableIterable<Record>, CloseableIterator<Record> {
	private final Schema schema;
	private final Expression filter;
	private final Evaluator evaluator;
	private final ParquetFileReader reader;
	private final MessageType fileSchema;
	private final GenericRecord keys;
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
	private Record next;

	SeriesSkippingReader(Schema schema, FileScanTask task, Expression filter) throws IOException {
		this.schema = schema;
		this.filter = filter;
		this.evaluator = new Evaluator(schema.asStruct(), filter);
		this.keys = GenericRecord.create(schema);
		this.groupFilter = new ParquetMetricsRowGroupFilter(schema, filter);
		var path = new org.apache.hadoop.fs.Path(task.file().location());
		var conf = new Configuration();
		this.reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf),
				HadoopReadOptions.builder(conf, path).withRange(task.start(), task.start() + task.length()).build());
		this.fileSchema = reader.getFileMetaData().getSchema();
	}

	void skipThrough(long item, long context, long property) {
		completed = true;
		completedItem = item;
		completedContext = context;
		completedProperty = property;
		groupFilterDirty = true;
	}

	@Override
	public CloseableIterator<Record> iterator() {
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
			var group = reader.getRowGroups().get(groupIndex++);
			if (!groupFilter.shouldRead(fileSchema, group)) {
				reader.skipNextRowGroup();
				continue;
			}
			pages = reader.readNextRowGroup();
			var store = new ColumnReadStoreImpl(pages,
					new GroupRecordConverter(fileSchema).getRootConverter(), fileSchema,
					reader.getFileMetaData().getCreatedBy());
			columns = new ColumnReader[schema.columns().size()];
			for (int i = 0; i < columns.length; i++) {
				columns[i] = store.getColumnReader(fileSchema.getColumnDescription(
						new String[]{schema.columns().get(i).name()}));
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
				for (int i = 0; i < 5; i++) {
					keys.set(i, i == 4 ? (Object) columns[i].getInteger() : columns[i].getLong());
					columns[i].consume();
				}
				boolean keep = !isCompleted((Long) keys.get(0), (Long) keys.get(1), (Long) keys.get(2))
						&& evaluator.eval(keys);
				GenericRecord record = keep ? keys.copy() : null;
				for (int i = 5; i < columns.length; i++) {
					ColumnReader column = columns[i];
					boolean present = column.getCurrentDefinitionLevel() == column.getDescriptor().getMaxDefinitionLevel();
					if (present) {
						if (keep) {
							record.set(i, switch (i) {
								case 5, 11 -> column.getBoolean();
								case 6 -> column.getInteger();
								case 7 -> column.getLong();
								case 8 -> column.getFloat();
								case 9 -> column.getDouble();
								case 10 -> column.getBinary().toStringUsingUTF8();
								case 12 -> ByteBuffer.wrap(column.getBinary().getBytes());
								default -> throw new IllegalStateException("Unexpected KVIN column: " + i);
							});
						} else {
							column.skip();
						}
					}
					column.consume();
				}
				if (keep) {
					next = record;
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
	public Record next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}
		Record result = next;
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
