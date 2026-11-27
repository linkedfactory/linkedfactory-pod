package io.github.linkedfactory.core.kvin.iceberg;

import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Computes the rows of a row group that may match a fetch filter, from the minimum and
 * maximum values of the pages of the key columns.
 *
 * <p>Rows are returned as sorted, disjoint and inclusive ranges in one array:
 * {@code [from0, to0, from1, to1, ...]}. The result may contain more rows than match, but
 * never fewer: and, or, comparisons and in on the key columns restrict the rows, everything
 * else counts as all rows. Unlike Parquet's column index filter, the page values are decoded
 * once per row group into arrays, so that computing the rows of a fetch neither boxes values
 * nor builds sets of page indexes.</p>
 */
final class PageRanges {
	static final long[] NONE = {};

	private PageRanges() {
	}

	static long[] all(long rowCount) {
		return rowCount == 0 ? NONE : new long[]{0, rowCount - 1};
	}

	static long rowCount(long[] ranges) {
		long count = 0;
		for (int i = 0; i < ranges.length; i += 2) {
			count += ranges[i + 1] - ranges[i] + 1;
		}
		return count;
	}

	/**
	 * The page indexes of a row group, with the page values of the key columns decoded on
	 * first use. Instances are cached and shared by threads.
	 */
	static final class RowGroupIndexes implements ColumnIndexStore {
		private final ColumnIndexStore indexes;
		private final long rowCount;
		private final Map<String, KeyPages> keyPages = new ConcurrentHashMap<>();

		RowGroupIndexes(ColumnIndexStore indexes, long rowCount) {
			this.indexes = indexes;
			this.rowCount = rowCount;
		}

		@Override
		public ColumnIndex getColumnIndex(ColumnPath column) {
			return indexes.getColumnIndex(column);
		}

		@Override
		public OffsetIndex getOffsetIndex(ColumnPath column) throws MissingOffsetIndexException {
			return indexes.getOffsetIndex(column);
		}

		/**
		 * Returns the decoded pages of a long or int key column, or {@code null} if the column
		 * has no column index.
		 */
		KeyPages keyPages(String column) {
			KeyPages pages = keyPages.get(column);
			if (pages == null) {
				ColumnPath path = ColumnPath.get(column);
				ColumnIndex index = indexes.getColumnIndex(path);
				if (index == null) {
					return null;
				}
				OffsetIndex offsets = indexes.getOffsetIndex(path);
				pages = keyPages.computeIfAbsent(column, ignored -> new KeyPages(index, offsets, rowCount));
			}
			return pages;
		}
	}

	/**
	 * The rows, minimum and maximum values of the pages of a key column.
	 */
	static final class KeyPages {
		final long[] firstRows;
		final long[] lastRows;
		final long[] minimums;
		final long[] maximums;
		// pages without values, which match no comparison
		final boolean[] empty;

		KeyPages(ColumnIndex index, OffsetIndex offsets, long rowCount) {
			int count = offsets.getPageCount();
			firstRows = new long[count];
			lastRows = new long[count];
			minimums = new long[count];
			maximums = new long[count];
			empty = new boolean[count];
			List<Boolean> nullPages = index.getNullPages();
			List<ByteBuffer> minValues = index.getMinValues();
			List<ByteBuffer> maxValues = index.getMaxValues();
			for (int page = 0; page < count; page++) {
				firstRows[page] = offsets.getFirstRowIndex(page);
				lastRows[page] = page + 1 < count ? offsets.getFirstRowIndex(page + 1) - 1 : rowCount - 1;
				empty[page] = nullPages.get(page);
				if (!empty[page]) {
					minimums[page] = decode(minValues.get(page));
					maximums[page] = decode(maxValues.get(page));
				}
			}
		}

		private static long decode(ByteBuffer value) {
			ByteBuffer buffer = value.duplicate().order(ByteOrder.LITTLE_ENDIAN);
			return buffer.remaining() >= 8 ? buffer.getLong(buffer.position()) : buffer.getInt(buffer.position());
		}
	}

	/**
	 * Returns the rows of a row group that may match the filter.
	 */
	static long[] compute(Expression filter, RowGroupIndexes indexes, long rowCount) {
		switch (filter.op()) {
			case TRUE:
				return all(rowCount);
			case FALSE:
				return NONE;
			case AND:
				return intersect(compute(((And) filter).left(), indexes, rowCount),
						compute(((And) filter).right(), indexes, rowCount));
			case OR:
				return union(compute(((Or) filter).left(), indexes, rowCount),
						compute(((Or) filter).right(), indexes, rowCount));
			default:
				return filter instanceof UnboundPredicate<?> predicate ? compute(predicate, indexes, rowCount) : all(rowCount);
		}
	}

	private static long[] compute(UnboundPredicate<?> predicate, RowGroupIndexes indexes, long rowCount) {
		String column = predicate.ref().name();
		if (!Arrays.asList(KeyFilter.KEY_COLUMNS).contains(column)) {
			return all(rowCount);
		}
		switch (predicate.op()) {
			case IS_NULL:
				// key columns are required
				return NONE;
			case EQ, LT, LT_EQ, GT, GT_EQ, IN:
				break;
			default:
				return all(rowCount);
		}
		KeyPages pages = indexes.keyPages(column);
		if (pages == null) {
			return all(rowCount);
		}
		long[] values;
		if (predicate.op() == org.apache.iceberg.expressions.Expression.Operation.IN) {
			List<? extends Literal<?>> literals = predicate.literals();
			values = new long[literals.size()];
			for (int i = 0; i < values.length; i++) {
				values[i] = ((Number) literals.get(i).value()).longValue();
			}
			Arrays.sort(values);
		} else {
			values = new long[]{((Number) predicate.literal().value()).longValue()};
		}
		long[] ranges = new long[16];
		int size = 0;
		for (int page = 0; page < pages.firstRows.length; page++) {
			if (pages.empty[page] || !matches(predicate.op(), values, pages.minimums[page], pages.maximums[page])) {
				continue;
			}
			long from = pages.firstRows[page];
			long to = pages.lastRows[page];
			if (size > 0 && ranges[size - 1] + 1 >= from) {
				ranges[size - 1] = Math.max(ranges[size - 1], to);
			} else {
				if (size == ranges.length) {
					ranges = Arrays.copyOf(ranges, size * 2);
				}
				ranges[size++] = from;
				ranges[size++] = to;
			}
		}
		return Arrays.copyOf(ranges, size);
	}

	private static boolean matches(Expression.Operation op, long[] values, long min, long max) {
		long value = values[0];
		return switch (op) {
			case EQ -> min <= value && value <= max;
			case LT -> min < value;
			case LT_EQ -> min <= value;
			case GT -> max > value;
			case GT_EQ -> max >= value;
			case IN -> {
				// the smallest value that is not below the minimum
				int i = Arrays.binarySearch(values, min);
				if (i < 0) {
					i = -i - 1;
				}
				yield i < values.length && values[i] <= max;
			}
			default -> true;
		};
	}

	static long[] intersect(long[] a, long[] b) {
		long[] result = new long[a.length + b.length];
		int size = 0;
		int i = 0;
		int j = 0;
		while (i < a.length && j < b.length) {
			long from = Math.max(a[i], b[j]);
			long to = Math.min(a[i + 1], b[j + 1]);
			if (from <= to) {
				result[size++] = from;
				result[size++] = to;
			}
			if (a[i + 1] < b[j + 1]) {
				i += 2;
			} else {
				j += 2;
			}
		}
		return Arrays.copyOf(result, size);
	}

	static long[] union(long[] a, long[] b) {
		long[] result = new long[a.length + b.length];
		int size = 0;
		int i = 0;
		int j = 0;
		while (i < a.length || j < b.length) {
			long from;
			long to;
			if (j >= b.length || i < a.length && a[i] <= b[j]) {
				from = a[i];
				to = a[i + 1];
				i += 2;
			} else {
				from = b[j];
				to = b[j + 1];
				j += 2;
			}
			if (size > 0 && result[size - 1] + 1 >= from) {
				result[size - 1] = Math.max(result[size - 1], to);
			} else {
				result[size++] = from;
				result[size++] = to;
			}
		}
		return Arrays.copyOf(result, size);
	}
}
