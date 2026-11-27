package io.github.linkedfactory.core.kvin.iceberg;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * The values of one data page of a column, decoded into an array with one element per row.
 *
 * <p>Pages are immutable once decoded, so several readers can use a cached page at once.
 * Values are stored as primitive arrays; strings as {@code String[]} and other binary values
 * as {@code byte[][]}.</p>
 */
final class DecodedPage {
	private static final PrimitiveConverter NO_CONVERTER = new PrimitiveConverter() {
	};

	// first row of the page within its row group
	final long firstRow;
	final int rowCount;
	// rows without a value, or null if every row has one
	private final BitSet nulls;
	private final Object values;
	// estimated memory use in bytes
	final int weight;

	private DecodedPage(long firstRow, int rowCount, BitSet nulls, Object values, int weight) {
		this.firstRow = firstRow;
		this.rowCount = rowCount;
		this.nulls = nulls;
		this.values = values;
		this.weight = weight;
	}

	boolean contains(long row) {
		return row >= firstRow && row < firstRow + rowCount;
	}

	boolean isNull(long row) {
		return nulls != null && nulls.get((int) (row - firstRow));
	}

	long getLong(long row) {
		return ((long[]) values)[(int) (row - firstRow)];
	}

	int getInt(long row) {
		return ((int[]) values)[(int) (row - firstRow)];
	}

	/**
	 * Returns the value of a row as the Java type of the column, or {@code null}.
	 * Binary values that are not strings are returned as a new {@link ByteBuffer}.
	 */
	Object get(long row) {
		if (isNull(row)) {
			return null;
		}
		int i = (int) (row - firstRow);
		if (values instanceof long[] longs) {
			return longs[i];
		} else if (values instanceof int[] ints) {
			return ints[i];
		} else if (values instanceof double[] doubles) {
			return doubles[i];
		} else if (values instanceof float[] floats) {
			return floats[i];
		} else if (values instanceof boolean[] booleans) {
			return booleans[i];
		} else if (values instanceof String[] strings) {
			return strings[i];
		}
		return ByteBuffer.wrap(((byte[][]) values)[i]);
	}

	/**
	 * Decodes a data page of a flat column.
	 *
	 * @param firstRow   the first row of the page within its row group
	 * @param dictionary the dictionary page of the column chunk, or {@code null}
	 */
	static DecodedPage decode(ColumnDescriptor column, DataPage page, DictionaryPage dictionary,
			org.apache.parquet.VersionParser.ParsedVersion writerVersion, long firstRow) {
		ColumnReaderImpl reader = new ColumnReaderImpl(column, new PageReader() {
			boolean read;

			@Override
			public DictionaryPage readDictionaryPage() {
				return dictionary;
			}

			@Override
			public long getTotalValueCount() {
				return page.getValueCount();
			}

			@Override
			public DataPage readPage() {
				if (read) {
					return null;
				}
				read = true;
				return page;
			}
		}, NO_CONVERTER, writerVersion);
		int rows = page.getValueCount();
		int maxLevel = column.getMaxDefinitionLevel();
		BitSet nulls = null;
		PrimitiveType type = column.getPrimitiveType();
		Object values;
		long weight = 64;
		switch (type.getPrimitiveTypeName()) {
			case INT64 -> {
				long[] longs = new long[rows];
				for (int i = 0; i < rows; i++) {
					if (reader.getCurrentDefinitionLevel() == maxLevel) {
						longs[i] = reader.getLong();
					} else {
						nulls = setNull(nulls, i);
					}
					reader.consume();
				}
				values = longs;
				weight += 8L * rows;
			}
			case INT32 -> {
				int[] ints = new int[rows];
				for (int i = 0; i < rows; i++) {
					if (reader.getCurrentDefinitionLevel() == maxLevel) {
						ints[i] = reader.getInteger();
					} else {
						nulls = setNull(nulls, i);
					}
					reader.consume();
				}
				values = ints;
				weight += 4L * rows;
			}
			case DOUBLE -> {
				double[] doubles = new double[rows];
				for (int i = 0; i < rows; i++) {
					if (reader.getCurrentDefinitionLevel() == maxLevel) {
						doubles[i] = reader.getDouble();
					} else {
						nulls = setNull(nulls, i);
					}
					reader.consume();
				}
				values = doubles;
				weight += 8L * rows;
			}
			case FLOAT -> {
				float[] floats = new float[rows];
				for (int i = 0; i < rows; i++) {
					if (reader.getCurrentDefinitionLevel() == maxLevel) {
						floats[i] = reader.getFloat();
					} else {
						nulls = setNull(nulls, i);
					}
					reader.consume();
				}
				values = floats;
				weight += 4L * rows;
			}
			case BOOLEAN -> {
				boolean[] booleans = new boolean[rows];
				for (int i = 0; i < rows; i++) {
					if (reader.getCurrentDefinitionLevel() == maxLevel) {
						booleans[i] = reader.getBoolean();
					} else {
						nulls = setNull(nulls, i);
					}
					reader.consume();
				}
				values = booleans;
				weight += rows;
			}
			case BINARY, FIXED_LEN_BYTE_ARRAY -> {
				boolean string = type.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation;
				Object[] binaries = string ? new String[rows] : new byte[rows][];
				for (int i = 0; i < rows; i++) {
					if (reader.getCurrentDefinitionLevel() == maxLevel) {
						var binary = reader.getBinary();
						binaries[i] = string ? binary.toStringUsingUTF8() : binary.getBytes();
						// characters of strings take up to two bytes
						weight += 24 + (string ? 2L : 1L) * binary.length();
					} else {
						nulls = setNull(nulls, i);
					}
					reader.consume();
				}
				values = binaries;
				weight += 8L * rows;
			}
			default -> throw new IllegalStateException("Unsupported column type " + type);
		}
		if (nulls != null) {
			weight += rows / 8;
		}
		return new DecodedPage(firstRow, rows, nulls, values, (int) Math.min(Integer.MAX_VALUE, weight));
	}

	private static BitSet setNull(BitSet nulls, int row) {
		if (nulls == null) {
			nulls = new BitSet();
		}
		nulls.set(row);
		return nulls;
	}
}
