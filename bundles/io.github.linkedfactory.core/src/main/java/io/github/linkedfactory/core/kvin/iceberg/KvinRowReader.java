package io.github.linkedfactory.core.kvin.iceberg;

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.parquet.BaseParquetReaders;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.parquet.ParquetValueReaders;
import org.apache.iceberg.types.Types;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.List;

record KvinRow(long itemId, long contextId, long propertyId, long time, int seqNr, Object value) {
}

final class KvinRowReader extends BaseParquetReaders<KvinRow> {
	static ParquetValueReader<KvinRow> buildReader(Schema schema, MessageType fileSchema) {
		return new KvinRowReader().createReader(schema, fileSchema);
	}

	@Override
	protected ParquetValueReader<KvinRow> createStructReader(List<Type> types, List<ParquetValueReader<?>> readers, Types.StructType struct) {
		return new ParquetValueReaders.StructReader<KvinRow, RowData>(readers) {
			private final RowData data = new RowData();

			@Override
			protected RowData newStructData(KvinRow reuse) {
				data.value = null;
				return data;
			}

			@Override
			protected Object getField(RowData row, int pos) {
				return null;
			}

			@Override
			protected void set(RowData row, int pos, Object value) {
				switch (pos) {
					case 0 -> row.itemId = (Long) value;
					case 1 -> row.contextId = (Long) value;
					case 2 -> row.propertyId = (Long) value;
					case 3 -> row.time = (Long) value;
					case 4 -> row.seqNr = (Integer) value;
					case 5 -> { } // first is not needed for reads
					default -> {
						if (row.value == null && value != null) row.value = value;
					}
				}
			}

			@Override
			protected KvinRow buildStruct(RowData row) {
				return new KvinRow(row.itemId, row.contextId, row.propertyId, row.time, row.seqNr, row.value);
			}
		};
	}

	private static final class RowData {
		long itemId;
		long contextId;
		long propertyId;
		long time;
		int seqNr;
		Object value;
	}
}
