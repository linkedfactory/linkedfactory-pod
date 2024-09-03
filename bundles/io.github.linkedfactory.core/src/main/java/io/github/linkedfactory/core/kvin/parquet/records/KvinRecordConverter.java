package io.github.linkedfactory.core.kvin.parquet.records;

import io.github.linkedfactory.core.kvin.records.KvinRecord;
import org.apache.parquet.io.api.*;

import java.nio.charset.StandardCharsets;

public class KvinRecordConverter extends RecordMaterializer<KvinRecord> {
	private KvinRecord record;
	private long itemId;
	private long propertyId;
	private long contextId;
	private long time;
	private int seqNr;
	private Object recordValue;

	private final GroupConverter root = new GroupConverter() {
		@Override
		public Converter getConverter(int fieldIndex) {
			switch (fieldIndex) {
				case 0: return itemIdConverter;
				case 1: return contextIdConverter;
				case 2: return propertyIdConverter;
				case 3: return timeConverter;
				case 4: return seqNrConverter;
				case 5: return firstConverter;
				case 10: return stringValueConverter;
				default: return valueConverter;
			}
		}

		@Override
		public void start() {
			seqNr = 0;
		}

		@Override
		public void end() {
			record = new KvinRecord(itemId, contextId, propertyId, time, seqNr, recordValue);
		}
	};

	private final PrimitiveConverter firstConverter = new PrimitiveConverter() {
		@Override
		public void addBoolean(boolean value) {
			// ignore first value
		}
	};

	private final PrimitiveConverter timeConverter = new PrimitiveConverter() {
		@Override
		public void addLong(long value) {
			time = value;
		}
	};

	private final PrimitiveConverter seqNrConverter = new PrimitiveConverter() {
		@Override
		public void addInt(int value) {
			seqNr = value;
		}
	};

	private final PrimitiveConverter itemIdConverter = new PrimitiveConverter() {
		@Override
		public void addLong(long value) {
			itemId = value;
		}
	};

	private final PrimitiveConverter contextIdConverter = new PrimitiveConverter() {
		@Override
		public void addLong(long value) {
			contextId = value;
		}
	};

	private final PrimitiveConverter propertyIdConverter = new PrimitiveConverter() {
		@Override
		public void addLong(long value) {
			propertyId = value;
		}
	};

	private final PrimitiveConverter stringValueConverter = new PrimitiveConverter() {
		@Override
		public void addBinary(Binary value) {
			recordValue = new String(value.getBytes(), StandardCharsets.UTF_8);
		}
	};

	private final PrimitiveConverter valueConverter = new PrimitiveConverter() {
		void addObject(Object value) {
			recordValue = value;
		}

		@Override
		public void addBinary(Binary value) {
			addObject(value.toByteBuffer());
		}

		@Override
		public void addBoolean(boolean value) {
			addObject(value);
		}

		@Override
		public void addDouble(double value) {
			addObject(value);
		}

		@Override
		public void addFloat(float value) {
			addObject(value);
		}

		@Override
		public void addInt(int value) {
			addObject(value);
		}

		@Override
		public void addLong(long value) {
			addObject(value);
		}
	};

	@Override
	public KvinRecord getCurrentRecord() {
		return record;
	}

	@Override
	public GroupConverter getRootConverter() {
		return root;
	}
}