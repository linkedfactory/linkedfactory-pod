package io.github.linkedfactory.core.kvin.parquet.records;

import org.apache.parquet.io.api.*;

import java.nio.charset.StandardCharsets;

public class KvinRecordConverter extends RecordMaterializer<KvinRecord> {
	private KvinRecord currentRecord;

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
			currentRecord = new KvinRecord();
		}

		@Override
		public void end() {
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
			currentRecord.time = value;
		}
	};

	private final PrimitiveConverter seqNrConverter = new PrimitiveConverter() {
		@Override
		public void addInt(int value) {
			currentRecord.seqNr = value;
		}
	};

	private final PrimitiveConverter itemIdConverter = new PrimitiveConverter() {
		@Override
		public void addLong(long value) {
			currentRecord.itemId = value;
		}
	};

	private final PrimitiveConverter contextIdConverter = new PrimitiveConverter() {
		@Override
		public void addLong(long value) {
			currentRecord.contextId = value;
		}
	};

	private final PrimitiveConverter propertyIdConverter = new PrimitiveConverter() {
		@Override
		public void addLong(long value) {
			currentRecord.propertyId = value;
		}
	};

	private final PrimitiveConverter stringValueConverter = new PrimitiveConverter() {
		@Override
		public void addBinary(Binary value) {
			currentRecord.value = new String(value.getBytes(), StandardCharsets.UTF_8);
		}
	};

	private final PrimitiveConverter valueConverter = new PrimitiveConverter() {
		void addObject(Object value) {
			currentRecord.value = value;
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
		return currentRecord;
	}

	@Override
	public GroupConverter getRootConverter() {
		return root;
	}
}