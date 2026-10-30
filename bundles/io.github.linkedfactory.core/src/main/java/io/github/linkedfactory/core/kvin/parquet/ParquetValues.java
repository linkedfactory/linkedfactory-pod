package io.github.linkedfactory.core.kvin.parquet;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Stream;

/** Shared value columns of the archive and the public KVIN Parquet format. */
public final class ParquetValues {
	private ParquetValues() {}

	public static final List<Type> FIELDS = List.of(
			new PrimitiveType(Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "valueInt"),
			new PrimitiveType(Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT64, "valueLong"),
			new PrimitiveType(Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.FLOAT, "valueFloat"),
			new PrimitiveType(Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "valueDouble"),
			new PrimitiveType(Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "valueString"),
			new PrimitiveType(Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BOOLEAN, "valueBool"),
			new PrimitiveType(Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "valueObject")
	);

	public static MessageType schema(String name, Type... identityFields) {
		return new MessageType(name, Stream.concat(Stream.of(identityFields), FIELDS.stream()).toList());
	}

	/** Writes an archive value (complex values must already be encoded by Records). */
	public static void write(RecordConsumer rc, int firstField, Object value) {
		if (value instanceof Integer) {
			rc.startField("valueInt", firstField);
			rc.addInteger((Integer) value);
			rc.endField("valueInt", firstField);
		} else if (value instanceof Long) {
			rc.startField("valueLong", firstField + 1);
			rc.addLong((Long) value);
			rc.endField("valueLong", firstField + 1);
		} else if (value instanceof Float) {
			rc.startField("valueFloat", firstField + 2);
			rc.addFloat((Float) value);
			rc.endField("valueFloat", firstField + 2);
		} else if (value instanceof Double) {
			rc.startField("valueDouble", firstField + 3);
			rc.addDouble((Double) value);
			rc.endField("valueDouble", firstField + 3);
		} else if (value instanceof String) {
			rc.startField("valueString", firstField + 4);
			rc.addBinary(Binary.fromString((String) value));
			rc.endField("valueString", firstField + 4);
		} else if (value instanceof Boolean) {
			rc.startField("valueBool", firstField + 5);
			rc.addBoolean((Boolean) value);
			rc.endField("valueBool", firstField + 5);
		} else if (value instanceof ByteBuffer) {
			rc.startField("valueObject", firstField + 6);
			rc.addBinary(Binary.fromConstantByteBuffer((ByteBuffer) value));
			rc.endField("valueObject", firstField + 6);
		} else if (value instanceof byte[]) {
			rc.startField("valueObject", firstField + 6);
			rc.addBinary(Binary.fromConstantByteArray((byte[]) value));
			rc.endField("valueObject", firstField + 6);
		} else {
			throw new IllegalArgumentException("Unsupported Parquet value: " + value);
		}
	}
}
