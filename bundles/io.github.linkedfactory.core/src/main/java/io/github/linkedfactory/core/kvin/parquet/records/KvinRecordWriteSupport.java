package io.github.linkedfactory.core.kvin.parquet.records;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.conf.HadoopParquetConfiguration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.nio.ByteBuffer;
import java.util.HashMap;

public class KvinRecordWriteSupport extends WriteSupport<KvinRecord> {
	final MessageType schema;
	RecordConsumer rc;
	KvinRecord prevRecord;

	KvinRecordWriteSupport(MessageType schema) {
		this.schema = schema;
	}

	@Override
	public WriteContext init(Configuration configuration) {
		return init(new HadoopParquetConfiguration(configuration));
	}

	public WriteContext init(ParquetConfiguration configuration) {
		return new WriteContext(schema, new HashMap<>());
	}

	@Override
	public void prepareForWrite(RecordConsumer recordConsumer) {
		this.rc = recordConsumer;
		this.prevRecord = null;
	}

	@Override
	public void write(KvinRecord r) {
		rc.startMessage();
		rc.startField("itemId", 0);
		rc.addLong(r.itemId);
		rc.endField("itemId", 0);

		rc.startField("contextId", 1);
		rc.addLong(r.contextId);
		rc.endField("contextId", 1);

		rc.startField("propertyId", 2);
		rc.addLong(r.propertyId);
		rc.endField("propertyId", 2);

		rc.startField("time", 3);
		rc.addLong(r.time);
		rc.endField("time", 3);

		rc.startField("seqNr", 4);
		rc.addInteger(r.seqNr);
		rc.endField("seqNr", 4);

		boolean first = prevRecord == null || prevRecord.itemId != r.itemId || prevRecord.contextId != r.contextId ||
				prevRecord.propertyId != r.propertyId;
		rc.startField("first", 5);
		rc.addBoolean(first);
		rc.endField("first", 5);

		Object value = r.value;
		if (value instanceof Integer) {
			rc.startField("valueInt", 6);
			rc.addInteger((Integer) value);
			rc.endField("valueInt", 6);
		} else if (value instanceof Long) {
			rc.startField("valueLong", 7);
			rc.addLong((Long) value);
			rc.endField("valueLong", 7);
		} else if (value instanceof Float) {
			rc.startField("valueFloat", 8);
			rc.addFloat((Float) value);
			rc.endField("valueFloat", 8);
		} else if (value instanceof Double) {
			rc.startField("valueDouble", 9);
			rc.addDouble((Double) value);
			rc.endField("valueDouble", 9);
		} else if (value instanceof String) {
			rc.startField("valueString", 10);
			rc.addBinary(Binary.fromString((String) value));
			rc.endField("valueString", 10);
		} else if (value instanceof Boolean) {
			rc.startField("valueBool", 11);
			rc.addBoolean((Boolean) value);
			rc.endField("valueBool", 11);
		} else if (value instanceof ByteBuffer) {
			rc.startField("valueObject", 12);
			rc.addBinary(Binary.fromConstantByteBuffer((ByteBuffer) value));
			rc.endField("valueObject", 12);
		} else if (value instanceof byte[]) {
			rc.startField("valueObject", 12);
			rc.addBinary(Binary.fromConstantByteArray((byte[]) value));
			rc.endField("valueObject", 12);
		}
		rc.endMessage();
		// store previous record
		prevRecord = r;
	}
}
