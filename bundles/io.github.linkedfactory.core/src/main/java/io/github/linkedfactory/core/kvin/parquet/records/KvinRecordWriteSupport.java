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
		rc.startField("id", 0);
		ByteBuffer idBb = ByteBuffer.allocate(Long.BYTES * 3);
		idBb.putLong(r.itemId).putLong(r.contextId).putLong(r.propertyId);
		rc.addBinary(Binary.fromConstantByteBuffer(idBb.flip()));
		rc.endField("id", 0);

		rc.startField("time", 1);
		rc.addLong(r.time);
		rc.endField("time", 1);

		rc.startField("seqNr", 2);
		rc.addInteger(r.seqNr);
		rc.endField("seqNr", 2);

		boolean first = prevRecord == null || prevRecord.itemId != r.itemId || prevRecord.contextId != r.contextId ||
				prevRecord.propertyId != r.propertyId;
		if (first) {
			rc.startField("first", 3);
			rc.addBoolean(true);
			rc.endField("first", 3);
		}

		Object value = r.value;
		if (value instanceof Integer) {
			rc.startField("valueInt", 4);
			rc.addInteger((Integer) value);
			rc.endField("valueInt", 4);
		} else if (value instanceof Long) {
			rc.startField("valueLong", 5);
			rc.addLong((Long) value);
			rc.endField("valueLong", 5);
		} else if (value instanceof Float) {
			rc.startField("valueFloat", 6);
			rc.addFloat((Float) value);
			rc.endField("valueFloat", 6);
		} else if (value instanceof Double) {
			rc.startField("valueDouble", 7);
			rc.addDouble((Double) value);
			rc.endField("valueDouble", 7);
		} else if (value instanceof String) {
			rc.startField("valueString", 8);
			rc.addBinary(Binary.fromString((String) value));
			rc.endField("valueString", 8);
		} else if (value instanceof Boolean) {
			rc.startField("valueBool", 9);
			rc.addBoolean((Boolean) value);
			rc.endField("valueBool", 9);
		} else if (value instanceof ByteBuffer) {
			rc.startField("valueObject", 10);
			rc.addBinary(Binary.fromConstantByteBuffer((ByteBuffer) value));
			rc.endField("valueObject", 10);
		}
		rc.endMessage();
	}
}
