package io.github.linkedfactory.core.kvin.parquet.records;

import io.github.linkedfactory.core.kvin.parquet.ParquetValues;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.conf.HadoopParquetConfiguration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

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

		ParquetValues.write(rc, 6, r.value);
		rc.endMessage();
		// store previous record
		prevRecord = r;
	}
}
