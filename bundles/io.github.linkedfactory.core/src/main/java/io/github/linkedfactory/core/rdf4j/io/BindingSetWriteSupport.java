package io.github.linkedfactory.core.rdf4j.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.conf.HadoopParquetConfiguration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.query.BindingSet;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;

public class BindingSetWriteSupport extends WriteSupport<BindingSet> {
	final List<String> bindingNames;
	final MessageType schema;
	final PrimitiveType.PrimitiveTypeName[] fieldTypes;
	RecordConsumer rc;

	BindingSetWriteSupport(List<String> bindingNames, MessageType schema) {
		this.bindingNames = bindingNames;
		this.schema = schema;
		this.fieldTypes = new PrimitiveType.PrimitiveTypeName[this.bindingNames.size()];
		for (int i = 0; i < this.bindingNames.size(); i++) {
			Type fieldType = schema.getType(i);
			if (fieldType.isPrimitive()) {
				this.fieldTypes[i] = fieldType.asPrimitiveType().getPrimitiveTypeName();
			}
		}
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
	}

	@Override
	public void write(BindingSet bs) {
		rc.startMessage();
		for (int i = 0; i < this.bindingNames.size(); i++) {
			String name = this.bindingNames.get(i);
			rc.startField(name, i);

			PrimitiveType.PrimitiveTypeName fieldType = this.fieldTypes[i];

			Value value = bs.getValue(this.bindingNames.get(i));
			if (value != null) {
				if (value.isLiteral()) {
					Literal literal = (Literal) value;
					CoreDatatype datatype = literal.getCoreDatatype();
					if (datatype.isXSDDatatype()) {
						switch ((CoreDatatype.XSD) datatype) {
							case INTEGER:
							case INT:
								if (fieldType == PrimitiveType.PrimitiveTypeName.INT32) {
									rc.addInteger(literal.intValue());
								}
								break;
							case LONG:
								if (fieldType == PrimitiveType.PrimitiveTypeName.INT64) {
									rc.addLong(literal.longValue());
								}
								break;
							case FLOAT:
								if (fieldType == PrimitiveType.PrimitiveTypeName.FLOAT) {
									rc.addFloat(literal.floatValue());
								}
								break;
							case DOUBLE:
								if (fieldType == PrimitiveType.PrimitiveTypeName.DOUBLE) {
									rc.addDouble(literal.doubleValue());
								}
								break;
							case BOOLEAN:
								if (fieldType == PrimitiveType.PrimitiveTypeName.BOOLEAN) {
									rc.addBoolean(literal.booleanValue());
								}
								break;
							case STRING:
							default:
								if (fieldType == PrimitiveType.PrimitiveTypeName.BINARY) {
									rc.addBinary(Binary.fromReusedByteBuffer(
											ByteBuffer.wrap(literal.stringValue().getBytes())));
								}
								break;
						}
					} else {
						if (fieldType == PrimitiveType.PrimitiveTypeName.BINARY) {
							rc.addBinary(Binary.fromReusedByteBuffer(
									ByteBuffer.wrap(literal.stringValue().getBytes())));
						}
					}
				} else {
					// for IRIs, BNodes, and non-literal values, we just store the string representation
					if (fieldType == PrimitiveType.PrimitiveTypeName.BINARY) {
						rc.addBinary(Binary.fromReusedByteBuffer(
								ByteBuffer.wrap(value.stringValue().getBytes())));
					}
				}
			}

			rc.endField(name, i);
		}
		rc.endMessage();
	}
}