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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;

/**
 * Parquet WriteSupport for RDF4J BindingSets.
 */
public class BindingSetWriteSupport extends WriteSupport<BindingSet> {
	private static final Logger log = LoggerFactory.getLogger(BindingSetWriteSupport.class);

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

			PrimitiveType.PrimitiveTypeName fieldType = this.fieldTypes[i];

			Value value = bs.getValue(this.bindingNames.get(i));
			if (value != null) {
				if (value.isLiteral()) {
					Literal literal = (Literal) value;
					boolean valueWritten = false;
					CoreDatatype datatype = literal.getCoreDatatype();
					if (datatype.isXSDDatatype()) {
						switch ((CoreDatatype.XSD) datatype) {
							case INTEGER:
							case INT:
								if (fieldType == PrimitiveType.PrimitiveTypeName.INT32) {
									rc.startField(name, i);
									rc.addInteger(literal.intValue());
									rc.endField(name, i);
									valueWritten = true;
									break;
								}
								// if the field is not INT32, we can still try LONG
							case LONG:
								if (fieldType == PrimitiveType.PrimitiveTypeName.INT64) {
									rc.startField(name, i);
									rc.addLong(literal.longValue());
									rc.endField(name, i);
									valueWritten = true;
								} else if (fieldType == PrimitiveType.PrimitiveTypeName.DOUBLE) {
									// LONG can also be stored as DOUBLE
									rc.startField(name, i);
									rc.addDouble(literal.doubleValue());
									rc.endField(name, i);
									valueWritten = true;
								}
								break;
							case FLOAT:
								if (fieldType == PrimitiveType.PrimitiveTypeName.FLOAT) {
									rc.startField(name, i);
									rc.addFloat(literal.floatValue());
									rc.endField(name, i);
									valueWritten = true;
									break;
								}
								// if the field is not FLOAT, we can still try DOUBLE
							case DOUBLE:
								if (fieldType == PrimitiveType.PrimitiveTypeName.DOUBLE) {
									rc.startField(name, i);
									rc.addDouble(literal.doubleValue());
									rc.endField(name, i);
									valueWritten = true;
								}
								break;
							case BOOLEAN:
								if (fieldType == PrimitiveType.PrimitiveTypeName.BOOLEAN) {
									rc.startField(name, i);
									rc.addBoolean(literal.booleanValue());
									rc.endField(name, i);
									valueWritten = true;
								}
								break;
							case STRING:
							default:
								if (fieldType == PrimitiveType.PrimitiveTypeName.BINARY) {
									rc.startField(name, i);
									rc.addBinary(Binary.fromConstantByteArray(value.stringValue().getBytes()));
									rc.endField(name, i);
									valueWritten = true;
								}
								break;
						}
					}

					if (! valueWritten) {
						// fallback: store the string representation
						if (fieldType == PrimitiveType.PrimitiveTypeName.BINARY) {
							rc.startField(name, i);
							rc.addBinary(Binary.fromConstantByteArray(value.stringValue().getBytes()));
							rc.endField(name, i);
						} else {
							log.info("Can't store literal {} in field '{}' of type {}", value, name, fieldType.name());
						}
					}
				} else {
					// for IRIs, BNodes, and non-literal values, we just store the string representation
					if (fieldType == PrimitiveType.PrimitiveTypeName.BINARY) {
						rc.startField(name, i);
						rc.addBinary(Binary.fromConstantByteArray(value.stringValue().getBytes()));
						rc.endField(name, i);
					} else {
						log.info("Can't store resource {} in field '{}' of type {}", value, name, fieldType.name());
					}
				}
			}
		}
		rc.endMessage();
	}
}