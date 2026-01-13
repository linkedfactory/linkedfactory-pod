package io.github.linkedfactory.core.rdf4j.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;

import java.util.Collections;
import java.util.List;

/**
 * A ParquetWriter for RDF4J BindingSets.
 */
public class BindingSetParquetWriter  {
	/**
	 * Creates a builder for configuring ParquetWriter.
	 *
	 * @param file the output file to create
	 * @return a {@link Builder} to create a {@link ParquetWriter}
	 */
	public static Builder builder(Path file) {
		return new Builder(file);
	}

	/**
	 * Creates a builder for configuring ParquetWriter.
	 *
	 * @param file the output file to create
	 * @return a {@link Builder} to create a {@link ParquetWriter}
	 */
	public static Builder builder(OutputFile file) {
		return new Builder(file);
	}

	/**
	 * A builder for configuring ParquetWriter for RDF4J BindingSets.
	 */
	public static class Builder extends ParquetWriter.Builder<BindingSet, Builder> {
		protected List<String> bindingNames = Collections.emptyList();
		protected BindingSet bindingSet = new EmptyBindingSet();
		protected MessageType schema;

		private Builder(Path file) {
			super(file);
		}

		private Builder(OutputFile file) {
			super(file);
		}

		@Override
		protected Builder self() {
			return this;
		}

		public Builder withBindingNames(List<String> bindingNames) {
			this.bindingNames = bindingNames;
			return this;
		}

		public Builder withBindingSet(BindingSet bs) {
			this.bindingSet = bs;
			return this;
		}

		@Override
		protected WriteSupport<BindingSet> getWriteSupport(Configuration conf) {
			return getWriteSupport((ParquetConfiguration) null);
		}

		@Override
		protected WriteSupport<BindingSet> getWriteSupport(ParquetConfiguration conf) {
				// create a simple schema: all bindings are OPTIONAL fields
				PrimitiveType[] types = new PrimitiveType[this.bindingNames.size()];
				for (int i = 0; i < this.bindingNames.size(); i++) {
					Value value = this.bindingSet.getValue(this.bindingNames.get(i));
					if (value != null && value.isLiteral()) {
						Literal literal = (Literal) value;
						CoreDatatype datatype = literal.getCoreDatatype();
						if (datatype.isXSDDatatype()) {
							switch ((CoreDatatype.XSD) datatype) {
								case INTEGER:
								case INT:
									types[i] =  new PrimitiveType(Type.Repetition.OPTIONAL,
											PrimitiveType.PrimitiveTypeName.INT32, this.bindingNames.get(i));
									break;
								case LONG:
									types[i] = new PrimitiveType(Type.Repetition.OPTIONAL,
											PrimitiveType.PrimitiveTypeName.INT64, this.bindingNames.get(i));
									break;
								case FLOAT:
									types[i] =  new PrimitiveType(Type.Repetition.OPTIONAL,
											PrimitiveType.PrimitiveTypeName.FLOAT, this.bindingNames.get(i));
									break;
								case DOUBLE:
									types[i] =  new PrimitiveType(Type.Repetition.OPTIONAL,
											PrimitiveType.PrimitiveTypeName.DOUBLE, this.bindingNames.get(i));
									break;
								case BOOLEAN:
									types[i] =  new PrimitiveType(Type.Repetition.OPTIONAL,
											PrimitiveType.PrimitiveTypeName.BOOLEAN, this.bindingNames.get(i));
									break;
								default:
									types[i] =  new PrimitiveType(Type.Repetition.OPTIONAL,
											PrimitiveType.PrimitiveTypeName.BINARY, this.bindingNames.get(i));
							}
						} else {
							types[i] =  new PrimitiveType(Type.Repetition.OPTIONAL,
									PrimitiveType.PrimitiveTypeName.BINARY, this.bindingNames.get(i));
						}
					} else {
						// for URIs and BNodes, we store the string representation
						types[i] = new PrimitiveType(Type.Repetition.OPTIONAL,
								PrimitiveType.PrimitiveTypeName.BINARY, this.bindingNames.get(i));
					}
				}
				this.schema = new MessageType("BindingSet", types);

			return new BindingSetWriteSupport(this.bindingNames, this.schema);
		}
	}
}