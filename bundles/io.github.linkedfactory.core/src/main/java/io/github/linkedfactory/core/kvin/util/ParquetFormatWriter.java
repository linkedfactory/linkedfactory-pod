package io.github.linkedfactory.core.kvin.util;

import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.parquet.ParquetValues;
import io.github.linkedfactory.core.kvin.parquet.Records;
import io.github.linkedfactory.core.rdf4j.io.OutputFileOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Map;

import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;

/** Streaming KVIN exchange format: archive value columns with self-contained URI identifiers. */
public final class ParquetFormatWriter implements AutoCloseable {
	public static final String MIME_TYPE = "application/vnd.apache.parquet";
	public static final MessageType SCHEMA = ParquetValues.schema("KvinTuple",
			Types.required(BINARY).as(stringType()).named("item"),
			Types.required(BINARY).as(stringType()).named("context"),
			Types.required(BINARY).as(stringType()).named("property"),
			Types.required(INT64).named("time"),
			Types.required(INT32).named("seqNr"));

	private final ParquetWriter<KvinTuple> writer;

	public ParquetFormatWriter(OutputStream out) throws IOException {
		writer = new Builder(new OutputFileOutputStream(new BufferedOutputStream(out)))
				.withConf(new PlainParquetConfiguration())
				.withCompressionCodec(CompressionCodecName.SNAPPY)
				.withDictionaryEncoding(true).withDictionaryEncoding("valueObject", false)
				.withRowGroupSize(2 * 1024 * 1024).withPageSize(8192)
				.build();
	}

	public void writeTuple(KvinTuple tuple) throws IOException {
		writer.write(tuple);
	}

	@Override
	public void close() throws IOException {
		writer.close();
	}

	private static class Builder extends ParquetWriter.Builder<KvinTuple, Builder> {
		Builder(OutputFile file) { super(file); }
		@Override protected Builder self() { return this; }
		@Override protected WriteSupport<KvinTuple> getWriteSupport(Configuration conf) {
			return getWriteSupport((ParquetConfiguration) null);
		}
		@Override protected WriteSupport<KvinTuple> getWriteSupport(ParquetConfiguration conf) {
			return new WriteSupport<>() {
				RecordConsumer rc;
				@Override public WriteContext init(Configuration conf) {
					return init((ParquetConfiguration) null);
				}
				@Override public WriteContext init(ParquetConfiguration conf) {
					return new WriteContext(SCHEMA, Map.of());
				}
				@Override public void prepareForWrite(RecordConsumer consumer) { rc = consumer; }
				@Override public void write(KvinTuple tuple) {
					Object value = tuple.value;
					if (!(value instanceof Integer || value instanceof Long || value instanceof Float ||
							value instanceof Double || value instanceof String || value instanceof Boolean)) {
						try {
							value = Records.encodeRecord(value);
						} catch (IOException e) {
							throw new UncheckedIOException(e);
						}
					}
					rc.startMessage();
					writeUri("item", 0, tuple.item.toString());
					writeUri("context", 1, tuple.context.toString());
					writeUri("property", 2, tuple.property.toString());
					rc.startField("time", 3);
					rc.addLong(tuple.time);
					rc.endField("time", 3);
					rc.startField("seqNr", 4);
					rc.addInteger(tuple.seqNr);
					rc.endField("seqNr", 4);
					ParquetValues.write(rc, 5, value);
					rc.endMessage();
				}
				private void writeUri(String name, int index, String uri) {
					rc.startField(name, index);
					rc.addBinary(Binary.fromString(uri));
					rc.endField(name, index);
				}
			};
		}
	}
}
