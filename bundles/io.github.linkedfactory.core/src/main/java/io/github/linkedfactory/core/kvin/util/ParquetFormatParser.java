/*
 * Copyright (c) 2026 Fraunhofer IWU.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package io.github.linkedfactory.core.kvin.util;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.komma.core.URI;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;

/**
 * Parses KVIN tuples from a Parquet file with one tuple per row.
 * <p>
 * The columns <code>item</code> and <code>property</code> are URIs that are resolved against the base URI,
 * <code>time</code> is in epoch milliseconds (or a timestamp) and <code>seqNr</code> is optional.
 * Each row has exactly one value in the typed value columns of the Parquet archive
 * (see {@link io.github.linkedfactory.core.kvin.parquet.ParquetHelpers#kvinTupleType}).
 * Other columns are ignored.
 */
public class ParquetFormatParser {
	static final Map<String, PrimitiveTypeName> COLUMNS = Map.of(
			"item", BINARY, "property", BINARY, "time", INT64, "seqNr", INT32,
			"valueInt", INT32, "valueLong", INT64, "valueFloat", FLOAT, "valueDouble", DOUBLE,
			"valueString", BINARY, "valueBool", BOOLEAN);

	final URI base;
	final Path file;
	URI context = Kvin.DEFAULT_CONTEXT;

	public ParquetFormatParser(URI base, Path file) {
		this.base = base;
		this.file = file;
	}

	public ParquetFormatParser setContext(URI context) {
		this.context = context;
		return this;
	}

	public NiceIterator<KvinTuple> parse() throws IOException {
		// avoid Hadoop's configuration as it parses its XML resources for each new instance
		ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file),
				ParquetReadOptions.builder(new PlainParquetConfiguration()).build());
		MessageType schema;
		try {
			schema = requestedSchema(reader.getFileMetaData().getSchema());
		} catch (RuntimeException e) {
			reader.close();
			throw e;
		}
		reader.setRequestedSchema(schema);
		MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
		TupleConverter converter = new TupleConverter(schema);
		RecordMaterializer<KvinTuple> materializer = new RecordMaterializer<>() {
			@Override
			public KvinTuple getCurrentRecord() {
				return converter.tuple;
			}

			@Override
			public GroupConverter getRootConverter() {
				return converter;
			}
		};

		return new NiceIterator<>() {
			PageReadStore rowGroup;
			RecordReader<KvinTuple> records;
			long remaining;

			@Override
			public boolean hasNext() {
				try {
					while (remaining == 0) {
						if (rowGroup != null) {
							rowGroup.close();
						}
						rowGroup = reader.readNextRowGroup();
						if (rowGroup == null) {
							return false;
						}
						remaining = rowGroup.getRowCount();
						records = columnIO.getRecordReader(rowGroup, materializer);
					}
					return true;
				} catch (IOException e) {
					throw new UncheckedIOException(e);
				}
			}

			@Override
			public KvinTuple next() {
				ensureHasNext();
				remaining--;
				return records.read();
			}

			@Override
			public void close() {
				try {
					if (rowGroup != null) {
						rowGroup.close();
					}
					reader.close();
				} catch (IOException e) {
					throw new UncheckedIOException(e);
				}
			}
		};
	}

	/**
	 * Selects the known columns of the file and checks their types.
	 */
	static MessageType requestedSchema(MessageType fileSchema) {
		List<Type> fields = new ArrayList<>();
		for (Type field : fileSchema.getFields()) {
			PrimitiveTypeName expectedType = COLUMNS.get(field.getName());
			if (expectedType == null) {
				continue;
			}
			// only annotations that do not change the meaning of the values
			LogicalTypeAnnotation annotation = field.getLogicalTypeAnnotation();
			boolean supported = field.isPrimitive() && !field.isRepetition(Type.Repetition.REPEATED)
					&& field.asPrimitiveType().getPrimitiveTypeName() == expectedType
					&& (annotation == null || annotation instanceof StringLogicalTypeAnnotation
					|| annotation instanceof IntLogicalTypeAnnotation intType && intType.isSigned()
					|| annotation instanceof TimestampLogicalTypeAnnotation && field.getName().equals("time"));
			if (!supported) {
				throw new IllegalArgumentException("Unsupported type of Parquet column: " + field);
			}
			fields.add(field);
		}
		for (String name : List.of("item", "property", "time")) {
			if (!fileSchema.containsField(name)) {
				throw new IllegalArgumentException("Missing Parquet column '" + name + "'");
			}
		}
		if (fields.stream().noneMatch(field -> field.getName().startsWith("value"))) {
			throw new IllegalArgumentException("Missing Parquet value column, one of: valueInt, valueLong, valueFloat, " +
					"valueDouble, valueString, valueBool");
		}
		return new MessageType(fileSchema.getName(), fields);
	}

	static long unitsPerMillisecond(Type timeField) {
		if (timeField.getLogicalTypeAnnotation() instanceof TimestampLogicalTypeAnnotation timestamp) {
			return switch (timestamp.getUnit()) {
				case MILLIS -> 1;
				case MICROS -> 1_000;
				case NANOS -> 1_000_000;
			};
		}
		return 1;
	}

	/**
	 * Converts the columns of a row to a tuple.
	 */
	class TupleConverter extends GroupConverter {
		final Converter[] converters;
		URI item, property;
		long time;
		boolean hasTime;
		int seqNr;
		Object value;
		KvinTuple tuple;

		TupleConverter(MessageType schema) {
			PrimitiveConverter valueConverter = new ValueConverter();
			converters = new Converter[schema.getFieldCount()];
			for (int i = 0; i < converters.length; i++) {
				converters[i] = switch (schema.getFieldName(i)) {
					case "item" -> new UriConverter(uri -> item = uri);
					case "property" -> new UriConverter(uri -> property = uri);
					case "time" -> {
						long unitsPerMillisecond = unitsPerMillisecond(schema.getType(i));
						yield new PrimitiveConverter() {
							@Override
							public void addLong(long value) {
								time = Math.floorDiv(value, unitsPerMillisecond);
								hasTime = true;
							}
						};
					}
					case "seqNr" -> new PrimitiveConverter() {
						@Override
						public void addInt(int value) {
							seqNr = value;
						}
					};
					default -> valueConverter;
				};
			}
		}

		@Override
		public Converter getConverter(int fieldIndex) {
			return converters[fieldIndex];
		}

		@Override
		public void start() {
			item = property = null;
			hasTime = false;
			seqNr = 0;
			value = null;
		}

		@Override
		public void end() {
			if (item == null || property == null || !hasTime || value == null) {
				throw new IllegalArgumentException("Parquet row requires item, property, time and a value");
			}
			tuple = new KvinTuple(item, property, context, time, seqNr, value);
		}

		class ValueConverter extends PrimitiveConverter {
			void setValue(Object value) {
				if (TupleConverter.this.value != null) {
					throw new IllegalArgumentException("Parquet row must contain exactly one value");
				}
				TupleConverter.this.value = value;
			}

			@Override
			public void addBinary(Binary value) {
				setValue(value.toStringUsingUTF8());
			}

			@Override
			public void addBoolean(boolean value) {
				setValue(value);
			}

			@Override
			public void addDouble(double value) {
				setValue(value);
			}

			@Override
			public void addFloat(float value) {
				setValue(value);
			}

			@Override
			public void addInt(int value) {
				setValue(value);
			}

			@Override
			public void addLong(long value) {
				setValue(value);
			}
		}
	}

	/**
	 * Converts each used dictionary entry only once as URIs are usually repeated in many rows.
	 */
	class UriConverter extends PrimitiveConverter {
		final Consumer<URI> target;
		Dictionary dictionary;
		URI[] uris;

		UriConverter(Consumer<URI> target) {
			this.target = target;
		}

		@Override
		public boolean hasDictionarySupport() {
			return true;
		}

		@Override
		public void setDictionary(Dictionary dictionary) {
			this.dictionary = dictionary;
			this.uris = new URI[dictionary.getMaxId() + 1];
		}

		@Override
		public void addValueFromDictionary(int dictionaryId) {
			if (uris[dictionaryId] == null) {
				uris[dictionaryId] = CsvFormatParser.createURI(base, dictionary.decodeToBinary(dictionaryId).toStringUsingUTF8());
			}
			target.accept(uris[dictionaryId]);
		}

		@Override
		public void addBinary(Binary value) {
			target.accept(CsvFormatParser.createURI(base, value.toStringUsingUTF8()));
		}
	}
}
