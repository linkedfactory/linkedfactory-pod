/*
 * Copyright (c) 2026 Fraunhofer IWU.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package io.github.linkedfactory.core.kvin.util;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.parquet.ParquetValues;
import io.github.linkedfactory.core.kvin.parquet.Records;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Reads the archive-aligned KVIN exchange format, one tuple per row with exactly
 * one typed value column. Relative item/property URIs resolve against the request
 * URI. Context and seqNr default to the request context and zero.
 */
public class ParquetFormatParser {
	protected final static Logger logger = LoggerFactory.getLogger(ParquetFormatParser.class);
	private static final Set<String> REQUIRED_COLUMNS = Set.of("item", "property", "time");

	protected final URI base;
	protected final InputFile file;
	protected URI context = Kvin.DEFAULT_CONTEXT;

	/**
	 * Parses Parquet content from memory.
	 */
	public ParquetFormatParser(URI base, byte[] data) {
		this(base, new ByteArrayInputFile(data));
	}

	/**
	 * Parses a Parquet file.
	 */
	public ParquetFormatParser(URI base, Path path) {
		this(base, new LocalInputFile(path));
	}

	protected ParquetFormatParser(URI base, InputFile file) {
		this.base = base;
		this.file = file;
	}

	public NiceIterator<KvinTuple> parse() throws IOException {
		// avoid Hadoop configuration, which parses its default XML resources for every new instance
		ParquetFileReader reader = ParquetFileReader.open(file,
				ParquetReadOptions.builder(new PlainParquetConfiguration()).build());
		MessageType schema;
		TupleConverter converter;
		try {
			schema = projectSchema(reader.getFileMetaData().getSchema());
			checkRequiredValues(reader.getRowGroups());
			reader.setRequestedSchema(schema);
			converter = new TupleConverter(schema);
		} catch (RuntimeException e) {
			reader.close();
			throw e;
		}
		MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
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
			KvinTuple tuple;
			boolean closed;

			@Override
			public boolean hasNext() {
				if (tuple != null) {
					return true;
				}
				if (closed) {
					return false;
				}
				try {
					while (remaining == 0) {
						if (rowGroup != null) {
							rowGroup.close();
						}
						rowGroup = reader.readNextRowGroup();
						if (rowGroup == null) {
							close();
							return false;
						}
						remaining = rowGroup.getRowCount();
						records = columnIO.getRecordReader(rowGroup, materializer);
					}
					tuple = records.read();
					remaining--;
					return true;
				} catch (IOException e) {
					close();
					throw new UncheckedIOException(e);
				} catch (RuntimeException e) {
					close();
					throw e;
				}
			}

			@Override
			public KvinTuple next() {
				ensureHasNext();
				KvinTuple next = tuple;
				tuple = null;
				return next;
			}

			@Override
			public void close() {
				if (closed) {
					return;
				}
				closed = true;
				try {
					if (rowGroup != null) {
						rowGroup.close();
					}
					reader.close();
				} catch (IOException e) {
					// ignore
					logger.error("Exception while closing Parquet reader", e);
				}
			}
		};
	}

	protected MessageType projectSchema(MessageType fileSchema) {
		if (fileSchema.containsField("value")) {
			throw new IllegalArgumentException("Use typed value columns (valueInt, valueDouble, etc.), not 'value'");
		}
		List<Type> fields = new ArrayList<>();
		for (Type field : fileSchema.getFields()) {
			String name = field.getName();
			if (!ParquetFormatWriter.SCHEMA.containsField(name)) {
				// ignore unrelated columns
				continue;
			}
			if (!field.isPrimitive() || field.isRepetition(Type.Repetition.REPEATED)) {
				throw new IllegalArgumentException("Parquet column '" + name + "' must be a non-repeated primitive");
			}
			PrimitiveTypeName type = field.asPrimitiveType().getPrimitiveTypeName();
			LogicalTypeAnnotation annotation = field.getLogicalTypeAnnotation();
			// Only annotations whose meaning is preserved by the tuple conversion are accepted.
			boolean supportedAnnotation = annotation == null ||
					annotation instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation && !name.equals("valueObject") ||
					annotation instanceof TimestampLogicalTypeAnnotation && name.equals("time") ||
					annotation instanceof IntLogicalTypeAnnotation intType && intType.isSigned();
			boolean supportedType = type == ParquetFormatWriter.SCHEMA.getType(name).asPrimitiveType().getPrimitiveTypeName() ||
					name.equals("seqNr") && type == PrimitiveTypeName.INT64;
			if (!supportedType || !supportedAnnotation) {
				throw new IllegalArgumentException("Unsupported type " + type + (annotation == null ? "" : " (" + annotation + ")")
						+ " of Parquet column '" + name + "'");
			}
			fields.add(field);
		}
		for (String name : REQUIRED_COLUMNS) {
			if (!fileSchema.containsField(name)) {
				throw new IllegalArgumentException("Missing required Parquet column '" + name + "'");
			}
		}
		if (ParquetValues.FIELDS.stream().noneMatch(field -> fileSchema.containsField(field.getName()))) {
			throw new IllegalArgumentException("Missing typed Parquet value column");
		}
		return new MessageType(fileSchema.getName(), fields);
	}

	/**
	 * Rejects known missing identity values early when footer statistics are available.
	 * Row validation remains necessary; streaming ingestion is not transactional.
	 */
	protected void checkRequiredValues(List<BlockMetaData> rowGroups) {
		for (BlockMetaData rowGroup : rowGroups) {
			for (ColumnChunkMetaData column : rowGroup.getColumns()) {
				String name = column.getPath().toDotString();
				Statistics<?> statistics = column.getStatistics();
				if (REQUIRED_COLUMNS.contains(name) && statistics != null && statistics.isNumNullsSet()
						&& statistics.getNumNulls() > 0) {
					throw new IllegalArgumentException("Parquet column '" + name + "' contains missing values");
				}
			}
		}
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

	public ParquetFormatParser setContext(URI context) {
		this.context = context;
		return this;
	}

	public URI getContext() {
		return context;
	}

	class TupleConverter extends GroupConverter {
		final Converter[] converters;
		URI item, property, tupleContext;
		long time;
		int seqNr;
		Object value;
		boolean hasTime;
		KvinTuple tuple;

		TupleConverter(MessageType schema) {
			converters = new Converter[schema.getFieldCount()];
			for (int i = 0; i < converters.length; i++) {
				converters[i] = switch (schema.getFieldName(i)) {
					case "item" -> new UriConverter() {
						@Override
						void set(URI uri) {
							item = uri;
						}
					};
					case "property" -> new UriConverter() {
						@Override
						void set(URI uri) {
							property = uri;
						}
					};
					case "context" -> new UriConverter() {
						@Override
						URI convert(String uriString) {
							URI uri = URIs.createURI(uriString);
							if (uri.isRelative() || CsvFormatParser.containsWhitespace(uriString)) {
								// contexts are not resolved against the base URI
								throw new IllegalArgumentException("Invalid context URI: '" + uriString + "'");
							}
							return uri;
						}

						@Override
						void set(URI uri) {
							tupleContext = uri;
						}
					};
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

						@Override
						public void addLong(long value) {
							if (value != (int) value) {
								throw new IllegalArgumentException("Invalid seqNr: " + value);
							}
							seqNr = (int) value;
						}
					};
					case "valueObject" -> new ValueConverter() {
						@Override public void addBinary(Binary binary) {
							try {
								var buffer = binary.toByteBuffer();
								Object decoded = Records.decodeRecord(buffer);
								if (buffer.hasRemaining()) throw new IOException("Trailing object bytes");
								setValue(decoded);
							} catch (IOException | RuntimeException e) {
								throw new IllegalArgumentException("Invalid Parquet valueObject", e);
							}
						}
					};
					default -> new ValueConverter();
				};
			}
		}

		@Override
		public Converter getConverter(int fieldIndex) {
			return converters[fieldIndex];
		}

		@Override
		public void start() {
			item = property = tupleContext = null;
			seqNr = 0;
			value = null;
			hasTime = false;
		}

		@Override
		public void end() {
			if (item == null || property == null || !hasTime || value == null) {
				throw new IllegalArgumentException("Missing required value in Parquet row");
			}
			tuple = new KvinTuple(item, property, tupleContext == null ? context : tupleContext, time, seqNr, value);
		}

		/**
		 * Converts each dictionary entry only once as URIs are usually repeated in many rows.
		 */
		abstract class UriConverter extends PrimitiveConverter {
			URI[] dictionary;

			URI convert(String uriString) {
				return CsvFormatParser.createURI(base, uriString);
			}

			abstract void set(URI uri);

			@Override
			public boolean hasDictionarySupport() {
				return true;
			}

			@Override
			public void setDictionary(Dictionary dictionary) {
				this.dictionary = new URI[dictionary.getMaxId() + 1];
				for (int id = 0; id < this.dictionary.length; id++) {
					this.dictionary[id] = convert(dictionary.decodeToBinary(id).toStringUsingUTF8());
				}
			}

			@Override
			public void addValueFromDictionary(int dictionaryId) {
				set(dictionary[dictionaryId]);
			}

			@Override
			public void addBinary(Binary value) {
				set(convert(value.toStringUsingUTF8()));
			}
		}

		void setValue(Object value) {
			if (this.value != null) {
				throw new IllegalArgumentException("Parquet row must contain exactly one typed value");
			}
			this.value = value;
		}

		class ValueConverter extends PrimitiveConverter {
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
	 * Parquet input from memory, which avoids spooling small uploads to a temporary file.
	 */
	static class ByteArrayInputFile implements InputFile {
		final byte[] data;

		ByteArrayInputFile(byte[] data) {
			this.data = data;
		}

		@Override
		public long getLength() {
			return data.length;
		}

		@Override
		public SeekableInputStream newStream() {
			var bytes = new ByteArrayInputStream(data) {
				long position() {
					return pos;
				}

				void seek(long newPos) {
					pos = (int) Math.min(newPos, count);
				}
			};
			return new DelegatingSeekableInputStream(bytes) {
				@Override
				public long getPos() {
					return bytes.position();
				}

				@Override
				public void seek(long newPos) {
					bytes.seek(newPos);
				}
			};
		}

		@Override
		public String toString() {
			return "Parquet payload";
		}
	}
}
