package io.github.linkedfactory.core.kvin.parquet;

import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.parquet.records.KvinParquetWriter;
import io.github.linkedfactory.core.kvin.parquet.records.KvinRecord;
import io.github.linkedfactory.core.kvin.parquet.records.KvinRecordConverter;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.commons.util.Pair;
import net.enilink.komma.core.URI;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type.Repetition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.github.linkedfactory.core.kvin.parquet.Records.decodeRecord;

public class ParquetHelpers {
	static final Logger log = LoggerFactory.getLogger(ParquetHelpers.class);
	static final ReflectData reflectData = new ReflectData(ParquetHelpers.class.getClassLoader());

	// parquet file writer config
	static final long ROW_GROUP_SIZE_MAPPINGS = 1048576L;  // 1 MB
	static final long ROW_GROUP_SIZE_DATA = 1048576L; // 1 MB - 134217728L is Parquet Java default
	static final int PAGE_SIZE = 8192; // 8 KB
	static final int DICT_PAGE_SIZE = 1048576; // 1 MB
	static final int ZSTD_COMPRESSION_LEVEL = 12; // 1 - 22
	public static MessageType kvinTupleType = new MessageType("KvinTupleInternal",
			// new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "id"),

			new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "itemId"),
			new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "contextId"),
			new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "propertyId"),
			new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "time"),
			new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "seqNr"),

			new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BOOLEAN, "first"),

			new PrimitiveType(Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "valueInt"),
			new PrimitiveType(Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT64, "valueLong"),
			new PrimitiveType(Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.FLOAT, "valueFloat"),
			new PrimitiveType(Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "valueDouble"),
			new PrimitiveType(Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "valueString"),
			new PrimitiveType(Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BOOLEAN, "valueBool"),
			new PrimitiveType(Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "valueObject")
	);
	// mapping file schema
	static Schema idMappingSchema = SchemaBuilder.record("SimpleMapping").namespace(IdMapping.class.getPackageName()).fields()
			.name("id").type().longType().noDefault()
			.name("value").type().stringType().noDefault().endRecord();

	static Pattern fileWithSeqNr = Pattern.compile("^([^.].*)__([0-9]+)\\..*$");
	static Pattern fileOrDotFileWithSeqNr = Pattern.compile("^\\.?([^.].*)__([0-9]+)\\..*$");
	static Configuration configuration = new Configuration();

	static {
		configuration.setInt("parquet.zstd.compressionLevel", ZSTD_COMPRESSION_LEVEL);
	}

	static ParquetWriter<KvinRecord> getKvinRecordWriter(Path dataFile) throws IOException {
		return KvinParquetWriter.builder(HadoopOutputFile.fromPath(dataFile, configuration))
				.withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
				.withConf(configuration)
				.withDictionaryEncoding(true)
				.withDictionaryEncoding("valueObject", false)
				//.withCompressionCodec(CompressionCodecName.ZSTD)
				.withCompressionCodec(CompressionCodecName.SNAPPY)
				.withRowGroupSize(ROW_GROUP_SIZE_DATA)
				.withPageSize(PAGE_SIZE)
				.withDictionaryPageSize(DICT_PAGE_SIZE)
				//.withBloomFilterEnabled("id", true)
				.build();
	}

	static IExtendedIterator<KvinRecord> createKvinRecordReader(Path path, FilterCompat.Filter filter) {
		try {
			ParquetReadOptions.Builder optionsBuilder = HadoopReadOptions.builder(configuration, path);
			optionsBuilder.withAllocator(new HeapByteBufferAllocator());
			if (filter != null) {
				optionsBuilder.withRecordFilter(filter);
			}
			ParquetReadOptions options = optionsBuilder.build();
			ParquetFileReader r = new ParquetFileReader(HadoopInputFile.fromPath(path, configuration), options);
			MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(r.getFileMetaData().getSchema());
			return new NiceIterator<>() {
				RecordReader recordReader;
				KvinRecord next;
				PageReadStore pages;
				long readRows;

				@Override
				public boolean hasNext() {
					if (next == null) {
						try {
							while (next == null) {
								if (pages == null || readRows == pages.getRowCount()) {
									readRows = 0;
									recordReader = null;
									if (pages != null) {
										pages.close();
									}
									pages = r.readNextFilteredRowGroup();
									if (pages != null && pages.getRowCount() > 0) {
										recordReader = columnIO.getRecordReader(pages,
												new KvinRecordConverter());
									}
								}
								if (recordReader != null) {
									while (readRows < pages.getRowCount()) {
										next = (KvinRecord) recordReader.read();
										readRows++;
										if (!recordReader.shouldSkipCurrentRecord()) {
											break;
										}
									}
								} else {
									r.close();
									break;
								}
							}
						} catch (IOException e) {
							throw new UncheckedIOException(e);
						} catch (Exception e) {
							log.error("Error while reading next element", e);
							close();
							return false;
						}
					}
					return next != null;
				}

				@Override
				public KvinRecord next() {
					if (!hasNext()) {
						throw new NoSuchElementException();
					}
					KvinRecord result = next;
					next = null;
					return result;
				}

				@Override
				public void close() {
					if (pages != null) {
						pages.close();
					}
					try {
						r.close();
					} catch (IOException e) {
						throw new UncheckedIOException(e);
					}
				}
			};
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	static ParquetWriter<Object> getParquetMappingWriter(Path dataFile) throws IOException {
		return AvroParquetWriter.builder(HadoopOutputFile.fromPath(dataFile, configuration))
				.withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
				.withSchema(idMappingSchema)
				.withConf(configuration)
				.withDictionaryEncoding(true)
				//.withCompressionCodec(CompressionCodecName.ZSTD)
				.withCompressionCodec(CompressionCodecName.SNAPPY)
				.withRowGroupSize(ROW_GROUP_SIZE_MAPPINGS)
				.withPageSize(PAGE_SIZE)
				.withDictionaryPageSize(DICT_PAGE_SIZE)
				//.withBloomFilterEnabled("value", true)
				.withDataModel(reflectData)
				.build();
	}

	public static Map<String, List<Pair<String, Integer>>> getMappingFiles(java.nio.file.Path folder)
			throws IOException {
		Map<String, List<Pair<String, Integer>>> fileMap = new HashMap<>();
		if (Files.isDirectory(folder)) {
			Files.list(folder).forEach(p -> {
				String name = p.getFileName().toString();
				Matcher m = fileWithSeqNr.matcher(name);
				if (m.matches()) {
					fileMap.compute(m.group(1), (k, v) -> {
						List<Pair<String, Integer>> files = v == null ? new ArrayList<>() : v;
						files.add(new Pair<>(name, Integer.parseInt(m.group(2))));
						return files;
					});
				}
			});
			fileMap.values().forEach(list -> {
				list.sort(Comparator.comparing(Pair::getFirst));
			});
		}
		return fileMap;
	}

	public static void deleteMappingFiles(java.nio.file.Path folder, Set<String> types)
			throws IOException {
		if (Files.isDirectory(folder)) {
			Files.list(folder).forEach(p -> {
				String name = p.getFileName().toString();
				Matcher m = fileOrDotFileWithSeqNr.matcher(name);
				if (m.matches()) {
					String type = m.group(1);
					if (types.contains(type)) {
						// delete file
						try {
							Files.delete(p);
						} catch (IOException e) {
							log.error("Unable to delete file", e);
						}
					}
				}
			});
		}
	}

	public static KvinTuple recordToTuple(URI item, URI property, URI context, KvinRecord record) throws IOException {
		Object value = record.value;
		if (value != null) {
			if (value instanceof ByteBuffer) {
				value = decodeRecord((ByteBuffer) value);
			}
		}
		return new KvinTuple(item, property, context, record.time, record.seqNr, value);
	}
}
