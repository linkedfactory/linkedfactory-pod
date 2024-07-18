package io.github.linkedfactory.core.kvin.parquet;

import io.github.linkedfactory.core.kvin.KvinTuple;
import net.enilink.commons.util.Pair;
import net.enilink.komma.core.URI;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
	static final long ROW_GROUP_SIZE = 134217728L; // Parquet Java default
	static final int PAGE_SIZE = 8192; // 8 KB
	static final int DICT_PAGE_SIZE = 1048576; // 1 MB
	static final int ZSTD_COMPRESSION_LEVEL = 12; // 1 - 22

	// mapping file schema
	static Schema idMappingSchema = SchemaBuilder.record("SimpleMapping").namespace(IdMapping.class.getPackageName()).fields()
			.name("id").type().longType().noDefault()
			.name("value").type().stringType().noDefault().endRecord();

	// data file schema
	static Schema kvinTupleSchema = SchemaBuilder.record("KvinTupleInternal").namespace(KvinTupleInternal.class.getPackageName()).fields()
			.name("id").type().bytesType().noDefault()
			.name("time").type().longType().noDefault()
			.name("seqNr").type().intType().intDefault(0)
			.name("valueInt").type().nullable().intType().noDefault()
			.name("valueLong").type().nullable().longType().noDefault()
			.name("valueFloat").type().nullable().floatType().noDefault()
			.name("valueDouble").type().nullable().doubleType().noDefault()
			.name("valueString").type().nullable().stringType().noDefault()
			.name("valueBool").type().nullable().intType().noDefault()
			.name("valueObject").type().nullable().bytesType().noDefault().endRecord();

	static Pattern fileWithSeqNr = Pattern.compile("^([^.].*)__([0-9]+)\\..*$");
	static Pattern fileOrDotFileWithSeqNr = Pattern.compile("^\\.?([^.].*)__([0-9]+)\\..*$");
	static Configuration configuration = new Configuration();

	static {
		configuration.setInt("parquet.zstd.compressionLevel", ZSTD_COMPRESSION_LEVEL);
	}

	static ParquetWriter<KvinTupleInternal> getParquetDataWriter(Path dataFile) throws IOException {
		return AvroParquetWriter.<KvinTupleInternal>builder(HadoopOutputFile.fromPath(dataFile, configuration))
				.withSchema(kvinTupleSchema)
				.withConf(configuration)
				.withDictionaryEncoding(true)
				//.withCompressionCodec(CompressionCodecName.ZSTD)
				.withCompressionCodec(CompressionCodecName.SNAPPY)
				.withRowGroupSize(ROW_GROUP_SIZE)
				.withPageSize(PAGE_SIZE)
				.withDictionaryPageSize(DICT_PAGE_SIZE)
				.withDataModel(reflectData)
				.withBloomFilterEnabled("id", true)
				.build();
	}

	static ParquetWriter<Object> getParquetMappingWriter(Path dataFile) throws IOException {
		return AvroParquetWriter.builder(HadoopOutputFile.fromPath(dataFile, configuration))
				.withSchema(idMappingSchema)
				.withConf(configuration)
				.withDictionaryEncoding(true)
				//.withCompressionCodec(CompressionCodecName.ZSTD)
				.withCompressionCodec(CompressionCodecName.SNAPPY)
				.withRowGroupSize(ROW_GROUP_SIZE_MAPPINGS)
				.withPageSize(PAGE_SIZE)
				.withDictionaryPageSize(DICT_PAGE_SIZE)
				.withBloomFilterEnabled("value", true)
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

	public static KvinTuple recordToTuple(URI item, URI property, URI context, GenericRecord record) throws IOException {
		long time = (Long) record.get(1);
		int seqNr = (Integer) record.get(2);
		int fields = record.getSchema().getFields().size();
		Object value = null;
		for (int i = 3; i < fields; i++) {
			value = record.get(i);
			if (value != null) {
				if (i == fields - 1) {
					value = decodeRecord((ByteBuffer) value);
				}
				break;
			}
		}
		return new KvinTuple(item, property, context, time, seqNr, value);
	}
}
