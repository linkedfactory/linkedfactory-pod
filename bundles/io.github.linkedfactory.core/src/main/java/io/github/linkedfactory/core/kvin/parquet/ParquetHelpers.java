package io.github.linkedfactory.core.kvin.parquet;

import net.enilink.commons.util.Pair;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParquetHelpers {
	final static ReflectData reflectData = new ReflectData(ParquetHelpers.class.getClassLoader());

	// parquet file writer config
	static final long ROW_GROUP_SIZE = 1048576;  // 1 MB
	static final int PAGE_SIZE = 8192; // 8 KB
	static final int DICT_PAGE_SIZE = 1048576; // 1 MB
	static final int ZSTD_COMPRESSION_LEVEL = 12; // 1 - 22

	// mapping file schema
	static Schema idMappingSchema = SchemaBuilder.record("SimpleMapping").namespace(IdMapping.class.getPackageName()).fields()
			.name("id").type().longType().noDefault()
			.name("value").type().stringType().noDefault().endRecord();

	// data file schema
	static Schema kvinTupleSchema = SchemaBuilder.record("KvinTupleInternal").namespace(KvinTupleInternal.class.getPackageName()).fields()
			.name("id").type().nullable().bytesType().noDefault()
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

	static ParquetWriter<KvinTupleInternal> getParquetDataWriter(Path dataFile) throws IOException {
		Configuration writerConf = new Configuration();
		writerConf.setInt("parquet.zstd.compressionLevel", ZSTD_COMPRESSION_LEVEL);
		return AvroParquetWriter.<KvinTupleInternal>builder(HadoopOutputFile.fromPath(dataFile, new Configuration()))
				.withSchema(kvinTupleSchema)
				.withConf(writerConf)
				.withDictionaryEncoding(true)
				.withCompressionCodec(CompressionCodecName.ZSTD)
				//.withCompressionCodec(CompressionCodecName.SNAPPY)
				.withRowGroupSize(ROW_GROUP_SIZE)
				.withPageSize(PAGE_SIZE)
				.withDictionaryPageSize(DICT_PAGE_SIZE)
				.withDataModel(reflectData)
				.build();
	}

	static ParquetWriter<Object> getParquetMappingWriter(Path dataFile) throws IOException {
		Configuration writerConf = new Configuration();
		writerConf.setInt("parquet.zstd.compressionLevel", 12);
		return AvroParquetWriter.builder(HadoopOutputFile.fromPath(dataFile, new Configuration()))
				.withSchema(idMappingSchema)
				.withConf(writerConf)
				.withDictionaryEncoding(true)
				.withCompressionCodec(CompressionCodecName.ZSTD)
				//.withCompressionCodec(CompressionCodecName.SNAPPY)
				.withRowGroupSize(ROW_GROUP_SIZE)
				.withPageSize(PAGE_SIZE)
				.withDictionaryPageSize(DICT_PAGE_SIZE)
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
}
